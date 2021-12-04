{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

module ATrade.Broker.TradeSinks.TelegramTradeSink (
  withTelegramTradeSink
) where

import           Control.Concurrent
import qualified Control.Concurrent.BoundedChan as BC
import           Control.Exception
import           Control.Monad.Extra
import           Control.Monad.Loops
import           Data.Aeson
import           Data.Aeson.Types
import qualified Data.ByteString                as B hiding (putStrLn)
import qualified Data.ByteString.Lazy           as BL hiding (putStrLn)
import           Data.IORef
import qualified Data.List                      as L
import           Data.List.NonEmpty
import           Data.Maybe

import           ATrade.Broker.Protocol
import           ATrade.Types
import           Network.Connection
import           Network.HTTP.Client
import           Network.HTTP.Client.TLS

import qualified Data.ByteString.UTF8           as BU8
import qualified Data.Text                      as T
import qualified Data.Text.Lazy                 as TL
import           Language.Haskell.Printf

withTelegramTradeSink apitoken chatId f = do
  killMv <- newEmptyMVar
  chan <- BC.newBoundedChan 1000
  bracket (forkIO $ sinkThread apitoken chatId killMv chan) (stopSinkThread killMv) (\_ -> f $ sink chan)
  where
    sink = BC.writeChan

sinkThread apitoken chatId killMv chan = do
  man <- newManager $ mkManagerSettings tlsSettings Nothing
  whileM_ (not <$> wasKilled) $ do
    maybeTrade <- BC.tryReadChan chan
    case maybeTrade of
      Just trade -> sendMessage man apitoken chatId $ [t|Trade: %? %? of %? at %? for %? (%?/%?)|]
        (show $ tradeOperation trade)
        (tradeQuantity trade)
        (tradeSecurity trade)
        (show $ tradePrice trade)
        (tradeAccount trade)
        ((strategyId . tradeSignalId) trade)
        ((signalName . tradeSignalId) trade)
      Nothing -> threadDelay 1000000
  where
    tlsSettings = TLSSettingsSimple { settingDisableCertificateValidation = True, settingDisableSession = False, settingUseServerName = False }
    wasKilled = isJust <$> tryReadMVar killMv
    encodeTrade :: Trade -> B.ByteString
    encodeTrade = BL.toStrict . encode . convertTrade
    convertTrade trade = TradeSinkTrade {
      tsAccountId = tradeAccount trade,
      tsSecurity = tradeSecurity trade,
      tsPrice = fromRational . toRational . tradePrice $ trade,
      tsQuantity = fromInteger $ tradeQuantity trade,
      tsVolume = fromRational . toRational . tradeVolume $ trade,
      tsCurrency = tradeVolumeCurrency trade,
      tsOperation = tradeOperation trade,
      tsExecutionTime = tradeTimestamp trade,
      tsCommission = toDouble (tradeCommission trade),
      tsSignalId = tradeSignalId trade
    }

    sendMessage httpManager apitoken chatId text = do
      req <- parseUrl $ "https://api.telegram.org/bot" ++ (T.unpack apitoken) ++ "/sendMessage"
      void $ withResponse (req { method = "POST", requestHeaders = [("Content-Type", BU8.fromString "application/json")], requestBody = (RequestBodyLBS . encode) (object ["chat_id" .= chatId, "text" .= text]) }) httpManager (\resp -> brConsume (responseBody resp))


stopSinkThread killMv threadId = putMVar killMv () >> killThread threadId

