{-# LANGUAGE OverloadedStrings #-}

module ATrade.Broker.TradeSinks.TelegramTradeSink (
  withTelegramTradeSink
) where

import Control.Exception
import Control.Concurrent
import qualified Control.Concurrent.BoundedChan as BC
import Data.Aeson
import Data.Aeson.Types
import Data.IORef
import Data.Maybe
import Data.List.NonEmpty
import qualified Data.List as L
import qualified Data.ByteString as B hiding (putStrLn)
import qualified Data.ByteString.Lazy as BL hiding (putStrLn)
import System.Log.Logger
import Control.Monad.Loops
import Control.Monad.Extra

import ATrade.Types
import ATrade.Broker.Protocol
import Network.Connection
import Network.HTTP.Client
import Network.HTTP.Client.TLS

import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import Data.Text.Format
import qualified Data.ByteString.UTF8 as BU8

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
      Just trade -> sendMessage man apitoken chatId $ format "Trade: {} {} of {} at {} for {} ({}/{})"
        (show (tradeOperation trade),
          show (tradeQuantity trade),
          tradeSecurity trade,
          show (tradePrice trade),
          tradeAccount trade,
          (strategyId . tradeSignalId) trade,
          (signalName . tradeSignalId) trade)
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
  
