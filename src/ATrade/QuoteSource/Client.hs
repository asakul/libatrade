{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module ATrade.QuoteSource.Client (
  QuoteData(..),
  startQuoteSourceClient,
  stopQuoteSourceClient,
  quoteSourceClientSubscribe
) where

import           ATrade.Types
import           Control.Concurrent             hiding (readChan, writeChan,
                                                 writeList2Chan)
import           Control.Concurrent.BoundedChan
import           Control.Concurrent.MVar
import           Control.Exception
import           Control.Monad
import           Control.Monad.Loops
import qualified Data.ByteString.Char8          as B8
import qualified Data.ByteString.Lazy           as BL
import           Data.IORef
import qualified Data.List                      as L
import           Data.List.NonEmpty
import           Data.Maybe
import qualified Data.Set                       as S
import qualified Data.Text                      as T
import           Data.Text.Encoding
import           Data.Time.Clock
import           System.Log.Logger
import           System.ZMQ4
import           System.ZMQ4.ZAP

import           Safe

data QSSClientMessage = QSSSubscribe [(TickerId, BarTimeframe)] | QSSUnsubscribe [(TickerId, BarTimeframe)]

data QuoteSourceClientHandle = QuoteSourceClientHandle {
  tid            :: ThreadId,
  completionMvar :: MVar (),
  killMVar       :: MVar (),
  messageBox     :: BoundedChan QSSClientMessage,
  subscriptions  :: IORef (S.Set (TickerId, BarTimeframe))
}

data QuoteData = QDTick Tick | QDBar (BarTimeframe, Bar)
  deriving (Show, Eq)

deserializeTicks :: [BL.ByteString] -> [QuoteData]
deserializeTicks (secname:raw:_) = deserializeWithName (decodeUtf8 . BL.toStrict $ secname) raw
  where
    deserializeWithName secNameT raw = case deserializeTickBody raw of
      (rest, Just tick) -> QDTick (tick { security = secNameT }) : deserializeWithName secNameT rest
      _ -> []

deserializeTicks _ = []

startQuoteSourceClient :: BoundedChan QuoteData -> [T.Text] -> Context -> T.Text -> ClientSecurityParams -> IO QuoteSourceClientHandle
startQuoteSourceClient chan tickers ctx endpoint csp = do
  compMv <- newEmptyMVar
  killMv <- newEmptyMVar
  msgbox <- newBoundedChan 500
  subs <- newIORef $ S.fromList $ fmap (\x -> (x, BarTimeframe 0)) tickers
  now <- getCurrentTime
  lastHeartbeat <- newIORef now
  tid <- forkIO $ finally (clientThread lastHeartbeat killMv msgbox subs) (cleanup compMv)
  return QuoteSourceClientHandle { tid = tid, completionMvar = compMv, killMVar = killMv, messageBox = msgbox, subscriptions = subs }
  where
    clientThread lastHeartbeat killMv msgbox subs = whileM_ (isNothing <$> tryReadMVar killMv) $ withSocket ctx Sub (\sock -> do
      setLinger (restrict 0) sock
      debugM "QuoteSource.Client" $ "Client security parameters: " ++ show csp
      case (cspCertificate csp, cspServerCertificate csp) of
        (Just cert, Just serverCert) -> do
          zapApplyCertificate cert sock
          zapSetServerCertificate serverCert sock
        _                            -> return ()
      connect sock $ T.unpack endpoint
      subslist <- readIORef subs
      debugM "QuoteSource.Client" $ "Tickers: " ++ show subslist
      mapM_ (subscribe sock . encodeUtf8 . mkSubCode) subslist
      subscribe sock $ B8.pack "SYSTEM#HEARTBEAT"

      now <- getCurrentTime
      writeIORef lastHeartbeat now
      whileM_ (andM [notTimeout lastHeartbeat, isNothing <$> tryReadMVar killMv]) $ do
        evs <- poll 50 [Sock sock [In] Nothing]
        unless (null (L.head evs)) $ do
          rawTick <- fmap BL.fromStrict <$> receiveMulti sock
          now <- getCurrentTime
          prevHeartbeat <- readIORef lastHeartbeat
          if headMay rawTick == Just "SYSTEM#HEARTBEAT"
            then writeIORef lastHeartbeat now
            else case deserializeBar rawTick of
              Just (tf, bar) -> writeChan chan $ QDBar (tf, bar)
              _ -> writeList2Chan chan (deserializeTicks rawTick)
        whileJust (tryReadChan msgbox) $ \case
          QSSSubscribe tickers -> do
            atomicModifyIORef' subs (\x -> (foldr S.insert x tickers, ()))
            mapM_ (subscribe sock . encodeUtf8 . mkSubCode) tickers
          _ -> return ()
      debugM "QuoteSource.Client" "Heartbeat timeout")

    notTimeout ts = do
      now <- getCurrentTime
      heartbeatTs <- readIORef ts
      return $ diffUTCTime now heartbeatTs < 30

    cleanup compMv = putMVar compMv ()

    mkSubCode (tid, BarTimeframe tf) =
      if tf == 0 then tid else tid <> ":" <> T.pack (show tf) <> ";"

stopQuoteSourceClient :: QuoteSourceClientHandle -> IO ()
stopQuoteSourceClient handle = yield >> putMVar (killMVar handle) () >> readMVar (completionMvar handle)

quoteSourceClientSubscribe :: QuoteSourceClientHandle -> [(TickerId, BarTimeframe)] -> IO ()
quoteSourceClientSubscribe handle tickers = writeChan (messageBox handle) (QSSSubscribe tickers)
