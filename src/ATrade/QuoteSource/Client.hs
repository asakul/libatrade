{-# LANGUAGE OverloadedStrings #-}

module ATrade.QuoteSource.Client (
  QuoteData(..),
  startQuoteSourceClient,
  stopQuoteSourceClient
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
import qualified Data.Text                      as T
import           Data.Text.Encoding
import           Data.Time.Clock
import           System.Log.Logger
import           System.ZMQ4
import           System.ZMQ4.ZAP

import           Safe

data QuoteSourceClientHandle = QuoteSourceClientHandle {
  tid            :: ThreadId,
  completionMvar :: MVar (),
  killMVar       :: MVar ()
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
  now <- getCurrentTime
  lastHeartbeat <- newIORef now
  tid <- forkIO $ finally (clientThread lastHeartbeat killMv) (cleanup compMv)
  return QuoteSourceClientHandle { tid = tid, completionMvar = compMv, killMVar = killMv }
  where
    clientThread lastHeartbeat killMv = whileM_ (isNothing <$> tryReadMVar killMv) $ withSocket ctx Sub (\sock -> do
      setLinger (restrict 0) sock
      debugM "QuoteSource.Client" $ "Client security parameters: " ++ show csp
      case (cspCertificate csp, cspServerCertificate csp) of
        (Just cert, Just serverCert) -> do
          zapApplyCertificate cert sock
          zapSetServerCertificate serverCert sock
        _                            -> return ()
      connect sock $ T.unpack endpoint
      debugM "QuoteSource.Client" $ "Tickers: " ++ show tickers
      mapM_ (subscribe sock . encodeUtf8) tickers
      subscribe sock $ B8.pack "SYSTEM#HEARTBEAT"

      now <- getCurrentTime
      writeIORef lastHeartbeat now
      whileM_ (andM [notTimeout lastHeartbeat, isNothing <$> tryReadMVar killMv]) $ do
        evs <- poll 200 [Sock sock [In] Nothing]
        unless (null (L.head evs)) $ do
          rawTick <- fmap BL.fromStrict <$> receiveMulti sock
          now <- getCurrentTime
          prevHeartbeat <- readIORef lastHeartbeat
          if headMay rawTick == Just "SYSTEM#HEARTBEAT"
            then writeIORef lastHeartbeat now
            else case deserializeBar rawTick of
              Just (tf, bar) -> writeChan chan $ QDBar (tf, bar)
              _ -> writeList2Chan chan (deserializeTicks rawTick)
      debugM "QuoteSource.Client" "Heartbeat timeout")

    notTimeout ts = do
      now <- getCurrentTime
      heartbeatTs <- readIORef ts
      return $ diffUTCTime now heartbeatTs < 30

    cleanup compMv = putMVar compMv ()

stopQuoteSourceClient :: QuoteSourceClientHandle -> IO ()
stopQuoteSourceClient handle = yield >> putMVar (killMVar handle) () >> readMVar (completionMvar handle)
