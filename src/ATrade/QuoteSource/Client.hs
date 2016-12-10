{-# LANGUAGE OverloadedStrings #-}

module ATrade.QuoteSource.Client (
  startQuoteSourceClient,
  stopQuoteSourceClient
) where

import ATrade.Types
import Control.Concurrent.BoundedChan
import Control.Concurrent hiding (readChan, writeChan)
import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Loops
import Control.Exception
import Data.List.NonEmpty
import Data.Maybe
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as B8
import qualified Data.List as L
import Data.Text.Encoding
import Data.Time.Clock
import Data.IORef
import System.ZMQ4
import System.Log.Logger

import Safe

data QuoteSourceClientHandle = QuoteSourceClientHandle {
  tid :: ThreadId,
  completionMvar :: MVar (),
  killMVar :: MVar ()
}

startQuoteSourceClient :: BoundedChan Tick -> [T.Text] -> Context -> T.Text -> IO QuoteSourceClientHandle
startQuoteSourceClient chan tickers ctx endpoint = do
  compMv <- newEmptyMVar
  killMv <- newEmptyMVar
  now <- getCurrentTime
  lastHeartbeat <- newIORef now
  tid <- forkIO $ finally (clientThread lastHeartbeat killMv) (cleanup compMv)
  return QuoteSourceClientHandle { tid = tid, completionMvar = compMv, killMVar = killMv }
  where
    clientThread lastHeartbeat killMv = whileM_ (isNothing <$> tryReadMVar killMv) $ withSocket ctx Sub (\sock -> do
      connect sock $ T.unpack endpoint
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
            else case deserializeTick rawTick of
              Just tick -> writeChan chan tick
              Nothing -> warningM "QuoteSource.Client" "Error: can't deserialize tick"
      debugM "QuoteSource.Client" "Heartbeat timeout")

    notTimeout ts = do
      now <- getCurrentTime
      heartbeatTs <- readIORef ts
      return $ diffUTCTime now heartbeatTs < 30

    cleanup compMv = putMVar compMv ()

stopQuoteSourceClient :: QuoteSourceClientHandle -> IO ()
stopQuoteSourceClient handle = yield >> putMVar (killMVar handle) () >> readMVar (completionMvar handle)
