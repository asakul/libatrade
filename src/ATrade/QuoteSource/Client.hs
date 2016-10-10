{-# LANGUAGE OverloadedStrings #-}

module ATrade.QuoteSource.Client (
  startQuoteSourceClient,
  stopQuoteSourceClient
) where

import ATrade.Types
import Control.Concurrent hiding (readChan, writeChan)
import Control.Concurrent.BoundedChan
import Control.Concurrent.MVar
import Control.Monad
import Control.Exception
import Data.List.NonEmpty
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
  completionMvar :: MVar ()
}

startQuoteSourceClient :: BoundedChan Tick -> [T.Text] -> Context -> T.Text -> IO QuoteSourceClientHandle
startQuoteSourceClient chan tickers ctx endpoint = do
  compMv <- newEmptyMVar
  now <- getCurrentTime
  lastHeartbeat <- newIORef now
  tid <- forkIO $ do
    sock <- createAndConnectSocket
    finally (clientThread sock lastHeartbeat) (cleanup compMv sock)
  return QuoteSourceClientHandle { tid = tid, completionMvar = compMv }
  where
    clientThread sock lastHeartbeat = forever $ do
      evs <- poll 200 [Sock sock [In] Nothing] 
      when ((L.length . L.head) evs > 0) $ do
        rawTick <- fmap BL.fromStrict <$> receiveMulti sock
        if headMay rawTick == Just "SYSTEM#HEARTBEAT"
          then do
            now <- getCurrentTime
            writeIORef lastHeartbeat now
          else case deserializeTick rawTick of
            Just tick -> writeChan chan tick
            Nothing -> warningM "QuoteSource.Client" "Error: can't deserialize tick"
    cleanup compMv sock = close sock >> putMVar compMv ()
    createAndConnectSocket = do
      sock <- socket ctx Sub
      connect sock $ T.unpack endpoint
      mapM_ (\t -> subscribe sock $ encodeUtf8 t) tickers
      subscribe sock $ B8.pack "SYSTEM#HEARTBEAT"
      return sock

stopQuoteSourceClient :: QuoteSourceClientHandle -> IO ()
stopQuoteSourceClient handle = yield >> killThread (tid handle) >> readMVar (completionMvar handle)
