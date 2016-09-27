
module ATrade.QuoteSource.Server (
  startQuoteSourceServer,
  stopQuoteSourceServer
) where

import ATrade.Types
import Control.Concurrent.BoundedChan
import Control.Concurrent hiding (readChan, writeChan)
import Control.Exception
import Control.Monad
import qualified Data.ByteString.Lazy as BL
import Data.List.NonEmpty hiding (map)
import System.Log.Logger
import System.ZMQ4

data QuoteSourceServer = QuoteSourceServerState {
  ctx :: Context,
  outSocket :: Socket Pub,
  tickChannel :: BoundedChan (Maybe Tick),
  completionMvar :: MVar (),
  serverThreadId :: ThreadId
}

serverThread :: QuoteSourceServer -> IO ()
serverThread state = do
  finally serverThread' cleanup
  debugM "QuoteSource" "server thread done"
  where
    cleanup = do
      close $ outSocket state
      putMVar (completionMvar state) ()

    serverThread' = do
      maybeTick <- readChan $ tickChannel state
      case maybeTick of
        Nothing -> return ()
        Just tick -> do
          sendMulti (outSocket state) $ fromList . map BL.toStrict $ serializeTick tick
          serverThread'

startQuoteSourceServer :: BoundedChan (Maybe Tick) -> Context -> String -> IO QuoteSourceServer
startQuoteSourceServer chan c ep = do
  sock <- socket c Pub
  bind sock ep
  tid <- myThreadId
  mv <- newEmptyMVar
  let state = QuoteSourceServerState {
    ctx = c,
    outSocket = sock,
    tickChannel = chan,
    completionMvar = mv,
    serverThreadId = tid
  }
  stid <- forkIO $ serverThread state
  return $ state { serverThreadId = stid }

stopQuoteSourceServer :: QuoteSourceServer -> IO ()
stopQuoteSourceServer server = writeChan (tickChannel server) Nothing >> readMVar (completionMvar server)

