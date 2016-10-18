
module ATrade.QuoteSource.Server (
  startQuoteSourceServer,
  stopQuoteSourceServer,
  QuoteSourceServerData(..)
) where

import ATrade.Types
import Control.Concurrent.BoundedChan
import Control.Concurrent hiding (readChan, writeChan)
import Control.Exception
import Control.Monad
import qualified Data.Text as T
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import Data.List.NonEmpty hiding (map)
import System.Log.Logger
import System.ZMQ4

data QuoteSourceServer = QuoteSourceServerState {
  ctx :: Context,
  outSocket :: Socket Pub,
  tickChannel :: BoundedChan QuoteSourceServerData,
  completionMvar :: MVar (),
  serverThreadId :: ThreadId,
  heartbeatThreadId :: ThreadId
}

data QuoteSourceServerData = QSSTick Tick | QSSHeartbeat | QSSKill
  deriving (Show, Eq)

serverThread :: QuoteSourceServer -> IO ()
serverThread state = do
  finally serverThread' cleanup
  debugM "QuoteSource" "server thread done"
  where
    cleanup = do
      close $ outSocket state
      putMVar (completionMvar state) ()

    serverThread' = do
      qssdata <- readChan $ tickChannel state
      case qssdata of
        QSSKill -> return ()
        QSSHeartbeat -> do
          send (outSocket state) [] $ B8.pack "SYSTEM#HEARTBEAT"
          serverThread'
        QSSTick tick -> do
          sendMulti (outSocket state) $ fromList . map BL.toStrict $ serializeTick tick
          serverThread'

startQuoteSourceServer :: BoundedChan QuoteSourceServerData -> Context -> T.Text -> IO QuoteSourceServer
startQuoteSourceServer chan c ep = do
  sock <- socket c Pub
  bind sock $ T.unpack ep
  tid <- myThreadId
  hbTid <- forkIO $ forever $ do
    threadDelay 1000000
    writeChan chan QSSHeartbeat
    
  mv <- newEmptyMVar
  let state = QuoteSourceServerState {
    ctx = c,
    outSocket = sock,
    tickChannel = chan,
    completionMvar = mv,
    serverThreadId = tid,
    heartbeatThreadId = hbTid
  }
  stid <- forkIO $ serverThread state
  return $ state { serverThreadId = stid }

stopQuoteSourceServer :: QuoteSourceServer -> IO ()
stopQuoteSourceServer server = killThread (heartbeatThreadId server) >> writeChan (tickChannel server) QSSKill >> readMVar (completionMvar server)

