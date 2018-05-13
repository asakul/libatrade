
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
import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Text.Encoding as E
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as B
import Data.List.NonEmpty hiding (map)
import Data.Maybe
import System.Log.Logger
import System.ZMQ4
import System.ZMQ4.ZAP
import Prelude hiding ((!!))

import Safe

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
      qssdata' <- readChan $ tickChannel state
      qssdata <- readChanN 15 $ tickChannel state
      let fulldata = qssdata' : qssdata
      let tickGroups = L.groupBy (\x y -> security x == security y) $ mapMaybe onlyTick fulldata

      mapM_ (\ticks -> case headMay ticks of
        Just h -> sendTicks (security h) ticks
        Nothing -> return()) tickGroups

      when (QSSHeartbeat `elem` fulldata) $ send (outSocket state) [] $ B8.pack "SYSTEM#HEARTBEAT"

      unless (QSSKill `elem` fulldata) serverThread'
    
    readChanN n chan
      | n <= 0 = return []
      | otherwise = do
        x <- tryReadChan chan
        case x of
          Nothing -> return []
          Just v -> do
            rest <- readChanN (n - 1) chan
            return $ v : rest

    onlyTick t = case t of
      QSSTick tick -> Just tick
      _ -> Nothing

    sendTicks secName ticklist = sendMulti (outSocket state) $ fromList . map BL.toStrict $ serializedTicks secName ticklist
    serializedTicks secName ticklist = header : [body]
      where
        header = BL.fromStrict . E.encodeUtf8 $ secName
        body = BL.concat $ map serializeTickBody ticklist

startQuoteSourceServer :: BoundedChan QuoteSourceServerData -> Context -> T.Text -> Maybe DomainId -> IO QuoteSourceServer
startQuoteSourceServer chan c ep socketDomainIdMb = do
  sock <- socket c Pub
  setLinger (restrict 0) sock
  case socketDomainIdMb of
    Just socketDomainId -> setZapDomain (restrict $ E.encodeUtf8 socketDomainId) sock
    _ -> return ()
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
stopQuoteSourceServer server = killThread (heartbeatThreadId server) >> (writeChan (tickChannel server) QSSKill) >> readMVar (completionMvar server)

