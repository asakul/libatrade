
module ATrade.Broker.Server (
  startBrokerServer,
  stopBrokerServer,
  BrokerInterface(..)
) where

import ATrade.Types
import ATrade.Broker.Protocol
import System.ZMQ4
import qualified Data.Map as M
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.List as L
import Data.Aeson
import Data.Time.Clock
import Data.IORef
import Control.Concurrent
import Control.Exception
import Control.Monad
import System.Log.Logger

newtype OrderIdGenerator = IO OrderId

data BrokerInterface = BrokerInterface {
  accounts :: [T.Text],
  setNotificationCallback :: Maybe (Notification -> IO()) -> IO (),
  submitOrder :: Order -> IO (),
  cancelOrder :: OrderId -> IO (),
  stopBroker :: IO ()
}

data BrokerServerState = BrokerServerState {
  bsSocket :: Socket Router,
  orderMap :: M.Map OrderId B.ByteString, -- Matches 0mq client identities with corresponding orders
  lastPacket :: M.Map B.ByteString (RequestSqnum, B.ByteString),
  pendingNotifications :: [(Notification, UTCTime)], -- List of tuples (Order with new state, Time when notification enqueued)
  brokers :: [BrokerInterface],
  completionMvar :: MVar ()
}

data BrokerServerHandle = BrokerServerHandle ThreadId (MVar ())

startBrokerServer :: [BrokerInterface] -> Context -> T.Text -> IO BrokerServerHandle
startBrokerServer brokers c ep = do
  sock <- socket c Router
  bind sock (T.unpack ep)
  tid <- myThreadId
  compMv <- newEmptyMVar
  state <- newIORef BrokerServerState {
    bsSocket = sock,
    orderMap = M.empty,
    lastPacket = M.empty,
    pendingNotifications = [],
    brokers = brokers,
    completionMvar = compMv
  }

  BrokerServerHandle <$> forkIO (brokerServerThread state) <*> pure compMv

brokerServerThread state = finally brokerServerThread' cleanup
  where
    brokerServerThread' = forever $ do
      sock <- bsSocket <$> readIORef state
      receiveMulti sock >>= handleMessage

    cleanup = do
      sock <- bsSocket <$> readIORef state
      close sock
      mv <- completionMvar <$> readIORef state
      putMVar mv ()

    handleMessage :: [B.ByteString] -> IO ()
    handleMessage [peerId, _, payload] = do
      bros <- brokers <$> readIORef state
      case decode . BL.fromStrict $ payload of
        Just (RequestSubmitOrder sqnum order) ->
          case findBroker (orderAccountId order) bros of
            Just bro -> submitOrder bro order
            Nothing -> return ()
        Nothing -> return ()
        
    handleMessage x = warningM "Broker.Server" ("Invalid packet received: " ++ show x)
    findBroker account = L.find (L.elem account . accounts)
      

stopBrokerServer :: BrokerServerHandle -> IO ()
stopBrokerServer (BrokerServerHandle tid compMv) = yield >> killThread tid >> readMVar compMv

