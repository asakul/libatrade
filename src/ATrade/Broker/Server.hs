{-# LANGUAGE OverloadedStrings #-}

module ATrade.Broker.Server (
  startBrokerServer,
  stopBrokerServer,
  BrokerInterface(..)
) where

import ATrade.Types
import ATrade.Broker.Protocol
import System.ZMQ4
import Data.List.NonEmpty
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
  cancelOrder :: OrderId -> IO Bool,
  stopBroker :: IO ()
}

data BrokerServerState = BrokerServerState {
  bsSocket :: Socket Router,
  orderToBroker :: M.Map OrderId BrokerInterface,
  orderMap :: M.Map OrderId B.ByteString, -- Matches 0mq client identities with corresponding orders
  lastPacket :: M.Map B.ByteString (RequestSqnum, B.ByteString),
  pendingNotifications :: [(Notification, UTCTime)], -- List of tuples (Order with new state, Time when notification enqueued)
  brokers :: [BrokerInterface],
  completionMvar :: MVar (),
  orderIdCounter :: OrderId
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
    orderToBroker = M.empty,
    lastPacket = M.empty,
    pendingNotifications = [],
    brokers = brokers,
    completionMvar = compMv,
    orderIdCounter = 1
  }

  BrokerServerHandle <$> forkIO (brokerServerThread state) <*> pure compMv

brokerServerThread state = finally brokerServerThread' cleanup
  where
    brokerServerThread' = forever $ do
      sock <- bsSocket <$> readIORef state
      receiveMulti sock >>= handleMessage >>= sendMessage sock

    cleanup = do
      sock <- bsSocket <$> readIORef state
      close sock
      mv <- completionMvar <$> readIORef state
      putMVar mv ()

    handleMessage :: [B.ByteString] -> IO (B.ByteString, BrokerServerResponse)
    handleMessage [peerId, _, payload] = do
      bros <- brokers <$> readIORef state
      case decode . BL.fromStrict $ payload of
        Just (RequestSubmitOrder sqnum order) ->
          case findBrokerForAccount (orderAccountId order) bros of
            Just bro -> do
              oid <- nextOrderId
              submitOrder bro order { orderId = oid }
              atomicModifyIORef' state (\s -> (s { orderToBroker = M.insert oid bro (orderToBroker s)}, ()))
              return (peerId, ResponseOrderSubmitted oid)

            Nothing -> return (peerId, ResponseError "Unknown account")
        Just (RequestCancelOrder sqnum oid) -> do
          m <- orderToBroker <$> readIORef state
          case M.lookup oid m of
            Just bro -> do
              cancelOrder bro oid
              return (peerId, ResponseOrderCancelled oid)
            Nothing -> return (peerId, ResponseError "Unknown order")
        Just _ -> return (peerId, ResponseError "Not implemented")
        Nothing -> return (peerId, ResponseError "Unable to parse request")
    handleMessage x = do
      warningM "Broker.Server" ("Invalid packet received: " ++ show x)
      return (B.empty, ResponseError "Invalid packet structure")

    sendMessage sock (peerId, resp) = sendMulti sock (peerId :| [B.empty, BL.toStrict . encode $ resp])

    findBrokerForAccount account = L.find (L.elem account . accounts)
    nextOrderId = atomicModifyIORef' state (\s -> ( s {orderIdCounter = 1 + orderIdCounter s}, orderIdCounter s))


stopBrokerServer :: BrokerServerHandle -> IO ()
stopBrokerServer (BrokerServerHandle tid compMv) = yield >> killThread tid >> readMVar compMv

