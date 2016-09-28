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
type PeerId = B.ByteString

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
  orderMap :: M.Map OrderId PeerId, -- Matches 0mq client identities with corresponding orders
  lastPacket :: M.Map PeerId (RequestSqnum, B.ByteString),
  pendingNotifications :: M.Map PeerId [Notification],
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
    pendingNotifications = M.empty,
    brokers = brokers,
    completionMvar = compMv,
    orderIdCounter = 1
  }
  mapM_ (\bro -> setNotificationCallback bro (Just $ notificationCallback state)) brokers

  BrokerServerHandle <$> forkIO (brokerServerThread state) <*> pure compMv

notificationCallback :: IORef BrokerServerState -> Notification -> IO ()
notificationCallback state n = do
  orders <- orderMap <$> readIORef state
  case M.lookup (notificationOrderId n) orders of
    Just peerId -> addNotification peerId n
    Nothing -> warningM "Broker.Server" "Notification: unknown order"

  where
    addNotification peerId n = atomicModifyIORef' state (\s ->
      case M.lookup peerId . pendingNotifications $ s of
        Just ns -> (s { pendingNotifications = M.insert peerId (n : ns) (pendingNotifications s)}, ())
        Nothing -> (s { pendingNotifications = M.insert peerId [n] (pendingNotifications s)}, ()))

brokerServerThread state = finally brokerServerThread' cleanup
  where
    brokerServerThread' = forever $ do
      sock <- bsSocket <$> readIORef state
      msg <- receiveMulti sock
      case msg of
        [peerId, _, payload] -> handleMessage peerId payload >>= sendMessage sock peerId
        _ -> warningM "Broker.Server" ("Invalid packet received: " ++ show msg)

    cleanup = do
      sock <- bsSocket <$> readIORef state
      close sock
      mv <- completionMvar <$> readIORef state
      putMVar mv ()

    handleMessage :: B.ByteString -> B.ByteString -> IO BrokerServerResponse
    handleMessage peerId payload = do
      bros <- brokers <$> readIORef state
      case decode . BL.fromStrict $ payload of
        Just (RequestSubmitOrder sqnum order) ->
          case findBrokerForAccount (orderAccountId order) bros of
            Just bro -> do
              oid <- nextOrderId
              atomicModifyIORef' state (\s -> (s {
                orderToBroker = M.insert oid bro (orderToBroker s),
                orderMap = M.insert oid peerId (orderMap s) }, ()))
              submitOrder bro order { orderId = oid }
              return $ ResponseOrderSubmitted oid

            Nothing -> return $ ResponseError "Unknown account"
        Just (RequestCancelOrder sqnum oid) -> do
          m <- orderToBroker <$> readIORef state
          case M.lookup oid m of
            Just bro -> do
              cancelOrder bro oid
              return $ ResponseOrderCancelled oid
            Nothing -> return $ ResponseError "Unknown order"
        Just (RequestNotifications sqnum) -> do
          maybeNs <- M.lookup peerId . pendingNotifications <$> readIORef state
          case maybeNs of
            Just ns -> do
              atomicModifyIORef' state (\s -> (s { pendingNotifications = M.insert peerId [] (pendingNotifications s)}, ()))
              return $ ResponseNotifications ns
            Nothing -> return $ ResponseNotifications []
        Nothing -> return $ ResponseError "Unable to parse request"

    sendMessage sock peerId resp = sendMulti sock (peerId :| [B.empty, BL.toStrict . encode $ resp])

    findBrokerForAccount account = L.find (L.elem account . accounts)
    nextOrderId = atomicModifyIORef' state (\s -> ( s {orderIdCounter = 1 + orderIdCounter s}, orderIdCounter s))


stopBrokerServer :: BrokerServerHandle -> IO ()
stopBrokerServer (BrokerServerHandle tid compMv) = yield >> killThread tid >> readMVar compMv

