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
import qualified Data.ByteString as B hiding (putStrLn)
import qualified Data.ByteString.Lazy as BL hiding (putStrLn)
import qualified Data.Text as T
import qualified Data.List as L
import Data.Aeson
import Data.Maybe
import Data.Time.Clock
import Data.IORef
import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.Loops
import System.Log.Logger
import ATrade.Util

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
  lastPacket :: M.Map PeerId (RequestSqnum, BrokerServerResponse),
  pendingNotifications :: M.Map PeerId [Notification],
  brokers :: [BrokerInterface],
  completionMvar :: MVar (),
  killMvar :: MVar (),
  orderIdCounter :: OrderId
}

data BrokerServerHandle = BrokerServerHandle ThreadId (MVar ()) (MVar ())

startBrokerServer :: [BrokerInterface] -> Context -> T.Text -> IO BrokerServerHandle
startBrokerServer brokers c ep = do
  sock <- socket c Router
  bind sock (T.unpack ep)
  tid <- myThreadId
  compMv <- newEmptyMVar
  killMv <- newEmptyMVar
  state <- newIORef BrokerServerState {
    bsSocket = sock,
    orderMap = M.empty,
    orderToBroker = M.empty,
    lastPacket = M.empty,
    pendingNotifications = M.empty,
    brokers = brokers,
    completionMvar = compMv,
    killMvar = killMv,
    orderIdCounter = 1
  }
  mapM_ (\bro -> setNotificationCallback bro (Just $ notificationCallback state)) brokers

  debugM "Broker.Server" "Forking broker server thread"
  BrokerServerHandle <$> forkIO (brokerServerThread state) <*> pure compMv <*> pure killMv

notificationCallback :: IORef BrokerServerState -> Notification -> IO ()
notificationCallback state n = do
  orders <- orderMap <$> readIORef state
  case M.lookup (notificationOrderId n) orders of
    Just peerId -> addNotification peerId n
    Nothing -> warningM "Broker.Server" "Notification: unknown order"

  where
    addNotification peerId n = atomicMapIORef state (\s ->
      case M.lookup peerId . pendingNotifications $ s of
        Just ns -> s { pendingNotifications = M.insert peerId (n : ns) (pendingNotifications s)}
        Nothing -> s { pendingNotifications = M.insert peerId [n] (pendingNotifications s)})

brokerServerThread :: IORef BrokerServerState -> IO ()
brokerServerThread state = finally brokerServerThread' cleanup
  where
    brokerServerThread' = whileM_ (fmap killMvar (readIORef state) >>= fmap isNothing . tryTakeMVar) $ do
      sock <- bsSocket <$> readIORef state
      evs <- poll 200 [Sock sock [In] Nothing] 
      when ((L.length . L.head) evs > 0) $ do
        msg <- receiveMulti sock
        case msg of
          [peerId, _, payload] ->
            case decode . BL.fromStrict $ payload of
              Just request -> do
                let sqnum = requestSqnum request
                -- Here, we should check if previous packet sequence number is the same
                -- If it is, we should resend previous response
                lastPackMap <- lastPacket <$> readIORef state
                case shouldResend sqnum peerId lastPackMap of
                  Just response -> sendMessage sock peerId response -- Resend
                  Nothing -> do
                    -- Handle incoming request, send response
                    response <- handleMessage peerId request
                    sendMessage sock peerId response
                    -- and store response in case we'll need to resend it
                    atomicMapIORef state (\s -> s { lastPacket = M.insert peerId (sqnum, response) (lastPacket s)})
              Nothing -> do
                -- If we weren't able to parse request, we should send error
                -- but shouldn't update lastPacket
                let response = ResponseError "Invalid request"
                sendMessage sock peerId response
          _ -> warningM "Broker.Server" ("Invalid packet received: " ++ show msg)

    shouldResend sqnum peerId lastPackMap = case M.lookup peerId lastPackMap of
      Just (lastSqnum, response) -> if sqnum == lastSqnum
        then Just response
        else Nothing
      Nothing -> Nothing

    cleanup = do
      sock <- bsSocket <$> readIORef state
      close sock

      mv <- completionMvar <$> readIORef state
      putMVar mv ()

    handleMessage :: PeerId -> BrokerServerRequest -> IO BrokerServerResponse
    handleMessage peerId request = do
      bros <- brokers <$> readIORef state
      case request of
        RequestSubmitOrder sqnum order ->
          case findBrokerForAccount (orderAccountId order) bros of
            Just bro -> do
              oid <- nextOrderId
              atomicMapIORef state (\s -> s {
                orderToBroker = M.insert oid bro (orderToBroker s),
                orderMap = M.insert oid peerId (orderMap s) })
              submitOrder bro order { orderId = oid }
              return $ ResponseOrderSubmitted oid

            Nothing -> return $ ResponseError "Unknown account"
        RequestCancelOrder sqnum oid -> do
          m <- orderToBroker <$> readIORef state
          case M.lookup oid m of
            Just bro -> do
              cancelOrder bro oid
              return $ ResponseOrderCancelled oid
            Nothing -> return $ ResponseError "Unknown order"
        RequestNotifications sqnum -> do
          maybeNs <- M.lookup peerId . pendingNotifications <$> readIORef state
          case maybeNs of
            Just ns -> do
              atomicMapIORef state (\s -> s { pendingNotifications = M.insert peerId [] (pendingNotifications s)})
              return $ ResponseNotifications . L.reverse $ ns
            Nothing -> return $ ResponseNotifications []

    sendMessage sock peerId resp = sendMulti sock (peerId :| [B.empty, BL.toStrict . encode $ resp])

    findBrokerForAccount account = L.find (L.elem account . accounts)
    nextOrderId = atomicModifyIORef' state (\s -> ( s {orderIdCounter = 1 + orderIdCounter s}, orderIdCounter s))


stopBrokerServer :: BrokerServerHandle -> IO ()
stopBrokerServer (BrokerServerHandle tid compMv killMv) = do
  putMVar killMv ()
  yield >> readMVar compMv

