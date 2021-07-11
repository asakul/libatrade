{-# LANGUAGE OverloadedStrings #-}

module ATrade.Broker.Server (
  startBrokerServer,
  stopBrokerServer,
  BrokerBackend(..),
  TradeSink
) where

import           ATrade.Broker.Backend
import           ATrade.Broker.Protocol
import           ATrade.Types
import           ATrade.Util
import           Control.Concurrent             hiding (readChan, writeChan)
import           Control.Concurrent.BoundedChan
import           Control.Exception
import           Control.Monad
import           Control.Monad.Loops
import           Data.Aeson
import qualified Data.Bimap                     as BM
import qualified Data.ByteString                as B hiding (putStrLn)
import qualified Data.ByteString.Lazy           as BL hiding (putStrLn)
import           Data.IORef
import qualified Data.List                      as L
import           Data.List.NonEmpty
import qualified Data.Map                       as M
import           Data.Maybe
import qualified Data.Text                      as T
import qualified Data.Text.Encoding             as E
import           Data.Time.Clock
import           System.Log.Logger
import           System.Timeout
import           System.ZMQ4
import           System.ZMQ4.ZAP

newtype OrderIdGenerator = IO OrderId
type PeerId = B.ByteString

data FullOrderId = FullOrderId ClientIdentity OrderId
  deriving (Show, Eq, Ord)

data BrokerServerState = BrokerServerState {
  bsSocket              :: Socket Router,
  bsNotificationsSocket :: Socket Pub,
  orderToBroker         :: M.Map FullOrderId BrokerBackend,
  orderMap              :: BM.Bimap FullOrderId OrderId,
  lastPacket            :: M.Map PeerId (RequestSqnum, BrokerServerResponse),
  pendingNotifications  :: M.Map ClientIdentity [Notification],
  notificationSqnum     :: M.Map ClientIdentity NotificationSqnum,
  brokers               :: [BrokerBackend],
  completionMvar        :: MVar (),
  killMvar              :: MVar (),
  orderIdCounter        :: OrderId,
  tradeSink             :: BoundedChan Trade
}

data BrokerServerHandle = BrokerServerHandle ThreadId ThreadId (MVar ()) (MVar ())

type TradeSink = Trade -> IO ()

startBrokerServer :: [BrokerBackend] -> Context -> T.Text -> T.Text -> [TradeSink] -> ServerSecurityParams -> IO BrokerServerHandle
startBrokerServer brokers c ep notificationsEp tradeSinks params = do
  sock <- socket c Router
  notificationsSock <- socket c Pub
  setLinger (restrict 0) sock
  setLinger (restrict 0) notificationsSock
  case sspDomain params of
    Just domain -> do
      setZapDomain (restrict $ E.encodeUtf8 domain) sock
      setZapDomain (restrict $ E.encodeUtf8 domain) notificationsSock
    Nothing     -> return ()
  case sspCertificate params of
    Just cert -> do
      setCurveServer True sock
      zapApplyCertificate cert sock
      setCurveServer True notificationsSock
      zapApplyCertificate cert notificationsSock
    Nothing -> return ()
  bind sock (T.unpack ep)
  bind notificationsSock (T.unpack notificationsEp)
  tid <- myThreadId
  compMv <- newEmptyMVar
  killMv <- newEmptyMVar
  tsChan <- newBoundedChan 100
  state <- newIORef BrokerServerState {
    bsSocket = sock,
    bsNotificationsSocket = notificationsSock,
    orderMap = BM.empty,
    orderToBroker = M.empty,
    lastPacket = M.empty,
    pendingNotifications = M.empty,
    notificationSqnum = M.empty,
    brokers = brokers,
    completionMvar = compMv,
    killMvar = killMv,
    orderIdCounter = 1,
    tradeSink = tsChan
  }
  mapM_ (\bro -> setNotificationCallback bro (Just $ notificationCallback state)) brokers

  debugM "Broker.Server" "Forking broker server thread"
  BrokerServerHandle <$> forkIO (brokerServerThread state) <*> forkIO (tradeSinkHandler c state tradeSinks) <*> pure compMv <*> pure killMv

notificationCallback :: IORef BrokerServerState -> BrokerBackendNotification -> IO ()
notificationCallback state n = do
  debugM "Broker.Server" $ "Notification: " ++ show n
  chan <- tradeSink <$> readIORef state
  case n of
    BackendTradeNotification trade -> tryWriteChan chan trade
    _                              -> return False
  orders <- orderMap <$> readIORef state
  case BM.lookupR (backendNotificationOrderId n) orders of
    Just (FullOrderId clientIdentity localOrderId) -> do
      sqnum <- atomicModifyIORef' state (\s -> let sqnum = M.findWithDefault (NotificationSqnum 1) clientIdentity (notificationSqnum s) in
                                                          (s { notificationSqnum = M.insert clientIdentity (nextSqnum sqnum) (notificationSqnum s) },
                                                           sqnum))
      case n of
        BackendTradeNotification trade -> addNotification clientIdentity (TradeNotification sqnum trade { tradeOrderId = localOrderId })
        BackendOrderNotification globalOrderId newstate -> addNotification clientIdentity (OrderNotification sqnum localOrderId newstate)
    Nothing -> warningM "Broker.Server" "Notification: unknown order"

  where
    addNotification clientIdentity n = do
      atomicMapIORef state (\s ->
        case M.lookup clientIdentity . pendingNotifications $ s of
          Just ns -> s { pendingNotifications = M.insert clientIdentity (n : ns) (pendingNotifications s)}
          Nothing -> s { pendingNotifications = M.insert clientIdentity [n] (pendingNotifications s)})
      sock <- bsNotificationsSocket <$> readIORef state
      sendMulti sock (E.encodeUtf8 clientIdentity :| [BL.toStrict $ encode n])

tradeSinkHandler :: Context -> IORef BrokerServerState -> [TradeSink] -> IO ()
tradeSinkHandler c state tradeSinks = unless (null tradeSinks) $
    whileM_ (not <$> wasKilled) $ do
      chan <- tradeSink <$> readIORef state
      maybeTrade <- tryReadChan chan
      case maybeTrade of
        Just trade -> mapM_ (\x -> x trade) tradeSinks
        Nothing    -> threadDelay 100000
    where
      wasKilled = isJust <$> (killMvar <$> readIORef state >>= tryReadMVar)


brokerServerThread :: IORef BrokerServerState -> IO ()
brokerServerThread state = finally brokerServerThread' cleanup
  where
    brokerServerThread' = whileM_ (fmap killMvar (readIORef state) >>= fmap isNothing . tryReadMVar) $ do
      sock <- bsSocket <$> readIORef state
      events <- poll 100 [Sock sock [In] Nothing]
      unless (null . L.head $ events) $ do
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
                  Just response -> do
                    debugM "Broker.Server" $ "Resending packet for peerId: " ++ show peerId
                    sendMessage sock peerId response -- Resend
                    atomicMapIORef state (\s -> s { lastPacket = M.delete peerId (lastPacket s)})
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
        RequestSubmitOrder sqnum clientIdentity order -> do
          debugM "Broker.Server" $ "Request: submit order:" ++ show request
          case findBrokerForAccount (orderAccountId order) bros of
            Just bro -> do
              globalOrderId <- nextOrderId
              let fullOrderId = (FullOrderId clientIdentity (orderId order))
              atomicMapIORef state (\s -> s {
                orderToBroker = M.insert fullOrderId bro (orderToBroker s),
                orderMap = BM.insert fullOrderId globalOrderId (orderMap s) })
              submitOrder bro order { orderId = globalOrderId }
              return $ ResponseOrderSubmitted (orderId order)

            Nothing -> do
              warningM "Broker.Server" $ "Unknown account: " ++ T.unpack (orderAccountId order)
              return $ ResponseError "Unknown account"
        RequestCancelOrder sqnum clientIdentity localOrderId -> do
          m <- orderToBroker <$> readIORef state
          bm <- orderMap <$> readIORef state
          let fullOrderId = FullOrderId clientIdentity localOrderId
          case (M.lookup fullOrderId m, BM.lookup fullOrderId bm) of
            (Just bro, Just globalOrderId) -> do
              cancelOrder bro globalOrderId
              return $ ResponseOrderCancelled localOrderId
            _ -> return $ ResponseError "Unknown order"
        RequestNotifications sqnum clientIdentity initialSqnum -> do
          maybeNs <- M.lookup clientIdentity . pendingNotifications <$> readIORef state
          case maybeNs of
            Just ns -> do
              let filtered = L.filter (\n -> getNotificationSqnum n >= initialSqnum) ns
              atomicMapIORef state (\s -> s { pendingNotifications = M.insert clientIdentity filtered (pendingNotifications s)})
              return $ ResponseNotifications . L.reverse $ filtered
            Nothing -> return $ ResponseNotifications []

    sendMessage sock peerId resp = sendMulti sock (peerId :| [B.empty, BL.toStrict . encode $ resp])

    findBrokerForAccount account = L.find (L.elem account . accounts)
    nextOrderId = atomicModifyIORef' state (\s -> ( s {orderIdCounter = 1 + orderIdCounter s}, orderIdCounter s))


stopBrokerServer :: BrokerServerHandle -> IO ()
stopBrokerServer (BrokerServerHandle tid tstid compMv killMv) = do
  putMVar killMv ()
  killThread tstid
  yield
  readMVar compMv
