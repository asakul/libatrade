{-# LANGUAGE OverloadedStrings #-}

module ATrade.Broker.Server (
  startBrokerServer,
  stopBrokerServer,
  BrokerInterface(..)
) where

import ATrade.Types
import ATrade.Broker.Protocol
import System.ZMQ4
import System.ZMQ4.ZAP
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
import Control.Concurrent hiding (readChan, writeChan)
import Control.Concurrent.BoundedChan
import Control.Exception
import Control.Monad
import Control.Monad.Loops
import System.Log.Logger
import System.Timeout
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
  orderIdCounter :: OrderId,
  tradeSink :: BoundedChan Trade
}

data BrokerServerHandle = BrokerServerHandle ThreadId ThreadId (MVar ()) (MVar ())

startBrokerServer :: [BrokerInterface] -> Context -> T.Text -> T.Text -> Maybe CurveCertificate -> IO BrokerServerHandle
startBrokerServer brokers c ep tradeSinkEp maybeCert = do
  sock <- socket c Router
  case maybeCert of
    Just cert -> do
      setCurveServer True sock
      zapApplyCertificate cert sock
    Nothing -> return ()
  bind sock (T.unpack ep)
  tid <- myThreadId
  compMv <- newEmptyMVar
  killMv <- newEmptyMVar
  tsChan <- newBoundedChan 100
  state <- newIORef BrokerServerState {
    bsSocket = sock,
    orderMap = M.empty,
    orderToBroker = M.empty,
    lastPacket = M.empty,
    pendingNotifications = M.empty,
    brokers = brokers,
    completionMvar = compMv,
    killMvar = killMv,
    orderIdCounter = 1,
    tradeSink = tsChan
  }
  mapM_ (\bro -> setNotificationCallback bro (Just $ notificationCallback state)) brokers

  debugM "Broker.Server" "Forking broker server thread"
  BrokerServerHandle <$> forkIO (brokerServerThread state) <*> forkIO (tradeSinkHandler c state tradeSinkEp) <*> pure compMv <*> pure killMv

notificationCallback :: IORef BrokerServerState -> Notification -> IO ()
notificationCallback state n = do
  chan <- tradeSink <$> readIORef state
  case n of
    TradeNotification trade -> tryWriteChan chan trade
    _ -> return False
  orders <- orderMap <$> readIORef state
  case M.lookup (notificationOrderId n) orders of
    Just peerId -> addNotification peerId n
    Nothing -> warningM "Broker.Server" "Notification: unknown order"

  where
    addNotification peerId n = atomicMapIORef state (\s ->
      case M.lookup peerId . pendingNotifications $ s of
        Just ns -> s { pendingNotifications = M.insert peerId (n : ns) (pendingNotifications s)}
        Nothing -> s { pendingNotifications = M.insert peerId [n] (pendingNotifications s)})

tradeSinkHandler :: Context -> IORef BrokerServerState -> T.Text -> IO ()
tradeSinkHandler c state tradeSinkEp = when (tradeSinkEp /= "") $
    whileM_ (not <$> wasKilled) $
      withSocket c Dealer (\sock -> do
        chan <- tradeSink <$> readIORef state
        connect sock $ T.unpack tradeSinkEp
        timeoutMv <- newEmptyMVar
        threadDelay 1000000
        whileM_ (andM [not <$> wasKilled, isNothing <$> tryReadMVar timeoutMv]) $ do
          maybeTrade <- tryReadChan chan
          case maybeTrade of
            Just trade -> sendMulti sock $ B.empty :| [encodeTrade trade]
            Nothing -> do
              threadDelay 1000000
              sendMulti sock $ B.empty :| [BL.toStrict $ encode TradeSinkHeartBeat]
              events <- poll 5000 [Sock sock [In] Nothing]
              if not . L.null . L.head $ events
                then void . receive $ sock -- anything will do
                else putMVar timeoutMv ())

    where
      wasKilled = fmap killMvar (readIORef state) >>= fmap isJust . tryReadMVar
      encodeTrade :: Trade -> B.ByteString
      encodeTrade = BL.toStrict . encode . convertTrade
      convertTrade trade = TradeSinkTrade {
        tsAccountId = tradeAccount trade,
        tsSecurity = tradeSecurity trade,
        tsPrice = fromRational . toRational . tradePrice $ trade,
        tsQuantity = fromInteger $ tradeQuantity trade,
        tsVolume = fromRational . toRational . tradeVolume $ trade,
        tsCurrency = tradeVolumeCurrency trade,
        tsOperation = tradeOperation trade,
        tsExecutionTime = tradeTimestamp trade,
        tsSignalId = tradeSignalId trade
      }

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
        RequestSubmitOrder sqnum order -> do
          debugM "Broker.Server" $ "Request: submit order:" ++ show request
          case findBrokerForAccount (orderAccountId order) bros of
            Just bro -> do
              oid <- nextOrderId
              atomicMapIORef state (\s -> s {
                orderToBroker = M.insert oid bro (orderToBroker s),
                orderMap = M.insert oid peerId (orderMap s) })
              submitOrder bro order { orderId = oid }
              return $ ResponseOrderSubmitted oid

            Nothing -> do
              debugM "Broker.Server" $ "Unknown account: " ++ T.unpack (orderAccountId order)
              return $ ResponseError "Unknown account"
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
stopBrokerServer (BrokerServerHandle tid tstid compMv killMv) = do
  putMVar killMv ()
  killThread tstid
  yield
  readMVar compMv
