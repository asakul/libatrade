{-# LANGUAGE OverloadedStrings #-}

module ATrade.Broker.Client (
  startBrokerClient,
  stopBrokerClient,
  submitOrder,
  cancelOrder,
  getNotifications
) where

import           ATrade.Broker.Protocol
import           ATrade.Types
import           Control.Concurrent             hiding (readChan, writeChan)
import           Control.Concurrent.BoundedChan
import           Control.Concurrent.MVar
import           Control.Exception
import           Control.Monad
import           Control.Monad.Loops
import           Data.Aeson
import qualified Data.ByteString                as B
import qualified Data.ByteString.Lazy           as BL
import           Data.Int
import           Data.IORef
import qualified Data.List                      as L
import           Data.List.NonEmpty
import           Data.Maybe
import qualified Data.Text                      as T
import           Data.Text.Encoding
import           System.Log.Logger
import           System.Timeout
import           System.ZMQ4
import           System.ZMQ4.ZAP

data BrokerClientHandle = BrokerClientHandle {
  tid                   :: ThreadId,
  completionMvar        :: MVar (),
  killMvar              :: MVar (),
  submitOrder           :: Order -> IO (Either T.Text OrderId),
  cancelOrder           :: OrderId -> IO (Either T.Text ()),
  getNotifications      :: IO (Either T.Text [Notification]),
  cmdVar                :: MVar (BrokerServerRequest, MVar BrokerServerResponse),
  lastKnownNotificationRef :: IORef NotificationSqnum
}

brokerClientThread :: B.ByteString -> Context -> T.Text -> MVar (BrokerServerRequest, MVar BrokerServerResponse) -> MVar () -> MVar () -> ClientSecurityParams -> IO ()
brokerClientThread socketIdentity ctx ep cmd comp killMv secParams = finally brokerClientThread' cleanup
  where
    cleanup = infoM "Broker.Client" "Quitting broker client thread" >> putMVar comp ()
    brokerClientThread' = whileM_ (isNothing <$> tryReadMVar killMv) $ do
      debugM "Broker.Client" "Starting event loop"
      handle (\e -> do
          warningM "Broker.Client" $ "Broker client: exception: " ++ show (e :: SomeException) ++ "; isZMQ: " ++ show (isZMQError e)
          if isZMQError e
            then do
              debugM "Broker.Client" "Rethrowing exception"
              throwIO e
            else do
              return ()) $ withSocket ctx Req (\sock -> do
        setLinger (restrict 0) sock

        case cspCertificate secParams of
          Just clientCert -> zapApplyCertificate clientCert sock
          Nothing         -> return ()
        case cspServerCertificate secParams of
          Just serverCert -> zapSetServerCertificate serverCert sock
          Nothing         -> return ()

        connect sock $ T.unpack ep
        debugM "Broker.Client" "Connected"
        isTimeout <- newIORef False

        whileM_ (andM [isNothing <$> tryReadMVar killMv, (== False) <$> readIORef isTimeout]) $ do
          (request, resp) <- takeMVar cmd
          send sock [] (BL.toStrict $ encode request)
          incomingMessage <- timeout 5000000 $ receive sock
          case incomingMessage of
            Just msg -> case decode . BL.fromStrict $ msg of
              Just response -> putMVar resp response
              Nothing -> putMVar resp (ResponseError "Unable to decode response")
            Nothing -> do
              putMVar resp (ResponseError "Response timeout")
              writeIORef isTimeout True
        threadDelay 1000000)
    isZMQError e = "ZMQError" `L.isPrefixOf` show e

startBrokerClient :: B.ByteString -> Context -> T.Text -> ClientSecurityParams -> IO BrokerClientHandle
startBrokerClient socketIdentity ctx endpoint secParams = do
  idCounter <- newIORef 1
  compMv <- newEmptyMVar
  killMv <- newEmptyMVar
  cmdVar <- newEmptyMVar :: IO (MVar (BrokerServerRequest, MVar BrokerServerResponse))
  tid <- forkIO (brokerClientThread socketIdentity ctx endpoint cmdVar compMv killMv secParams)
  notifSqnumRef <- newIORef (NotificationSqnum 0)

  return BrokerClientHandle {
    tid = tid,
    completionMvar = compMv,
    killMvar = killMv,
    submitOrder = bcSubmitOrder (decodeUtf8 socketIdentity) idCounter cmdVar,
    cancelOrder = bcCancelOrder (decodeUtf8 socketIdentity) idCounter cmdVar,
    getNotifications = bcGetNotifications (decodeUtf8 socketIdentity) idCounter notifSqnumRef cmdVar,
    cmdVar = cmdVar,
    lastKnownNotificationRef = notifSqnumRef
  }

stopBrokerClient :: BrokerClientHandle -> IO ()
stopBrokerClient handle = putMVar (killMvar handle) () >> yield >> killThread (tid handle) >> readMVar (completionMvar handle)

nextId cnt = atomicModifyIORef' cnt (\v -> (v + 1, v))

bcSubmitOrder :: ClientIdentity -> IORef Int64 -> MVar (BrokerServerRequest, MVar BrokerServerResponse) -> Order -> IO (Either T.Text OrderId)
bcSubmitOrder clientIdentity idCounter cmdVar order = do
  respVar <- newEmptyMVar
  sqnum <- nextId idCounter
  putMVar cmdVar (RequestSubmitOrder sqnum clientIdentity order, respVar)
  resp <- takeMVar respVar
  case resp of
    (ResponseOrderSubmitted oid) -> return $ Right oid
    (ResponseError msg)          -> return $ Left msg
    _                            -> return $ Left "Unknown error"

bcCancelOrder :: ClientIdentity -> IORef RequestSqnum -> MVar (BrokerServerRequest, MVar BrokerServerResponse) -> OrderId -> IO (Either T.Text ())
bcCancelOrder clientIdentity idCounter cmdVar orderId = do
  respVar <- newEmptyMVar
  sqnum <- nextId idCounter
  putMVar cmdVar (RequestCancelOrder sqnum clientIdentity orderId, respVar)
  resp <- takeMVar respVar
  case resp of
    (ResponseOrderCancelled oid) -> return $ Right ()
    (ResponseError msg)          -> return $ Left msg
    _                            -> return $ Left "Unknown error"

bcGetNotifications :: ClientIdentity -> IORef RequestSqnum -> IORef NotificationSqnum -> MVar (BrokerServerRequest, MVar BrokerServerResponse) -> IO (Either T.Text [Notification])
bcGetNotifications clientIdentity idCounter notifSqnumRef cmdVar = do
  respVar <- newEmptyMVar
  sqnum <- nextId idCounter
  notifSqnum <- nextSqnum <$> readIORef notifSqnumRef
  putMVar cmdVar (RequestNotifications sqnum clientIdentity notifSqnum, respVar)
  resp <- takeMVar respVar
  case resp of
    (ResponseNotifications ns) -> return $ Right ns
    (ResponseError msg)        -> return $ Left msg
    _                          -> return $ Left "Unknown error"
