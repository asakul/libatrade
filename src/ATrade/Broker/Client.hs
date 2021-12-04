{-# LANGUAGE OverloadedStrings #-}

module ATrade.Broker.Client (
  startBrokerClient,
  stopBrokerClient,
  BrokerClientHandle,
  submitOrder,
  cancelOrder,
  getNotifications,
  NotificationCallback(..)
) where

import           ATrade.Broker.Protocol         (BrokerServerRequest (..),
                                                 BrokerServerResponse (..),
                                                 ClientIdentity, Notification,
                                                 NotificationSqnum (NotificationSqnum),
                                                 RequestSqnum,
                                                 getNotificationSqnum,
                                                 nextSqnum)
import           ATrade.Logging                 (Message,
                                                 Severity (Debug, Info, Warning),
                                                 logWith)
import           ATrade.Types                   (ClientSecurityParams (cspCertificate, cspServerCertificate),
                                                 Order, OrderId)
import           Colog                          (LogAction)
import           Control.Concurrent             (MVar, ThreadId, forkIO,
                                                 killThread, newEmptyMVar,
                                                 putMVar, readMVar, takeMVar,
                                                 threadDelay, tryReadMVar,
                                                 yield)
import           Control.Concurrent.BoundedChan ()
import           Control.Concurrent.MVar        ()
import           Control.Exception              (SomeException, finally, handle,
                                                 throwIO)
import           Control.Monad                  (forM_, when)
import           Control.Monad.Loops            (andM, whileM_)
import           Data.Aeson                     (decode, encode)
import qualified Data.ByteString                as B
import qualified Data.ByteString.Lazy           as BL
import           Data.Int                       (Int64)
import           Data.IORef                     (IORef, atomicModifyIORef',
                                                 atomicWriteIORef, newIORef,
                                                 readIORef, writeIORef)
import qualified Data.List                      as L
import           Data.List.NonEmpty             ()
import           Data.Maybe                     (isNothing)
import qualified Data.Text                      as T
import           Data.Text.Encoding             (decodeUtf8)
import qualified Data.Text.Encoding             as T
import           Safe                           (lastMay)
import           System.Timeout                 (timeout)
import           System.ZMQ4                    (Context, Event (In),
                                                 Poll (Sock), Req (Req),
                                                 Sub (Sub), Switch (On),
                                                 connect, poll, receive,
                                                 receiveMulti, restrict, send,
                                                 setLinger, setTcpKeepAlive,
                                                 setTcpKeepAliveCount,
                                                 setTcpKeepAliveIdle,
                                                 setTcpKeepAliveInterval,
                                                 subscribe, withSocket)
import           System.ZMQ4.ZAP                (zapApplyCertificate,
                                                 zapSetServerCertificate)

type NotificationCallback = Notification -> IO ()

data BrokerClientHandle = BrokerClientHandle {
  tid                   :: ThreadId,
  completionMvar        :: MVar (),
  killMvar              :: MVar (),
  submitOrder           :: Order -> IO (Either T.Text ()),
  cancelOrder           :: OrderId -> IO (Either T.Text ()),
  getNotifications      :: IO (Either T.Text [Notification]),
  cmdVar                :: MVar (BrokerServerRequest, MVar BrokerServerResponse),
  lastKnownNotificationRef :: IORef NotificationSqnum,
  notificationCallback :: [NotificationCallback],
  notificationThreadId :: ThreadId
}

brokerClientThread :: B.ByteString ->
  Context ->
  T.Text ->
  MVar (BrokerServerRequest, MVar BrokerServerResponse) ->
  MVar () ->
  MVar () ->
  ClientSecurityParams ->
  LogAction IO Message ->
  IO ()
brokerClientThread socketIdentity ctx ep cmd comp killMv secParams logger = finally brokerClientThread' cleanup
  where
    log = logWith logger
    cleanup = log Info "Broker.Client" "Quitting broker client thread" >> putMVar comp ()
    brokerClientThread' = whileM_ (isNothing <$> tryReadMVar killMv) $ do
      log Debug "Broker.Client" "Starting event loop"
      handle (\e -> do
          log Warning "Broker.Client" $ "Broker client: exception: " <> (T.pack . show) (e :: SomeException) <> "; isZMQ: " <> (T.pack . show) (isZMQError e)
          if isZMQError e
            then do
              log Debug "Broker.Client" "Rethrowing exception"
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
        log Debug "Broker.Client" "Connected"
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


notificationThread :: ClientIdentity ->
                      [NotificationCallback] ->
                      Context ->
                      T.Text ->
                      IORef RequestSqnum ->
                      MVar (BrokerServerRequest, MVar BrokerServerResponse) ->
                      MVar () ->
                      ClientSecurityParams ->
                      LogAction IO Message ->
                      IO ()
notificationThread clientIdentity callbacks ctx ep idCounter cmdVar killMv secParams logger = flip finally (return ()) $ do
  whileM_ (isNothing <$> tryReadMVar killMv) $
    withSocket ctx Sub $ \sock -> do
      setLinger (restrict 0) sock
      case cspCertificate secParams of
        Just clientCert -> zapApplyCertificate clientCert sock
        Nothing         -> return ()
      case cspServerCertificate secParams of
        Just serverCert -> zapSetServerCertificate serverCert sock
        Nothing         -> return ()
      setTcpKeepAlive On sock
      setTcpKeepAliveCount (restrict 5) sock
      setTcpKeepAliveIdle (restrict 60) sock
      setTcpKeepAliveInterval (restrict 10) sock
      connect sock $ T.unpack ep
      log Debug "Broker.Client" $ "Subscribing: [" <> clientIdentity <> "]"
      subscribe sock $ T.encodeUtf8 clientIdentity

      initialSqnum <- requestCurrentSqnum cmdVar idCounter clientIdentity

      notifSqnumRef <- newIORef initialSqnum
      whileM_ (isNothing <$> tryReadMVar killMv) $ do
        evs <- poll 5000 [Sock sock [In] Nothing]
        if null . L.head $ evs
          then do
            respVar <- newEmptyMVar
            sqnum <- nextId idCounter
            notifSqnum <- readIORef notifSqnumRef
            putMVar cmdVar (RequestNotifications sqnum clientIdentity notifSqnum, respVar)
            resp <- takeMVar respVar
            case resp of
              (ResponseNotifications ns) -> do
                forM_ ns $ \notif -> do
                  lastSqnum <- readIORef notifSqnumRef
                  when (getNotificationSqnum notif >= lastSqnum) $ do
                    forM_ callbacks $ \c -> c notif
                    atomicWriteIORef notifSqnumRef (nextSqnum lastSqnum)
              (ResponseError msg)        -> log Warning "Broker.Client" $ "ResponseError: " <> msg
              _                          -> log Warning "Broker.Client" "Unknown error when requesting notifications"
          else do
            msg <- receiveMulti sock
            case msg of
              [_, payload] -> case decode (BL.fromStrict payload) of
                Just notification -> do
                  currentSqnum <- readIORef notifSqnumRef
                  if getNotificationSqnum notification /= currentSqnum
                    then
                      if currentSqnum > getNotificationSqnum notification
                        then log Debug "Broker.Client" $ "Already processed notification: " <> (T.pack . show) (getNotificationSqnum notification)
                        else log Warning "Broker.Client" $
                             "Notification sqnum mismatch: " <> (T.pack . show) currentSqnum <> " -> " <> (T.pack . show) (getNotificationSqnum notification)
                    else do
                      atomicWriteIORef notifSqnumRef (nextSqnum currentSqnum)
                      forM_ callbacks $ \c -> c notification
                _                 -> return ()
              _ -> return ()
  where
    log = logWith logger
    requestCurrentSqnum cmdVar idCounter clientIdentity = do
      respVar <- newEmptyMVar
      sqnum <- nextId idCounter
      putMVar cmdVar (RequestCurrentSqnum sqnum clientIdentity, respVar)
      resp <- takeMVar respVar
      case resp of
        (ResponseCurrentSqnum sqnum) -> return sqnum
        (ResponseError msg)        -> do
          log Warning "Broker.Client" $ "ResponseError: " <> msg
          return (NotificationSqnum 1)
        _                          -> do
          log Warning "Broker.Client" "Unknown error when requesting notifications"
          return (NotificationSqnum 1)


startBrokerClient :: B.ByteString -- ^ Socket Identity
  -> Context -- ^ ZeroMQ context
  -> T.Text -- ^ Broker endpoing
  -> T.Text -- ^ Notification endpoing
  -> [NotificationCallback] -- ^ List of notification callbacks
  -> ClientSecurityParams -- ^
  -> LogAction IO Message
  -> IO BrokerClientHandle
startBrokerClient socketIdentity ctx endpoint notifEndpoint notificationCallbacks secParams logger = do
  idCounter <- newIORef 1
  compMv <- newEmptyMVar
  killMv <- newEmptyMVar
  cmdVar <- newEmptyMVar :: IO (MVar (BrokerServerRequest, MVar BrokerServerResponse))
  tid <- forkIO (brokerClientThread socketIdentity ctx endpoint cmdVar compMv killMv secParams logger)
  notifSqnumRef <- newIORef (NotificationSqnum 0)
  notifThreadId <- forkIO (notificationThread (T.decodeUtf8 socketIdentity) notificationCallbacks ctx notifEndpoint idCounter cmdVar killMv secParams logger)

  return BrokerClientHandle {
    tid = tid,
    completionMvar = compMv,
    killMvar = killMv,
    submitOrder = bcSubmitOrder (decodeUtf8 socketIdentity) idCounter cmdVar,
    cancelOrder = bcCancelOrder (decodeUtf8 socketIdentity) idCounter cmdVar,
    getNotifications = bcGetNotifications (decodeUtf8 socketIdentity) idCounter notifSqnumRef cmdVar,
    cmdVar = cmdVar,
    lastKnownNotificationRef = notifSqnumRef,
    notificationCallback = [],
    notificationThreadId = notifThreadId
  }

stopBrokerClient :: BrokerClientHandle -> IO ()
stopBrokerClient handle = do
  putMVar (killMvar handle) ()
  yield
  killThread (tid handle)
  killThread (notificationThreadId handle)
  yield
  readMVar (completionMvar handle)

nextId cnt = atomicModifyIORef' cnt (\v -> (v + 1, v))

bcSubmitOrder :: ClientIdentity -> IORef Int64 -> MVar (BrokerServerRequest, MVar BrokerServerResponse) -> Order -> IO (Either T.Text ())
bcSubmitOrder clientIdentity idCounter cmdVar order = do
  respVar <- newEmptyMVar
  sqnum <- nextId idCounter
  putMVar cmdVar (RequestSubmitOrder sqnum clientIdentity order, respVar)
  resp <- takeMVar respVar
  case resp of
    ResponseOk          -> return $ Right ()
    (ResponseError msg) -> return $ Left msg
    _                   -> return $ Left "Unknown error"

bcCancelOrder :: ClientIdentity -> IORef RequestSqnum -> MVar (BrokerServerRequest, MVar BrokerServerResponse) -> OrderId -> IO (Either T.Text ())
bcCancelOrder clientIdentity idCounter cmdVar orderId = do
  respVar <- newEmptyMVar
  sqnum <- nextId idCounter
  putMVar cmdVar (RequestCancelOrder sqnum clientIdentity orderId, respVar)
  resp <- takeMVar respVar
  case resp of
    ResponseOk          -> return $ Right ()
    (ResponseError msg) -> return $ Left msg
    _                   -> return $ Left "Unknown error"

bcGetNotifications :: ClientIdentity ->
                      IORef RequestSqnum ->
                      IORef NotificationSqnum ->
                      MVar (BrokerServerRequest, MVar BrokerServerResponse) ->
                      IO (Either T.Text [Notification])
bcGetNotifications clientIdentity idCounter notifSqnumRef cmdVar = do
  respVar <- newEmptyMVar
  sqnum <- nextId idCounter
  notifSqnum <- nextSqnum <$> readIORef notifSqnumRef
  putMVar cmdVar (RequestNotifications sqnum clientIdentity notifSqnum, respVar)
  resp <- takeMVar respVar
  case resp of
    (ResponseNotifications ns) -> do
      case lastMay ns of
        Just n  -> atomicWriteIORef notifSqnumRef (getNotificationSqnum n)
        Nothing -> return ()
      return $ Right ns
    (ResponseError msg)        -> return $ Left msg
    _                          -> return $ Left "Unknown error"
