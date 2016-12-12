{-# LANGUAGE OverloadedStrings #-}

module ATrade.Broker.Client (
  startBrokerClient,
  stopBrokerClient,
  submitOrder,
  cancelOrder,
  getNotifications
) where

import ATrade.Types
import ATrade.Broker.Protocol
import Control.Concurrent hiding (readChan, writeChan)
import Control.Concurrent.BoundedChan
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad
import Control.Monad.Loops
import Data.Aeson
import Data.Int
import Data.IORef
import Data.Maybe
import Data.List.NonEmpty
import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as BL
import Data.Text.Encoding
import System.ZMQ4
import System.Log.Logger
import System.Timeout

data BrokerClientHandle = BrokerClientHandle {
  tid :: ThreadId,
  completionMvar :: MVar (),
  killMvar :: MVar (),
  submitOrder :: Order -> IO (Either T.Text OrderId),
  cancelOrder :: OrderId -> IO (Either T.Text ()),
  getNotifications :: IO (Either T.Text [Notification]),
  cmdVar :: MVar BrokerServerRequest,
  respVar :: MVar BrokerServerResponse
}

brokerClientThread :: Context -> T.Text -> MVar BrokerServerRequest -> MVar BrokerServerResponse -> MVar () -> MVar () -> IO ()
brokerClientThread ctx ep cmd resp comp killMv = finally brokerClientThread' cleanup
  where
    cleanup = infoM "Broker.Client" "Quitting broker client thread" >> putMVar comp ()
    brokerClientThread' = whileM_ (isNothing <$> tryReadMVar killMv) $ do
      debugM "Broker.Client" "Starting event loop"
      handle (\e -> do
          warningM "Broker.Client" $ "Broker client: exception: " ++ (show (e :: SomeException)) ++ "; isZMQ: " ++ show (isZMQError e)
          if isZMQError e
            then do
              debugM "Broker.Client" "Rethrowing exception"
              throwIO e
            else do
              putMVar resp (ResponseError "Response error")) $ withSocket ctx Req (\sock -> do
        debugM "Broker.Client" $ "Connecting to: " ++ show (T.unpack ep)
        connect sock $ T.unpack ep
        debugM "Broker.Client" $ "Connected"
        isTimeout <- newIORef False
        
        whileM_ (andM [isNothing <$> tryReadMVar killMv, (== False) <$> readIORef isTimeout]) $ do
          request <- takeMVar cmd
          send sock [] (BL.toStrict $ encode request)
          incomingMessage <- timeout 1000000 $ receive sock
          case incomingMessage of
            Just msg -> case decode . BL.fromStrict $ msg of
              Just response -> putMVar resp response
              Nothing -> putMVar resp (ResponseError "Unable to decode response")
            Nothing -> do
              putMVar resp (ResponseError "Response timeout")
              writeIORef isTimeout True)
    isZMQError e = "ZMQError" `L.isPrefixOf` show e


startBrokerClient :: Context -> T.Text -> IO BrokerClientHandle
startBrokerClient ctx endpoint = do
  idCounter <- newIORef 1
  compMv <- newEmptyMVar
  killMv <- newEmptyMVar
  cmdVar <- newEmptyMVar :: IO (MVar BrokerServerRequest)
  respVar <- newEmptyMVar :: IO (MVar BrokerServerResponse)
  tid <- forkIO (brokerClientThread ctx endpoint cmdVar respVar compMv killMv)

  return BrokerClientHandle {
    tid = tid,
    completionMvar = compMv,
    killMvar = killMv,
    submitOrder = bcSubmitOrder idCounter cmdVar respVar,
    cancelOrder = bcCancelOrder idCounter cmdVar respVar,
    getNotifications = bcGetNotifications idCounter cmdVar respVar,
    cmdVar = cmdVar,
    respVar = respVar
  }

stopBrokerClient :: BrokerClientHandle -> IO ()
stopBrokerClient handle = putMVar (killMvar handle) () >> yield >> killThread (tid handle) >> readMVar (completionMvar handle)

nextId cnt = atomicModifyIORef' cnt (\v -> (v + 1, v))

bcSubmitOrder :: IORef Int64 -> MVar BrokerServerRequest -> MVar BrokerServerResponse -> Order -> IO (Either T.Text OrderId)
bcSubmitOrder idCounter cmdVar respVar order = do
  sqnum <- nextId idCounter
  putMVar cmdVar (RequestSubmitOrder sqnum order)
  resp <- takeMVar respVar
  case resp of
    (ResponseOrderSubmitted oid) -> return $ Right oid
    (ResponseError msg) -> return $ Left msg
    _ -> return $ Left "Unknown error"

bcCancelOrder :: IORef RequestSqnum -> MVar BrokerServerRequest -> MVar BrokerServerResponse -> OrderId -> IO (Either T.Text ())
bcCancelOrder idCounter cmdVar respVar orderId = do
  sqnum <- nextId idCounter
  putMVar cmdVar (RequestCancelOrder sqnum orderId)
  resp <- takeMVar respVar
  case resp of
    (ResponseOrderCancelled oid) -> return $ Right ()
    (ResponseError msg) -> return $ Left msg
    _ -> return $ Left "Unknown error"

bcGetNotifications :: IORef RequestSqnum -> MVar BrokerServerRequest -> MVar BrokerServerResponse -> IO (Either T.Text [Notification])
bcGetNotifications idCounter cmdVar respVar = do
  sqnum <- nextId idCounter
  putMVar cmdVar (RequestNotifications sqnum)
  resp <- takeMVar respVar
  case resp of
    (ResponseNotifications ns) -> return $ Right ns
    (ResponseError msg) -> return $ Left msg
    _ -> return $ Left "Unknown error"
