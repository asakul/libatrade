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
import Data.Aeson
import Data.Int
import Data.IORef
import Data.List.NonEmpty
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as BL
import Data.Text.Encoding
import System.ZMQ4
import System.Log.Logger

data BrokerClientHandle = BrokerClientHandle {
  tid :: ThreadId,
  completionMvar :: MVar (),
  submitOrder :: Order -> IO (Either T.Text OrderId),
  cancelOrder :: OrderId -> IO (Either T.Text ()),
  getNotifications :: IO (Either T.Text [Notification]),
  cmdVar :: MVar BrokerServerRequest,
  respVar :: MVar BrokerServerResponse
}

brokerClientThread ctx ep cmd resp comp = do
  sock <- socket ctx Req
  connect sock $ T.unpack ep
  finally (brokerClientThread' sock) (cleanup sock)
  where
    cleanup sock = close sock >> putMVar comp ()
    brokerClientThread' sock = do
      forever $ do
        request <- takeMVar cmd
        send sock [] (BL.toStrict $ encode request)
        maybeResponse <- decode . BL.fromStrict <$> receive sock
        case maybeResponse of
          Just response -> putMVar resp response
          Nothing -> putMVar resp (ResponseError "Unable to decode response")

startBrokerClient :: Context -> T.Text -> IO BrokerClientHandle
startBrokerClient ctx endpoint = do
  idCounter <- newIORef 1
  compMv <- newEmptyMVar
  cmdVar <- newEmptyMVar :: IO (MVar BrokerServerRequest)
  respVar <- newEmptyMVar :: IO (MVar BrokerServerResponse)
  tid <- forkIO (brokerClientThread ctx endpoint cmdVar respVar compMv)

  return BrokerClientHandle {
    tid = tid,
    completionMvar = compMv,
    submitOrder = bcSubmitOrder idCounter cmdVar respVar,
    cancelOrder = bcCancelOrder idCounter cmdVar respVar,
    getNotifications = bcGetNotifications idCounter cmdVar respVar,
    cmdVar = cmdVar,
    respVar = respVar
  }

stopBrokerClient :: BrokerClientHandle -> IO ()
stopBrokerClient handle = yield >> killThread (tid handle) >> readMVar (completionMvar handle)

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