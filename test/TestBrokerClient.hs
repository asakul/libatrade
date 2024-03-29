
{-# LANGUAGE OverloadedStrings #-}

module TestBrokerClient (
  unitTests
) where

import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck          as QC
import           Test.Tasty.SmallCheck          as SC

import           ATrade.Broker.Client
import           ATrade.Broker.Protocol
import           ATrade.Broker.Server           hiding (cancelOrder,
                                                 submitOrder)
import           ATrade.Types
import ATrade.Logging (emptyLogger)
import           ATrade.Util
import           Control.Concurrent             hiding (writeChan)
import           Control.Concurrent.BoundedChan
import           Control.Concurrent.MVar
import           Control.Exception
import           Control.Monad
import           Control.Monad.Loops
import           Data.Aeson
import qualified Data.ByteString                as B
import qualified Data.ByteString.Lazy           as BL
import           Data.IORef
import qualified Data.List                      as L
import           Data.Maybe
import qualified Data.Text                      as T
import           Data.Time.Calendar
import           Data.Time.Clock
import           Data.UUID                      as U
import           Data.UUID.V4                   as UV4
import           MockBroker
import           System.ZMQ4
import           System.ZMQ4.ZAP

unitTests = testGroup "Broker.Client" [
    testBrokerClientStartStop
  , testBrokerClientCancelOrder
  , testBrokerClientGetNotifications
  ]

makeEndpoints = do
  uid <- toText <$> UV4.nextRandom
  return ("inproc://brokerserver-" `T.append` uid, "inproc://brokerserver-notifications-" `T.append` uid)

defaultOrder = mkOrder {
    orderAccountId = "demo",
    orderSecurity = "FOO",
    orderPrice = Market,
    orderQuantity = 10,
    orderOperation = Buy
  }

makeNotificationCallback :: IO (IORef [Notification], NotificationCallback)
makeNotificationCallback = do
  ref <- newIORef []
  return (ref, \n -> atomicModifyIORef' ref (\s -> (n : s, ())))

testBrokerClientStartStop = testCase "Broker client: submit order" $ withContext (\ctx -> do
  (ep, notifEp) <- makeEndpoints
  (ref, callback) <- makeNotificationCallback
  (mockBroker, broState) <- mkMockBroker ["demo"]
  bracket (startBrokerServer [mockBroker] ctx ep notifEp [] defaultServerSecurityParams emptyLogger) stopBrokerServer (\broS ->
    bracket (startBrokerClient "foo" ctx ep notifEp [callback] defaultClientSecurityParams emptyLogger) stopBrokerClient (\broC -> do
      result <- submitOrder broC defaultOrder
      case result of
        Left err -> assertFailure "Invalid response"
        Right _  -> do
          threadDelay 10000 -- Wait for callback
          notifs <- readIORef ref
          case head notifs of
            OrderNotification _ oid newState -> do
              newState @=? Submitted
              oid @=? orderId defaultOrder
            _ -> assertFailure "Invalid notification"
          )))

testBrokerClientCancelOrder = testCase "Broker client: submit and cancel order" $ withContext (\ctx -> do
  (ep, notifEp) <- makeEndpoints
  (ref, callback) <- makeNotificationCallback
  (mockBroker, broState) <- mkMockBroker ["demo"]
  bracket (startBrokerServer [mockBroker] ctx ep notifEp [] defaultServerSecurityParams emptyLogger) stopBrokerServer (\broS ->
    bracket (startBrokerClient "foo" ctx ep notifEp [callback] defaultClientSecurityParams emptyLogger) stopBrokerClient (\broC -> do
      maybeOid <- submitOrder broC defaultOrder
      case maybeOid of
        Left err -> assertFailure "Invalid response"
        Right _ -> do
          rc <- cancelOrder broC (orderId defaultOrder)
          case rc of
            Left err -> assertFailure "Invalid response"
            Right _  -> return()
          )))

testBrokerClientGetNotifications = testCase "Broker client: get notifications" $ withContext (\ctx -> do
  (ep, notifEp) <- makeEndpoints
  (ref, callback) <- makeNotificationCallback
  (mockBroker, broState) <- mkMockBroker ["demo"]
  bracket (startBrokerServer [mockBroker] ctx ep notifEp [] defaultServerSecurityParams emptyLogger) stopBrokerServer (\broS ->
    bracket (startBrokerClient "foo" ctx ep notifEp [callback] defaultClientSecurityParams emptyLogger) stopBrokerClient (\broC -> do
      maybeOid <- submitOrder broC defaultOrder
      case maybeOid of
        Left err -> assertFailure "Invalid response"
        Right oid -> do
          maybeNs <- getNotifications broC
          case maybeNs of
            Left err -> assertFailure "Invalid response"
            Right ns -> 1 @=? length ns
          )))

