
{-# LANGUAGE OverloadedStrings #-}

module TestBrokerClient (
  unitTests
) where

import Test.Tasty
import Test.Tasty.SmallCheck as SC
import Test.Tasty.QuickCheck as QC
import Test.Tasty.HUnit

import ATrade.Types
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import ATrade.Broker.Client
import ATrade.Broker.Server hiding (submitOrder, cancelOrder)
import ATrade.Broker.Protocol
import ATrade.Util
import qualified Data.Text as T
import Control.Monad
import Control.Monad.Loops
import Control.Concurrent.MVar
import Control.Concurrent.BoundedChan
import Control.Concurrent hiding (writeChan)
import Control.Exception
import System.ZMQ4
import Data.Aeson
import Data.Time.Clock
import Data.Time.Calendar
import Data.Maybe
import qualified Data.List as L
import Data.IORef
import Data.UUID as U
import Data.UUID.V4 as UV4
import MockBroker

unitTests = testGroup "Broker.Client" [
    testBrokerClientStartStop
  , testBrokerClientCancelOrder
  , testBrokerClientGetNotifications
  ]

makeEndpoint = do
  uid <- toText <$> UV4.nextRandom
  return $ "inproc://brokerserver" `T.append` uid

defaultOrder = mkOrder {
    orderAccountId = "demo",
    orderSecurity = "FOO",
    orderPrice = Market,
    orderQuantity = 10,
    orderOperation = Buy
  }

testBrokerClientStartStop = testCase "Broker client: submit order" $ withContext (\ctx -> do
  ep <- makeEndpoint
  (mockBroker, broState) <- mkMockBroker ["demo"]
  bracket (startBrokerServer [mockBroker] ctx ep "") stopBrokerServer (\broS ->
    bracket (startBrokerClient ctx ep) stopBrokerClient (\broC -> do
      oid <- submitOrder broC defaultOrder
      case oid of
        Left err -> assertFailure "Invalid response"
        Right _ -> return ())))

testBrokerClientCancelOrder = testCase "Broker client: submit and cancel order" $ withContext (\ctx -> do
  ep <- makeEndpoint
  (mockBroker, broState) <- mkMockBroker ["demo"]
  bracket (startBrokerServer [mockBroker] ctx ep "") stopBrokerServer (\broS ->
    bracket (startBrokerClient ctx ep) stopBrokerClient (\broC -> do
      maybeOid <- submitOrder broC defaultOrder
      case maybeOid of
        Left err -> assertFailure "Invalid response"
        Right oid -> do
          rc <- cancelOrder broC oid
          case rc of
            Left err -> assertFailure "Invalid response"
            Right _ -> return()
          )))

testBrokerClientGetNotifications = testCase "Broker client: get notifications" $ withContext (\ctx -> do
  ep <- makeEndpoint
  (mockBroker, broState) <- mkMockBroker ["demo"]
  bracket (startBrokerServer [mockBroker] ctx ep "") stopBrokerServer (\broS ->
    bracket (startBrokerClient ctx ep) stopBrokerClient (\broC -> do
      maybeOid <- submitOrder broC defaultOrder
      case maybeOid of
        Left err -> assertFailure "Invalid response"
        Right oid -> do
          maybeNs <- getNotifications broC
          case maybeNs of
            Left err -> assertFailure "Invalid response"
            Right ns -> 1 @=? length ns
          )))

