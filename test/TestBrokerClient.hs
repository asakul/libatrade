
{-# LANGUAGE OverloadedStrings #-}

module TestBrokerServer (
  unitTests
) where

import Test.Tasty
import Test.Tasty.SmallCheck as SC
import Test.Tasty.QuickCheck as QC
import Test.Tasty.HUnit

import ATrade.Types
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import ATrade.Broker.Server
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

unitTests = testGroup "Broker.Client" []

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

testBrokerClientStartStop = testCase "Broker client starts and stops" $ withContext (\ctx -> do
  ep <- makeEndpoint
  (mockBroker, broState) <- mkMockBroker ["demo"]
  bracket (startBrokerServer [mockBroker] ctx ep) stopBrokerServer (\broS ->
    bracket (startBrokerClient ctx ep) stopBrokerClient (\broC ->
      oid <- submitOrder broC defaultOrder
  )))

