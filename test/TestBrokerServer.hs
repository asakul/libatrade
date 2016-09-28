{-# LANGUAGE OverloadedStrings #-}

module TestBrokerServer (
  unitTests
) where

import Test.Tasty
import Test.Tasty.SmallCheck as SC
import Test.Tasty.QuickCheck as QC
import Test.Tasty.HUnit

import ATrade.Types
import qualified Data.ByteString.Lazy as BL
import ATrade.Broker.Server
import ATrade.Broker.Protocol
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
import Data.IORef
import Data.UUID as U
import Data.UUID.V4 as UV4

data MockBrokerState = MockBrokerState {
  orders :: [Order],
  notificationCallback :: Maybe (Notification -> IO ())
}

mockSubmitOrder :: IORef MockBrokerState -> Order -> IO ()
mockSubmitOrder state order = do
  atomicModifyIORef' state (\s -> (s { orders = submittedOrder : orders s }, ()))
  maybeCb <- notificationCallback <$> readIORef state
  case maybeCb of
    Just cb -> cb $ OrderNotification (orderId order) Submitted
    Nothing -> return ()
  where
    submittedOrder = order { orderState = Submitted }

mockCancelOrder :: IORef MockBrokerState -> OrderId -> IO ()
mockCancelOrder state = undefined

mockStopBroker :: IORef MockBrokerState -> IO ()
mockStopBroker state = return ()


mkMockBroker accs = do
  state <- newIORef MockBrokerState {
    orders = [],
    notificationCallback = Nothing
  }

  return (BrokerInterface {
    accounts = accs,
    setNotificationCallback = \cb -> atomicModifyIORef' state (\s -> (s { notificationCallback = cb }, ())),
    submitOrder = mockSubmitOrder state,
    cancelOrder = mockCancelOrder state,
    stopBroker = mockStopBroker state
  }, state)


unitTests = testGroup "Broker.Server" [testBrokerServerStartStop, testBrokerServerSubmitOrder]

testBrokerServerStartStop = testCase "Broker Server starts and stops" $ withContext (\ctx -> do
  ep <- toText <$> UV4.nextRandom
  broS <- startBrokerServer [] ctx ("inproc://brokerserver" `T.append` ep)
  stopBrokerServer broS)

testBrokerServerSubmitOrder = testCase "Broker Server submits order" $ withContext (\ctx -> do
  uid <- toText <$> UV4.nextRandom
  (mockBroker, broState) <- mkMockBroker ["demo"]
  let ep = "inproc://brokerserver" `T.append` uid
  let order = mkOrder {
    orderAccountId = "demo",
    orderSecurity = "FOO",
    orderPrice = Market,
    orderQuantity = 10,
    orderOperation = Buy
  }
  bracket (startBrokerServer [mockBroker] ctx ep) stopBrokerServer (\broS -> 
    withSocket ctx Req (\sock -> do
      connect sock (T.unpack ep)
      send sock [] (BL.toStrict . encode $ RequestSubmitOrder 1 order)
      threadDelay 10000
      s <- readIORef broState
      (length . orders) s @?= 1
      )))
    
