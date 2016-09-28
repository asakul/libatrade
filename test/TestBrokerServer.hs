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

data MockBrokerState = MockBrokerState {
  orders :: [Order],
  cancelledOrders :: [Order],
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

mockCancelOrder :: IORef MockBrokerState -> OrderId -> IO Bool
mockCancelOrder state oid = do
  ors <- orders <$> readIORef state
  case L.find (\o -> orderId o == oid) ors of
    Just order -> atomicModifyIORef' state (\s -> (s { cancelledOrders = order : cancelledOrders s}, True))
    Nothing -> return False

mockStopBroker :: IORef MockBrokerState -> IO ()
mockStopBroker state = return ()


mkMockBroker accs = do
  state <- newIORef MockBrokerState {
    orders = [],
    cancelledOrders = [],
    notificationCallback = Nothing
  }

  return (BrokerInterface {
    accounts = accs,
    setNotificationCallback = \cb -> atomicModifyIORef' state (\s -> (s { notificationCallback = cb }, ())),
    submitOrder = mockSubmitOrder state,
    cancelOrder = mockCancelOrder state,
    stopBroker = mockStopBroker state
  }, state)


unitTests = testGroup "Broker.Server" [testBrokerServerStartStop
  , testBrokerServerSubmitOrder
  , testBrokerServerSubmitOrderToUnknownAccount
  , testBrokerServerCancelOrder
  , testBrokerServerCancelUnknownOrder
  , testBrokerServerCorruptedPacket ]

testBrokerServerStartStop = testCase "Broker Server starts and stops" $ withContext (\ctx -> do
  ep <- toText <$> UV4.nextRandom
  broS <- startBrokerServer [] ctx ("inproc://brokerserver" `T.append` ep)
  stopBrokerServer broS)

makeEndpoint = do
  uid <- toText <$> UV4.nextRandom
  return $ "inproc://brokerserver" `T.append` uid

connectAndSendOrder step sock order ep = do
  step "Connecting"
  connect sock (T.unpack ep)

  step "Sending request"
  send sock [] (BL.toStrict . encode $ RequestSubmitOrder 1 order)
  threadDelay 10000

defaultOrder = mkOrder {
    orderAccountId = "demo",
    orderSecurity = "FOO",
    orderPrice = Market,
    orderQuantity = 10,
    orderOperation = Buy
  }


testBrokerServerSubmitOrder = testCaseSteps "Broker Server submits order" $ \step -> withContext (\ctx -> do
  step "Setup"
  (mockBroker, broState) <- mkMockBroker ["demo"]
  ep <- makeEndpoint
  bracket (startBrokerServer [mockBroker] ctx ep) stopBrokerServer (\broS ->
    withSocket ctx Req (\sock -> do
      connectAndSendOrder step sock defaultOrder ep

      step "Checking that order is submitted to BrokerInterface"
      s <- readIORef broState
      (length . orders) s @?= 1

      step "Reading response"
      resp <- decode . BL.fromStrict <$> receive sock
      case resp of
        Just (ResponseOrderSubmitted _) -> return ()
        Just _ -> assertFailure "Invalid response"
        Nothing -> assertFailure "Invalid response"

      )))

testBrokerServerSubmitOrderToUnknownAccount = testCaseSteps "Broker Server returns error if account is unknown" $
  \step -> withContext (\ctx -> do
    step "Setup"
    ep <- makeEndpoint
    (mockBroker, broState) <- mkMockBroker ["demo"]
    bracket (startBrokerServer [mockBroker] ctx ep) stopBrokerServer (\broS ->
      withSocket ctx Req (\sock -> do
        connectAndSendOrder step sock (defaultOrder { orderAccountId = "foobar" }) ep

        step "Reading response"
        resp <- decode . BL.fromStrict <$> receive sock
        case resp of
          Just (ResponseError _) -> return ()
          Just _ -> assertFailure "Invalid response"
          Nothing -> assertFailure "Invalid response"

        )))

testBrokerServerCancelOrder = testCaseSteps "Broker Server: submitted order cancellation" $
  \step -> withContext (\ctx -> do
    step "Setup"
    ep <- makeEndpoint
    (mockBroker, broState) <- mkMockBroker ["demo"]
    bracket (startBrokerServer [mockBroker] ctx ep) stopBrokerServer (\broS ->
      withSocket ctx Req (\sock -> do
        connectAndSendOrder step sock defaultOrder ep
        (Just (ResponseOrderSubmitted orderId)) <- decode . BL.fromStrict <$> receive sock

        step "Sending order cancellation request"
        send sock [] (BL.toStrict . encode $ RequestCancelOrder 2 orderId)
        threadDelay 10000

        step "Checking that order is cancelled in BrokerInterface"
        s <- readIORef broState
        (length . cancelledOrders) s @?= 1

        step "Reading response"
        resp <- decode . BL.fromStrict <$> receive sock
        case resp of
          Just (ResponseOrderCancelled _) -> return ()
          Just _ -> assertFailure "Invalid response"
          Nothing -> assertFailure "Invalid response"
        )))

testBrokerServerCancelUnknownOrder = testCaseSteps "Broker Server: order cancellation: error if order is unknown" $
  \step -> withContext (\ctx -> do
    step "Setup"
    ep <- makeEndpoint
    (mockBroker, broState) <- mkMockBroker ["demo"]
    bracket (startBrokerServer [mockBroker] ctx ep) stopBrokerServer (\broS ->
      withSocket ctx Req (\sock -> do
        connectAndSendOrder step sock defaultOrder ep
        receive sock

        step "Sending order cancellation request"
        send sock [] (BL.toStrict . encode $ RequestCancelOrder 2 100)
        threadDelay 10000

        step "Reading response"
        resp <- decode . BL.fromStrict <$> receive sock
        case resp of
          Just (ResponseError _) -> return ()
          Just _ -> assertFailure "Invalid response"
          Nothing -> assertFailure "Invalid response"
        )))


testBrokerServerCorruptedPacket = testCaseSteps "Broker Server: corrupted packet" $
  \step -> withContext (\ctx -> do
    step "Setup"
    ep <- makeEndpoint
    (mockBroker, broState) <- mkMockBroker ["demo"]
    bracket (startBrokerServer [mockBroker] ctx ep) stopBrokerServer (\broS ->
      withSocket ctx Req (\sock -> do
        step "Connecting"
        connect sock (T.unpack ep)

        step "Sending request"
        send sock [] (corrupt . BL.toStrict . encode $ RequestSubmitOrder 1 defaultOrder)
        threadDelay 10000

        step "Reading response"
        resp <- decode . BL.fromStrict <$> receive sock
        case resp of
          Just (ResponseError _) -> return ()
          Just _ -> assertFailure "Invalid response"
          Nothing -> assertFailure "Invalid response"
        )))
  where
    corrupt = B.drop 5
