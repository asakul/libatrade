{-# LANGUAGE OverloadedStrings, RecordWildCards #-}

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

unitTests = testGroup "Broker.Server" [testBrokerServerStartStop
  , testBrokerServerSubmitOrder
  , testBrokerServerSubmitOrderToUnknownAccount
  , testBrokerServerCancelOrder
  , testBrokerServerCancelUnknownOrder
  , testBrokerServerCorruptedPacket
  , testBrokerServerGetNotifications
  , testBrokerServerDuplicateRequest
  , testBrokerServerTradeSink ]

--
-- Few helpers
--

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

--
-- Tests
--

testBrokerServerStartStop = testCase "Broker Server starts and stops" $ withContext (\ctx -> do
  ep <- toText <$> UV4.nextRandom
  broS <- startBrokerServer [] ctx ("inproc://brokerserver" `T.append` ep) ""
  stopBrokerServer broS)

testBrokerServerSubmitOrder = testCaseSteps "Broker Server submits order" $ \step -> withContext (\ctx -> do
  step "Setup"
  (mockBroker, broState) <- mkMockBroker ["demo"]
  ep <- makeEndpoint
  bracket (startBrokerServer [mockBroker] ctx ep "") stopBrokerServer (\broS ->
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
    bracket (startBrokerServer [mockBroker] ctx ep "") stopBrokerServer (\broS ->
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
    bracket (startBrokerServer [mockBroker] ctx ep "") stopBrokerServer (\broS ->
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
    bracket (startBrokerServer [mockBroker] ctx ep "") stopBrokerServer (\broS ->
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
    bracket (startBrokerServer [mockBroker] ctx ep "") stopBrokerServer (\broS ->
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

testBrokerServerGetNotifications = testCaseSteps "Broker Server: notifications request" $
  \step -> withContext (\ctx -> do
    step "Setup"
    ep <- makeEndpoint
    (mockBroker, broState) <- mkMockBroker ["demo"]
    bracket (startBrokerServer [mockBroker] ctx ep "") stopBrokerServer (\broS ->
      withSocket ctx Req (\sock -> do
        -- We have to actually submit order, or else server won't know that we should
        -- be notified about this order
        connectAndSendOrder step sock defaultOrder ep
        (Just (ResponseOrderSubmitted orderId)) <- decode . BL.fromStrict <$> receive sock
        threadDelay 10000

        (Just cb) <- notificationCallback <$> readIORef broState
        cb (OrderNotification orderId Executed)
        let trade = Trade {
          tradeOrderId = orderId,
          tradePrice = 19.82,
          tradeQuantity = 1,
          tradeVolume = 1982,
          tradeVolumeCurrency = "TEST_CURRENCY",
          tradeOperation = Buy,
          tradeAccount = "demo",
          tradeSecurity = "FOO",
          tradeTimestamp = UTCTime (fromGregorian 2016 9 28) 16000,
          tradeSignalId = SignalId "Foo" "bar" "baz" }
        cb (TradeNotification trade)

        step "Sending notifications request"
        send sock [] (BL.toStrict . encode $ RequestNotifications 2)
        threadDelay 10000

        -- We should obtain 3 notifications:
        -- 1. When order became Submitted (from Unsubmitted)
        -- 2. When order became Executed (our first notificationCallback call)
        -- 3. Corresponding Trade notificatiot (our second notificationCallback call)
        step "Reading response"
        resp <- decode . BL.fromStrict <$> receive sock
        case resp of
          Just (ResponseNotifications ns) -> do
            length ns @=? 3
            let (OrderNotification oid newstate) = ns !! 1
            let (TradeNotification newtrade) = ns !! 2
            orderId @=? oid
            Executed @=? newstate
            trade @=? newtrade
          Just _ -> assertFailure "Invalid response"
          Nothing -> assertFailure "Invalid response"

        step "Sending second notifications request"
        send sock [] (BL.toStrict . encode $ RequestNotifications 3)
        threadDelay 10000

        step "Reading response"
        resp <- decode . BL.fromStrict <$> receive sock
        case resp of
          Just (ResponseNotifications ns) -> do
            0 @=? length ns
          Just _ -> assertFailure "Invalid response"
          Nothing -> assertFailure "Invalid response"
        )))

testBrokerServerDuplicateRequest = testCaseSteps "Broker Server: duplicate request" $ \step -> withContext (\ctx -> do
  step "Setup"
  (mockBroker, broState) <- mkMockBroker ["demo"]
  ep <- makeEndpoint
  bracket (startBrokerServer [mockBroker] ctx ep "") stopBrokerServer (\broS ->
    withSocket ctx Req (\sock -> do
      connectAndSendOrder step sock defaultOrder ep

      step "Reading response"
      (Just (ResponseOrderSubmitted orderId)) <- decode . BL.fromStrict <$> receive sock

      step "Sending duplicate request (with same sequence number)"
      send sock [] (BL.toStrict . encode $ RequestSubmitOrder 1 defaultOrder)
      threadDelay 10000

      step "Checking that only one order is submitted"
      s <- readIORef broState
      (length . orders) s @?= 1

      step "Reading response from duplicate request"
      resp <- decode . BL.fromStrict <$> receive sock
      case resp of
        Just (ResponseOrderSubmitted oid) -> orderId @?= oid
        Just _ -> assertFailure "Invalid response"
        Nothing -> assertFailure "Invalid response"

      )))

testBrokerServerTradeSink = testCaseSteps "Broker Server: sends trades to trade sink" $ \step -> withContext (\ctx -> do
  step "Setup"
  (mockBroker, broState) <- mkMockBroker ["demo"]
  ep <- makeEndpoint
  withSocket ctx Rep (\tradeSock -> do
    bind tradeSock "inproc://trade-sink"
    setReceiveTimeout (restrict 1000) tradeSock
    bracket (startBrokerServer [mockBroker] ctx ep "inproc://trade-sink") stopBrokerServer (\broS -> do
      withSocket ctx Req (\sock -> do
        step "Connecting"
        connectAndSendOrder step sock defaultOrder ep
        (Just (ResponseOrderSubmitted orderId)) <- decode . BL.fromStrict <$> receive sock

        (Just cb) <- notificationCallback <$> readIORef broState
        let trade = Trade {
          tradeOrderId = orderId,
          tradePrice = 19.82,
          tradeQuantity = 1,
          tradeVolume = 1982,
          tradeVolumeCurrency = "TEST_CURRENCY",
          tradeOperation = Buy,
          tradeAccount = "demo",
          tradeSecurity = "FOO",
          tradeTimestamp = UTCTime (fromGregorian 2016 9 28) 16000,
          tradeSignalId = SignalId "Foo" "bar" "baz" }
        cb (TradeNotification trade)

        threadDelay 100000
        step "Testing"
        tradeMsg <- receive tradeSock
        case decode . BL.fromStrict $ tradeMsg of
          Just tsTrade@TradeSinkTrade{..} -> do
            tsAccountId @?= tradeAccount trade
            tsPrice @?= (fromRational . toRational . tradePrice) trade
          _ -> assertFailure "Invalid trade in sink"
        ))))
