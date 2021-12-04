{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module TestBrokerServer (
  unitTests
) where

import           Test.Tasty
import           Test.Tasty.HUnit

import           ATrade.Broker.Backend
import           ATrade.Broker.Protocol
import           ATrade.Broker.Server
import           ATrade.Types
import ATrade.Logging (emptyLogger)
import           Control.Concurrent     hiding (writeChan)
import           Control.Exception
import           Data.Aeson
import qualified Data.ByteString        as B
import qualified Data.ByteString.Lazy   as BL
import           Data.IORef
import           Data.List              (sort)
import qualified Data.Text              as T
import           Data.Text.Encoding     (encodeUtf8)
import           Data.Time.Calendar
import           Data.Time.Clock
import           Data.UUID              as U
import           Data.UUID.V4           as UV4
import           Debug.Trace            (traceM)
import           MockBroker
import           System.Log.Logger
import           System.ZMQ4

unitTests :: TestTree
unitTests = testGroup "Broker.Server" [testBrokerServerStartStop
  , testBrokerServerSubmitOrder
  , testBrokerServerSubmitOrderDifferentIdentities
  , testBrokerServerSubmitOrderToUnknownAccount
  , testBrokerServerCancelOrder
  , testBrokerServerCancelUnknownOrder
  , testBrokerServerCorruptedPacket
  , testBrokerServerGetNotifications
  , testBrokerServerGetNotificationsFromSameSqnum
  , testBrokerServerGetNotificationsRemovesEarlierNotifications
  , testBrokerServerDuplicateRequest
  , testBrokerServerNotificationSocket ]

--
-- Few helpers
--

makeEndpoints :: IO (T.Text, T.Text)
makeEndpoints = do
  uid <- toText <$> UV4.nextRandom
  return ("inproc://brokerserver-" `T.append` uid, "inproc://brokerserver-notifications-" `T.append` uid)

connectAndSendOrder :: (Sender a) => (String -> IO ()) -> Socket a -> Order -> T.Text -> IO ()
connectAndSendOrder step sock order ep = do
  step "Connecting"
  connect sock (T.unpack ep)

  step "Sending request"
  send sock [] (BL.toStrict . encode $ RequestSubmitOrder 1 "identity" order)
  threadDelay 10000

connectAndSendOrderWithIdentity :: (Sender a) => (String -> IO ()) -> Socket a -> Order -> ClientIdentity -> T.Text -> IO ()
connectAndSendOrderWithIdentity step sock order clientIdentity ep = do
  step "Connecting"
  connect sock (T.unpack ep)

  step $ "Sending request for identity: " ++ show clientIdentity
  send sock [] (BL.toStrict . encode $ RequestSubmitOrder 1 clientIdentity order)
  threadDelay 10000

defaultOrder :: Order
defaultOrder = mkOrder {
    orderId = 25,
    orderAccountId = "demo",
    orderSecurity = "FOO",
    orderPrice = Market,
    orderQuantity = 10,
    orderOperation = Buy
  }

makeTestTradeSink :: IO (IORef (Maybe Trade), TradeSink)
makeTestTradeSink = do
  ref <- newIORef Nothing
  return (ref, f ref)
  where
    f ref t = writeIORef ref $ Just t


--
-- Tests
--

testBrokerServerStartStop :: TestTree
testBrokerServerStartStop = testCase "Broker Server starts and stops" $ withContext (\ctx -> do
  (ep, notifEp) <- makeEndpoints
  broS <- startBrokerServer [] ctx ep notifEp [] defaultServerSecurityParams emptyLogger
  stopBrokerServer broS)

testBrokerServerSubmitOrder :: TestTree
testBrokerServerSubmitOrder = testCaseSteps "Broker Server submits order" $ \step -> withContext $ \ctx -> do
  step "Setup"
  (mockBroker, broState) <- mkMockBroker ["demo"]
  (ep, notifEp) <- makeEndpoints
  bracket (startBrokerServer [mockBroker] ctx ep notifEp [] defaultServerSecurityParams emptyLogger) stopBrokerServer $ \_ -> do
    withSocket ctx Req $ \sock -> do
      connectAndSendOrder step sock defaultOrder ep

      step "Checking that order is submitted to BrokerInterface"
      s <- readIORef broState
      (length . orders) s @?= 1

      step "Reading response"
      resp <- decode . BL.fromStrict <$> receive sock
      case resp of
        Just ResponseOk -> return ()
        Just _          -> assertFailure "Invalid response"
        Nothing         -> assertFailure "Invalid response"

testBrokerServerSubmitOrderDifferentIdentities :: TestTree
testBrokerServerSubmitOrderDifferentIdentities = testCaseSteps "Broker Server submits order: different identities" $ \step -> withContext $ \ctx -> do
  step "Setup"
  (mockBroker, broState) <- mkMockBroker ["demo"]
  (ep, notifEp) <- makeEndpoints
  let orderId1 = 42
  let orderId2 = 76
  bracket (startBrokerServer [mockBroker] ctx ep notifEp [] defaultServerSecurityParams emptyLogger) stopBrokerServer $ \_ -> do
    withSocket ctx Req $ \sock1 -> do
      withSocket ctx Req $ \sock2 -> do
        connectAndSendOrderWithIdentity step sock1 defaultOrder {orderId = orderId1} "identity1" ep
        connectAndSendOrderWithIdentity step sock2 defaultOrder {orderId = orderId2} "identity2" ep

        step "Checking that orders are submitted to BrokerInterface"
        s <- readIORef broState
        (length . orders) s @?= 2

        step "Reading response for identity1"
        resp <- decode . BL.fromStrict <$> receive sock1
        case resp of
          Just ResponseOk -> return ()
          Just _          -> assertFailure "Invalid response"
          Nothing         -> assertFailure "Invalid response"

        step "Reading response for identity2"
        resp <- decode . BL.fromStrict <$> receive sock2
        case resp of
          Just ResponseOk -> return ()
          Just _          -> assertFailure "Invalid response"
          Nothing         -> assertFailure "Invalid response"

testBrokerServerSubmitOrderToUnknownAccount :: TestTree
testBrokerServerSubmitOrderToUnknownAccount = testCaseSteps "Broker Server returns error if account is unknown" $
  \step -> withContext (\ctx -> do
    step "Setup"
    (ep, notifEp) <- makeEndpoints
    (mockBroker, _) <- mkMockBroker ["demo"]
    bracket (startBrokerServer [mockBroker] ctx ep notifEp [] defaultServerSecurityParams emptyLogger) stopBrokerServer (\_ ->
      withSocket ctx Req (\sock -> do
        connectAndSendOrder step sock (defaultOrder { orderAccountId = "foobar" }) ep

        step "Reading response"
        resp <- decode . BL.fromStrict <$> receive sock
        case resp of
          Just (ResponseError _) -> return ()
          Just _                 -> assertFailure "Invalid response"
          Nothing                -> assertFailure "Invalid response"

        )))

testBrokerServerCancelOrder :: TestTree
testBrokerServerCancelOrder = testCaseSteps "Broker Server: submitted order cancellation" $
  \step -> withContext $ \ctx -> do
    step "Setup"
    (ep, notifEp) <- makeEndpoints
    (mockBroker, broState) <- mkMockBroker ["demo"]
    bracket (startBrokerServer [mockBroker] ctx ep notifEp [] defaultServerSecurityParams emptyLogger) stopBrokerServer $ \_ ->
      withSocket ctx Req $ \sock -> do
        connectAndSendOrder step sock defaultOrder ep
        Just ResponseOk <- decode . BL.fromStrict <$> receive sock

        step "Sending order cancellation request"
        send sock [] (BL.toStrict . encode $ RequestCancelOrder 2 "identity" (orderId defaultOrder))
        threadDelay 10000

        step "Checking that order is cancelled in BrokerBackend"
        s <- readIORef broState
        (length . cancelledOrders) s @?= 1

        step "Reading response"
        resp <- decode . BL.fromStrict <$> receive sock
        case resp of
          Just ResponseOk -> return ()
          Just _          -> assertFailure "Invalid response"
          Nothing         -> assertFailure "Invalid response"

testBrokerServerCancelUnknownOrder :: TestTree
testBrokerServerCancelUnknownOrder = testCaseSteps "Broker Server: order cancellation: error if order is unknown" $
  \step -> withContext (\ctx -> do
    step "Setup"
    (ep, notifEp) <- makeEndpoints
    (mockBroker, _) <- mkMockBroker ["demo"]
    bracket (startBrokerServer [mockBroker] ctx ep notifEp [] defaultServerSecurityParams emptyLogger) stopBrokerServer (\_ ->
      withSocket ctx Req (\sock -> do
        connectAndSendOrder step sock defaultOrder ep
        receive sock

        step "Sending order cancellation request"
        send sock [] (BL.toStrict . encode $ RequestCancelOrder 2 "identity" 100)
        threadDelay 10000

        step "Reading response"
        resp <- decode . BL.fromStrict <$> receive sock
        case resp of
          Just (ResponseError _) -> return ()
          Just _                 -> assertFailure "Invalid response"
          Nothing                -> assertFailure "Invalid response"
        )))

testBrokerServerCorruptedPacket :: TestTree
testBrokerServerCorruptedPacket = testCaseSteps "Broker Server: corrupted packet" $
  \step -> withContext (\ctx -> do
    step "Setup"
    (ep, notifEp) <- makeEndpoints
    (mockBroker, _) <- mkMockBroker ["demo"]
    bracket (startBrokerServer [mockBroker] ctx ep notifEp [] defaultServerSecurityParams emptyLogger) stopBrokerServer (\_ ->
      withSocket ctx Req (\sock -> do
        step "Connecting"
        connect sock (T.unpack ep)

        step "Sending request"
        send sock [] (corrupt . BL.toStrict . encode $ RequestSubmitOrder 1 "identity" defaultOrder)
        threadDelay 10000

        step "Reading response"
        resp <- decode . BL.fromStrict <$> receive sock
        case resp of
          Just (ResponseError _) -> return ()
          Just _                 -> assertFailure "Invalid response"
          Nothing                -> assertFailure "Invalid response"
        )))
  where
    corrupt = B.drop 5

testBrokerServerGetNotifications :: TestTree
testBrokerServerGetNotifications = testCaseSteps "Broker Server: notifications request" $
  \step -> withContext $ \ctx -> do
    step "Setup"
    (ep, notifEp) <- makeEndpoints
    (mockBroker, broState) <- mkMockBroker ["demo"]
    bracket (startBrokerServer [mockBroker] ctx ep notifEp [] defaultServerSecurityParams emptyLogger) stopBrokerServer $ \_ ->
      withSocket ctx Req $ \sock -> do
        -- We have to actually submit order, or else server won't know that we should
        -- be notified about this order
        connectAndSendOrder step sock defaultOrder ep
        Just ResponseOk <- decode . BL.fromStrict <$> receive sock
        threadDelay 10000

        globalOrderId <- orderId . head . orders <$> readIORef broState

        (Just cb) <- notificationCallback <$> readIORef broState
        cb (BackendOrderNotification globalOrderId Executed)
        let trade = Trade {
          tradeOrderId = globalOrderId,
          tradePrice = 19.82,
          tradeQuantity = 1,
          tradeVolume = 1982,
          tradeVolumeCurrency = "TEST_CURRENCY",
          tradeOperation = Buy,
          tradeAccount = "demo",
          tradeSecurity = "FOO",
          tradeTimestamp = UTCTime (fromGregorian 2016 9 28) 16000,
          tradeCommission = 0,
          tradeSignalId = SignalId "Foo" "bar" "baz" }
        cb (BackendTradeNotification trade)

        step "Sending notifications request"
        send sock [] (BL.toStrict . encode $ RequestNotifications 2 "identity" (NotificationSqnum 1))
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
            let (OrderNotification orderNotificationSqnum oid newstate) = ns !! 1
            let (TradeNotification tradeNotificationSqnum newtrade) = ns !! 2
            Executed @=? newstate
            trade { tradeOrderId = orderId defaultOrder } @=? newtrade
            -- Check notification sqnums
            step "Received notification sqnums are correct"
            let sqnums = sort $ fmap (unNotificationSqnum . getNotificationSqnum) ns
            sqnums @=? [1, 2, 3]


          Just _ -> assertFailure "Invalid response"
          Nothing -> assertFailure "Invalid response"

testBrokerServerGetNotificationsFromSameSqnum :: TestTree
testBrokerServerGetNotificationsFromSameSqnum = testCaseSteps "Broker Server: notifications request, twice from same sqnum" $
  \step -> withContext $ \ctx -> do
    step "Setup"
    (ep, notifEp) <- makeEndpoints
    (mockBroker, broState) <- mkMockBroker ["demo"]
    bracket (startBrokerServer [mockBroker] ctx ep notifEp [] defaultServerSecurityParams emptyLogger) stopBrokerServer $ \_ ->
      withSocket ctx Req $ \sock -> do
        connectAndSendOrder step sock defaultOrder ep
        Just ResponseOk <- decode . BL.fromStrict <$> receive sock
        threadDelay 10000

        globalOrderId <- orderId . head . orders <$> readIORef broState

        (Just cb) <- notificationCallback <$> readIORef broState
        cb (BackendOrderNotification globalOrderId Executed)
        let trade = Trade {
          tradeOrderId = globalOrderId,
          tradePrice = 19.82,
          tradeQuantity = 1,
          tradeVolume = 1982,
          tradeVolumeCurrency = "TEST_CURRENCY",
          tradeOperation = Buy,
          tradeAccount = "demo",
          tradeSecurity = "FOO",
          tradeTimestamp = UTCTime (fromGregorian 2016 9 28) 16000,
          tradeCommission = 0,
          tradeSignalId = SignalId "Foo" "bar" "baz" }
        cb (BackendTradeNotification trade)

        step "Sending notifications request"
        send sock [] (BL.toStrict . encode $ RequestNotifications 2 "identity" (NotificationSqnum 1))
        threadDelay 10000

        step "Reading response"
        resp <- decode . BL.fromStrict <$> receive sock
        case resp of
          Just (ResponseNotifications ns) -> do
            step "Received notification sqnums are correct"
            let sqnums = sort $ fmap (unNotificationSqnum . getNotificationSqnum) ns
            sqnums @=? [1, 2, 3]


          _ -> assertFailure "Invalid response"

        step "Sending second notifications request"
        send sock [] (BL.toStrict . encode $ RequestNotifications 3 "identity" (NotificationSqnum 1))
        threadDelay 10000

        step "Reading response"
        resp' <- decode . BL.fromStrict <$> receive sock
        case resp' of
          Just (ResponseNotifications ns) -> do
            step "Received notification sqnums are correct"
            let sqnums = sort $ fmap (unNotificationSqnum . getNotificationSqnum) ns
            [1, 2, 3] @=? sqnums
          _ -> assertFailure "Invalid response"

testBrokerServerGetNotificationsRemovesEarlierNotifications :: TestTree
testBrokerServerGetNotificationsRemovesEarlierNotifications = testCaseSteps "Broker Server: notifications request removes earlier notifications" $
  \step -> withContext $ \ctx -> do
    step "Setup"
    (ep, notifEp) <- makeEndpoints
    (mockBroker, broState) <- mkMockBroker ["demo"]
    bracket (startBrokerServer [mockBroker] ctx ep notifEp [] defaultServerSecurityParams emptyLogger) stopBrokerServer $ \_ ->
      withSocket ctx Req $ \sock -> do
        connectAndSendOrder step sock defaultOrder ep
        Just ResponseOk <- decode . BL.fromStrict <$> receive sock
        threadDelay 10000

        globalOrderId <- orderId . head . orders <$> readIORef broState

        (Just cb) <- notificationCallback <$> readIORef broState
        cb (BackendOrderNotification globalOrderId Executed)
        let trade = Trade {
          tradeOrderId = globalOrderId,
          tradePrice = 19.82,
          tradeQuantity = 1,
          tradeVolume = 1982,
          tradeVolumeCurrency = "TEST_CURRENCY",
          tradeOperation = Buy,
          tradeAccount = "demo",
          tradeSecurity = "FOO",
          tradeTimestamp = UTCTime (fromGregorian 2016 9 28) 16000,
          tradeCommission = 0,
          tradeSignalId = SignalId "Foo" "bar" "baz" }
        cb (BackendTradeNotification trade)

        step "Sending notifications request"
        send sock [] (BL.toStrict . encode $ RequestNotifications 2 "identity" (NotificationSqnum 4))
        threadDelay 10000

        step "Reading response"
        resp <- decode . BL.fromStrict <$> receive sock
        case resp of
          Just (ResponseNotifications ns) -> do
            step "Checking that request list is empty"
            [] @=? ns
          _ -> assertFailure "Invalid response"

        step "Sending second notifications request"
        send sock [] (BL.toStrict . encode $ RequestNotifications 3 "identity" (NotificationSqnum 1))
        threadDelay 10000

        step "Reading response"
        resp' <- decode . BL.fromStrict <$> receive sock
        case resp' of
          Just (ResponseNotifications ns) -> do
            step "Checking that request list is empty"
            [] @=? ns
          _ -> assertFailure "Invalid response"

testBrokerServerDuplicateRequest :: TestTree
testBrokerServerDuplicateRequest = testCaseSteps "Broker Server: duplicate request" $ \step -> withContext $ \ctx -> do
  step "Setup"
  (mockBroker, broState) <- mkMockBroker ["demo"]
  (ep, notifEp) <- makeEndpoints
  bracket (startBrokerServer [mockBroker] ctx ep notifEp [] defaultServerSecurityParams emptyLogger) stopBrokerServer $ \_ -> do
    withSocket ctx Req $ \sock -> do
      connectAndSendOrder step sock defaultOrder ep

      step "Reading response"
      Just ResponseOk <- decode . BL.fromStrict <$> receive sock

      step "Sending duplicate request (with same sequence number)"
      send sock [] (BL.toStrict . encode $ RequestSubmitOrder 1 "identity" defaultOrder)
      threadDelay 10000

      step "Checking that only one order is submitted"
      s <- readIORef broState
      (length . orders) s @?= 1

      step "Reading response from duplicate request"
      resp <- decode . BL.fromStrict <$> receive sock
      case resp of
        Just ResponseOk -> return ()
        Just _          -> assertFailure "Invalid response"
        Nothing         -> assertFailure "Invalid response"

testBrokerServerNotificationSocket :: TestTree
testBrokerServerNotificationSocket = testCaseSteps "Broker Server: sends notification via notification socket" $ \step -> withContext $ \ctx -> do
  (mockBroker, broState) <- mkMockBroker ["demo"]

  (ep, notifEp) <- makeEndpoints
  bracket (startBrokerServer [mockBroker] ctx ep notifEp [] defaultServerSecurityParams emptyLogger) stopBrokerServer $ \_ -> do
    withSocket ctx Req $ \sock -> do
      nSocket <- socket ctx Sub
      connect nSocket (T.unpack notifEp)
      subscribe nSocket (encodeUtf8 "test-identity")

      connectAndSendOrderWithIdentity step sock defaultOrder "test-identity" ep

      step "Reading response"
      Just ResponseOk <- decode . BL.fromStrict <$> receive sock

      step "Reading order submitted notification"

      [_, payload] <- receiveMulti nSocket
      let (Just (OrderNotification notifSqnum1 notifOid newOrderState)) = decode . BL.fromStrict $ payload
      notifOid @?= orderId defaultOrder
      newOrderState @?= Submitted

      backendOrderId <- mockBrokerLastOrderId broState
      let trade = Trade
                    {
                      tradeOrderId = backendOrderId,
                      tradePrice = 10,
                      tradeQuantity = 1,
                      tradeVolume = 1,
                      tradeVolumeCurrency = "TEST",
                      tradeOperation = Buy ,
                      tradeAccount = "TEST_ACCOUNT",
                      tradeSecurity = "TEST_SECURITY",
                      tradeTimestamp = UTCTime (fromGregorian 2021 09 24) 10000,
                      tradeCommission = 3.5,
                      tradeSignalId = SignalId "test" "test" ""
                    }
      step "Emitting trade notification"
      emitNotification broState (BackendTradeNotification trade)

      step "Receiving trade notification"
      [_, payload] <- receiveMulti nSocket
      let (Just (TradeNotification notifSqnum2 incomingTrade)) = decode . BL.fromStrict $ payload
      incomingTrade @?= trade { tradeOrderId = orderId defaultOrder }

{-
testBrokerServerTradeSink :: TestTree
testBrokerServerTradeSink = testCaseSteps "Broker Server: sends trades to trade sink" $ \step -> withContext (\ctx -> do
  step "Setup"
  (mockBroker, broState) <- mkMockBroker ["demo"]
  ep <- makeEndpoint
  (tradeRef, sink) <- makeTestTradeSink
  bracket (startBrokerServer [mockBroker] ctx ep [sink] defaultServerSecurityParams) stopBrokerServer (\_ -> do
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
        tradeCommission = 0,
        tradeSignalId = SignalId "Foo" "bar" "baz" }
      cb (BackendTradeNotification trade)

      threadDelay 100000
      step "Testing"
      maybeTrade <- readIORef tradeRef
      case maybeTrade of
        Just trade' -> do
          trade' @?= trade
        _ -> assertFailure "Invalid trade in sink"
      )))
-}
