
module MockBroker (
  MockBrokerState(..),
  mockSubmitOrder,
  mockCancelOrder,
  mockStopBroker,
  mkMockBroker
) where

import ATrade.Types
import ATrade.Broker.Protocol
import ATrade.Broker.Server
import ATrade.Util
import Data.IORef
import qualified Data.List as L

data MockBrokerState = MockBrokerState {
  orders :: [Order],
  cancelledOrders :: [Order],
  notificationCallback :: Maybe (Notification -> IO ())
}

mockSubmitOrder :: IORef MockBrokerState -> Order -> IO ()
mockSubmitOrder state order = do
  atomicMapIORef state (\s -> s { orders = submittedOrder : orders s })
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
    setNotificationCallback = \cb -> atomicMapIORef state (\s -> s { notificationCallback = cb }),
    submitOrder = mockSubmitOrder state,
    cancelOrder = mockCancelOrder state,
    stopBroker = mockStopBroker state
  }, state)

