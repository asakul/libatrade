
module MockBroker (
  MockBrokerState(..),
  mockSubmitOrder,
  mockCancelOrder,
  mockStopBroker,
  mkMockBroker
) where

import           ATrade.Broker.Backend
import           ATrade.Broker.Protocol
import           ATrade.Broker.Server
import           ATrade.Types
import           ATrade.Util
import           Data.IORef
import qualified Data.List              as L

data MockBrokerState = MockBrokerState {
  orders               :: [Order],
  cancelledOrders      :: [Order],
  notificationCallback :: Maybe (BrokerBackendNotification -> IO ())
}

mockSubmitOrder :: IORef MockBrokerState -> Order -> IO ()
mockSubmitOrder state order = do
  atomicModifyIORef' state (\s -> (s { orders = submittedOrder : orders s }, ()))
  maybeCb <- notificationCallback <$> readIORef state
  case maybeCb of
    Just cb -> cb $ BackendOrderNotification (orderId order) Submitted
    Nothing -> return ()
  where
    submittedOrder = order { orderState = Submitted }

mockCancelOrder :: IORef MockBrokerState -> OrderId -> IO ()
mockCancelOrder state oid = do
  ors <- orders <$> readIORef state
  case L.find (\o -> orderId o == oid) ors of
    Just order -> atomicModifyIORef' state (\s -> (s { cancelledOrders = order : cancelledOrders s}, ()))
    Nothing -> return ()

mockStopBroker :: IORef MockBrokerState -> IO ()
mockStopBroker state = return ()


mkMockBroker accs = do
  state <- newIORef MockBrokerState {
    orders = [],
    cancelledOrders = [],
    notificationCallback = Nothing
  }

  return (BrokerBackend {
    accounts = accs,
    setNotificationCallback = \cb -> atomicMapIORef state (\s -> s { notificationCallback = cb }),
    submitOrder = mockSubmitOrder state,
    cancelOrder = mockCancelOrder state,
    stop = mockStopBroker state
  }, state)

