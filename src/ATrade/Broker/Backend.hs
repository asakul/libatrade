
module ATrade.Broker.Backend
(
  BrokerBackend(..),
  BrokerBackendNotification(..),
  backendNotificationOrderId
) where

import           ATrade.Types
import qualified Data.Text    as T

data BrokerBackendNotification =
  BackendTradeNotification Trade |
  BackendOrderNotification OrderId OrderState
  deriving (Show, Eq)

backendNotificationOrderId :: BrokerBackendNotification -> OrderId
backendNotificationOrderId (BackendOrderNotification oid _) = oid
backendNotificationOrderId (BackendTradeNotification trade) = tradeOrderId trade

data BrokerBackend = BrokerBackend
  {
    accounts                :: [T.Text],
    setNotificationCallback :: (Maybe (BrokerBackendNotification -> IO ())) -> IO (),
    submitOrder             :: Order -> IO (),
    cancelOrder             :: OrderId -> IO (),
    stop                    :: IO ()
  }
