
module ATrade.Broker.Protocol (
) where

import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import Data.Aeson
import Data.Int
import ATrade.Types

type RequestSqnum = Int64

data BrokerServerRequest = RequestSubmitOrder Order
  | RequestCancelOrder OrderId
  | RequestNotifications

data BrokerServerResponse = ResponseOrderSubmitted OrderId
  | ResponseOrderCancelled
  | ResponseNotifications [Notification]

data Notification = OrderNotification OrderId OrderState | TradeNotification Trade
  deriving (Eq, Show)

instance FromJSON Notification where
  parseJSON n = withObject "notification" (\obj ->
    case HM.lookup "trade" obj of
      Just v -> parseTrade v
      Nothing -> parseOrder n) n
    where
      parseTrade v = TradeNotification <$> parseJSON v
      parseOrder (Object o) = case HM.lookup "order-state" o of
        Just v -> withObject "object" (\os -> do
          oid <- os .: "order-id"
          ns <- os .: "new-state"
          return $ OrderNotification oid ns) v
        Nothing -> fail "Should be order-state"

instance ToJSON Notification where
  toJSON (OrderNotification oid newState) = object ["order-state" .= object [ "order-id" .= oid, "new-state" .= newState] ]
  toJSON (TradeNotification trade) = object ["trade" .= toJSON trade]
