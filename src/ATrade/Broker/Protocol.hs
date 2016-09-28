{-# LANGUAGE OverloadedStrings, MultiWayIf #-}

module ATrade.Broker.Protocol (
  BrokerServerRequest(..),
  BrokerServerResponse(..),
  Notification(..),
  RequestSqnum(..)
) where

import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import Data.Aeson
import Data.Aeson.Types
import Data.Int
import ATrade.Types

type RequestSqnum = Int64

data BrokerServerRequest = RequestSubmitOrder RequestSqnum Order
  | RequestCancelOrder RequestSqnum OrderId
  | RequestNotifications RequestSqnum
  deriving (Eq, Show)

instance FromJSON BrokerServerRequest where
  parseJSON = withObject "object" (\obj -> do
    sqnum <- obj .: "request-sqnum"
    parseRequest sqnum obj)
    where
      parseRequest :: RequestSqnum -> Object -> Parser BrokerServerRequest
      parseRequest sqnum obj
        | HM.member "order" obj = do
          order <- obj .: "order"
          RequestSubmitOrder sqnum <$> parseJSON order
        | HM.member "cancel-order" obj = do
          orderId <- obj .: "cancel-order"
          RequestCancelOrder sqnum <$> parseJSON orderId
        | HM.member "request-notifications" obj = return (RequestNotifications sqnum)

instance ToJSON BrokerServerRequest where
  toJSON (RequestSubmitOrder sqnum order) = object ["request-sqnum" .= sqnum,
    "order" .= order ]
  toJSON (RequestCancelOrder sqnum oid) = object ["request-sqnum" .= sqnum,
    "cancel-order" .= oid ]
  toJSON (RequestNotifications sqnum) = object ["request-sqnum" .= sqnum,
    "request-notifications" .= ("" :: T.Text) ]

data BrokerServerResponse = ResponseOrderSubmitted OrderId
  | ResponseOrderCancelled OrderId
  | ResponseNotifications [Notification]
  | ResponseError T.Text
  deriving (Eq, Show)

instance FromJSON BrokerServerResponse where
  parseJSON = withObject "object" (\obj ->
    if | HM.member "order-id" obj -> do
        oid <- obj .: "order-id"
        return $ ResponseOrderSubmitted oid
       | HM.member "order-cancelled" obj -> do
        oid <- obj .: "order-cancelled"
        return $ ResponseOrderCancelled oid
       | HM.member "notifications" obj -> do
        notifications <- obj .: "notifications"
        ResponseNotifications <$> parseJSON notifications
       | HM.member "error" obj -> do
        error <- obj .: "error"
        ResponseError <$> parseJSON error)

instance ToJSON BrokerServerResponse where
  toJSON (ResponseOrderSubmitted oid) = object [ "order-id" .= oid ]
  toJSON (ResponseOrderCancelled oid) = object [ "order-cancelled" .= oid ]
  toJSON (ResponseNotifications notifications) = object [ "notifications" .= notifications ]
  toJSON (ResponseError errorMessage) = object [ "error" .= errorMessage ]

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
