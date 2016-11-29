{-# LANGUAGE OverloadedStrings, MultiWayIf, RecordWildCards #-}

module ATrade.Broker.Protocol (
  BrokerServerRequest(..),
  BrokerServerResponse(..),
  Notification(..),
  notificationOrderId,
  RequestSqnum(..),
  requestSqnum,
  TradeSinkMessage(..)
) where

import Control.Error.Util
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import Data.Text.Format
import Data.Aeson
import Data.Aeson.Types hiding (parse)
import Data.Int
import Data.Time.Clock
import Data.Time.Calendar
import ATrade.Types
import Text.Parsec

type RequestSqnum = Int64

data BrokerServerRequest = RequestSubmitOrder RequestSqnum Order
  | RequestCancelOrder RequestSqnum OrderId
  | RequestNotifications RequestSqnum
  deriving (Eq, Show)

requestSqnum :: BrokerServerRequest -> RequestSqnum
requestSqnum (RequestSubmitOrder sqnum _) = sqnum
requestSqnum (RequestCancelOrder sqnum _) = sqnum
requestSqnum (RequestNotifications sqnum) = sqnum

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
      parseRequest _ _ = fail "Invalid request object"

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

notificationOrderId :: Notification -> OrderId
notificationOrderId (OrderNotification oid _) = oid
notificationOrderId (TradeNotification trade) = tradeOrderId trade

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
      parseOrder _ = fail "Unable to parse order state"

instance ToJSON Notification where
  toJSON (OrderNotification oid newState) = object ["order-state" .= object [ "order-id" .= oid, "new-state" .= newState] ]
  toJSON (TradeNotification trade) = object ["trade" .= toJSON trade]

data TradeSinkMessage = TradeSinkHeartBeat | TradeSinkTrade {
  tsAccountId :: T.Text,
  tsSecurity :: T.Text,
  tsPrice :: Double,
  tsQuantity :: Int,
  tsVolume :: Double,
  tsCurrency :: T.Text,
  tsOperation :: Operation,
  tsExecutionTime :: UTCTime,
  tsSignalId :: SignalId
}

getHMS :: UTCTime -> (Int, Int, Int, Int)
getHMS (UTCTime _ diff) = (intsec `div` 3600, (intsec `mod` 3600) `div` 60, intsec `mod` 60, msec)
  where
    intsec = floor diff
    msec = floor $ (diff - fromIntegral intsec) * 1000

formatTimestamp dt = format "{}-{}-{} {}:{}:{}.{}" (left 4 '0' y, left 2 '0' m, left 2 '0' d, left 2 '0' hour, left 2 '0' min, left 2 '0' sec, left 3 '0' msec)
  where
    (y, m, d) = toGregorian $ utctDay dt
    (hour, min, sec, msec) = getHMS dt 

parseTimestamp (String t) = case hush $ parse p "" t of
  Just ts -> return ts
  Nothing -> fail "Unable to parse timestamp"
  where
    p = do
      year <- read <$> many1 digit
      char '-'
      mon <- read <$> many1 digit
      char '-'
      day <- read <$> many1 digit
      char ' '
      hour <- read <$> many1 digit
      char ':'
      min <- read <$> many1 digit
      char ':'
      sec <- read <$> many1 digit
      char '.'
      msec <- many1 digit -- TODO use msec
      return $ UTCTime (fromGregorian year mon day) (secondsToDiffTime $ hour * 3600 + min * 60 + sec)

parseTimestamp _ = fail "Unable to parse timestamp: invalid type"


instance ToJSON TradeSinkMessage where
  toJSON TradeSinkHeartBeat = object ["command" .= T.pack "heartbeat" ]
  toJSON TradeSinkTrade { .. } = object ["account" .= tsAccountId,
    "security" .= tsSecurity,
    "price" .= tsPrice,
    "quantity" .= tsQuantity,
    "volume" .= tsVolume,
    "volume-currency" .= tsCurrency,
    "operation" .= tsOperation,
    "execution-time" .= formatTimestamp tsExecutionTime,
    "strategy" .= strategyId tsSignalId,
    "signal-id" .= signalName tsSignalId,
    "comment" .= comment tsSignalId]

instance FromJSON TradeSinkMessage where
  parseJSON = withObject "object" (\obj ->
    case HM.lookup "command" obj of
      Nothing -> parseTrade obj
      Just cmd -> return TradeSinkHeartBeat)
    where
      parseTrade v = do
        acc <- v .: "account"
        sec <- v .: "security"
        pr <- v .: "price"
        q <- v .: "quantity"
        vol <- v .: "volume"
        cur <- v .: "volume-currency"
        op <- v .: "operation"
        extime <- v .: "execution-time" >>= parseTimestamp
        strategy <- v .: "strategy"
        sid <- v .: "signal-id"
        com <- v .: "comment"
        return TradeSinkTrade {
          tsAccountId = acc,
          tsSecurity = sec,
          tsPrice = pr,
          tsQuantity = q,
          tsVolume = vol,
          tsCurrency = cur,
          tsOperation = op,
          tsExecutionTime = extime,
          tsSignalId = SignalId strategy sid com }
