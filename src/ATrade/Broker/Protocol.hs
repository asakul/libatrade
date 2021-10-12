{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE RecordWildCards   #-}

module ATrade.Broker.Protocol (
  BrokerServerRequest(..),
  BrokerServerResponse(..),
  Notification(..),
  NotificationSqnum(..),
  nextSqnum,
  getNotificationSqnum,
  notificationOrderId,
  RequestSqnum(..),
  requestSqnum,
  TradeSinkMessage(..),
  mkTradeMessage,
  ClientIdentity(..)
) where

import           ATrade.Types
import           Control.Applicative     ((<|>))
import           Control.Error.Util
import           Data.Aeson
import           Data.Aeson.Types        hiding (parse)
import qualified Data.HashMap.Strict     as HM
import           Data.Int
import qualified Data.Text               as T
import           Data.Text.Encoding
import           Data.Time.Calendar
import           Data.Time.Clock
import           Language.Haskell.Printf
import           Text.Parsec             hiding ((<|>))

type ClientIdentity = T.Text
type RequestSqnum = Int64

newtype NotificationSqnum = NotificationSqnum { unNotificationSqnum :: Int64 }
  deriving (Eq, Show, Ord)

nextSqnum :: NotificationSqnum -> NotificationSqnum
nextSqnum (NotificationSqnum n) = NotificationSqnum (n + 1)

data Notification = OrderNotification NotificationSqnum OrderId OrderState | TradeNotification NotificationSqnum Trade
  deriving (Eq, Show)

getNotificationSqnum :: Notification -> NotificationSqnum
getNotificationSqnum (OrderNotification sqnum _ _) = sqnum
getNotificationSqnum (TradeNotification sqnum _)   = sqnum

notificationOrderId :: Notification -> OrderId
notificationOrderId (OrderNotification _ oid _) = oid
notificationOrderId (TradeNotification _ trade) = tradeOrderId trade

instance FromJSON Notification where
  parseJSON = withObject "notification" $ \obj -> parseNotification obj
    where
      parseNotification obj =
        case HM.lookup "notification-sqnum" obj of
          Just (Number sqnum) -> parseTrade (NotificationSqnum $ truncate sqnum) obj <|>
                                 parseOrder (NotificationSqnum $ truncate sqnum) obj <|>
                                 fail "Can't parse notification"
          Just _ -> fail "Invalid sqnum"
          Nothing -> fail "Unable to lookup notification sqnum"
      parseTrade sqnum obj = case HM.lookup "trade" obj of
        Just val -> TradeNotification sqnum <$> (parseJSON val)
        Nothing  -> fail "Can't parse trade"
      parseOrder sqnum obj = case HM.lookup "order-state" obj of
        Just v -> withObject "object" (\os -> do
          oid <- os .: "order-id"
          ns <- os .: "new-state"
          return $ OrderNotification sqnum oid ns) v
        Nothing -> fail "Should be order-state"

instance ToJSON Notification where
  toJSON (OrderNotification sqnum oid newState) = object [ "notification-sqnum" .= toJSON (unNotificationSqnum sqnum), "order-state" .= object [ "order-id" .= oid, "new-state" .= newState ] ]
  toJSON (TradeNotification sqnum trade) = object [ "notification-sqnum" .= toJSON (unNotificationSqnum sqnum), "trade" .= toJSON trade ]


data BrokerServerRequest = RequestSubmitOrder RequestSqnum ClientIdentity Order
  | RequestCancelOrder RequestSqnum ClientIdentity OrderId
  | RequestNotifications RequestSqnum ClientIdentity NotificationSqnum
  | RequestCurrentSqnum RequestSqnum ClientIdentity
  deriving (Eq, Show)

requestSqnum :: BrokerServerRequest -> RequestSqnum
requestSqnum (RequestSubmitOrder sqnum _ _)   = sqnum
requestSqnum (RequestCancelOrder sqnum _ _)   = sqnum
requestSqnum (RequestNotifications sqnum _ _) = sqnum
requestSqnum (RequestCurrentSqnum sqnum _)    = sqnum

instance FromJSON BrokerServerRequest where
  parseJSON = withObject "object" (\obj -> do
    sqnum <- obj .: "request-sqnum"
    clientIdentity <- obj .: "client-identity"
    parseRequest sqnum clientIdentity obj)
    where
      parseRequest :: RequestSqnum -> ClientIdentity -> Object -> Parser BrokerServerRequest
      parseRequest sqnum clientIdentity obj
        | HM.member "order" obj = do
          order <- obj .: "order"
          RequestSubmitOrder sqnum clientIdentity <$> parseJSON order
        | HM.member "cancel-order" obj = do
          orderId <- obj .: "cancel-order"
          RequestCancelOrder sqnum clientIdentity <$> parseJSON orderId
        | HM.member "request-notifications" obj = do
            initialSqnum <- obj .: "initial-sqnum"
            return (RequestNotifications sqnum clientIdentity (NotificationSqnum initialSqnum))
        | HM.member "request-current-sqnum" obj =
            return (RequestCurrentSqnum sqnum clientIdentity)
      parseRequest _ _ _ = fail "Invalid request object"

instance ToJSON BrokerServerRequest where
  toJSON (RequestSubmitOrder sqnum clientIdentity order) = object ["request-sqnum" .= sqnum,
    "client-identity" .= clientIdentity,
    "order" .= order ]
  toJSON (RequestCancelOrder sqnum clientIdentity oid) = object ["request-sqnum" .= sqnum,
    "client-identity" .= clientIdentity,
    "cancel-order" .= oid ]
  toJSON (RequestNotifications sqnum clientIdentity initialNotificationSqnum) = object ["request-sqnum" .= sqnum,
    "client-identity" .= clientIdentity,
    "request-notifications" .= ("" :: T.Text),
    "initial-sqnum" .= unNotificationSqnum initialNotificationSqnum]
  toJSON (RequestCurrentSqnum sqnum clientIdentity) = object
    ["request-sqnum" .= sqnum,
     "client-identity" .= clientIdentity,
     "request-current-sqnum" .= ("" :: T.Text) ]

data BrokerServerResponse = ResponseOk
  | ResponseNotifications [Notification]
  | ResponseCurrentSqnum NotificationSqnum
  | ResponseError T.Text
  deriving (Eq, Show)

instance FromJSON BrokerServerResponse where
  parseJSON = withObject "object" (\obj ->
    if | HM.member "result" obj -> do
           result <- obj .: "result"
           if (result :: T.Text) == "success"
             then return ResponseOk
             else do
               msg <- obj .:? "message" .!= ""
               return (ResponseError msg)
       | HM.member "notifications" obj -> do
           notifications <- obj .: "notifications"
           ResponseNotifications <$> parseJSON notifications
       | HM.member "current-sqnum" obj -> do
           rawSqnum <- obj .: "current-sqnum"
           return $ ResponseCurrentSqnum (NotificationSqnum rawSqnum)
       | otherwise -> fail "Unable to parse BrokerServerResponse")

instance ToJSON BrokerServerResponse where
  toJSON ResponseOk = object [ "result" .= ("success" :: T.Text) ]
  toJSON (ResponseNotifications notifications) = object [ "notifications" .= notifications ]
  toJSON (ResponseCurrentSqnum sqnum) = object [ "current-sqnum" .= unNotificationSqnum sqnum ]
  toJSON (ResponseError errorMessage) = object [ "result" .= ("error" :: T.Text), "message" .= errorMessage ]

data TradeSinkMessage = TradeSinkHeartBeat | TradeSinkTrade {
  tsAccountId     :: T.Text,
  tsSecurity      :: T.Text,
  tsPrice         :: Double,
  tsQuantity      :: Int,
  tsVolume        :: Double,
  tsCurrency      :: T.Text,
  tsOperation     :: Operation,
  tsExecutionTime :: UTCTime,
  tsCommission    :: Double,
  tsSignalId      :: SignalId
} deriving (Show, Eq)

mkTradeMessage trade = TradeSinkTrade {
  tsAccountId = tradeAccount trade,
  tsSecurity = tradeSecurity trade,
  tsPrice = (toDouble . tradePrice) trade,
  tsQuantity = (fromInteger . tradeQuantity) trade,
  tsVolume = (toDouble . tradeVolume) trade,
  tsCurrency = tradeVolumeCurrency trade,
  tsOperation = tradeOperation trade,
  tsExecutionTime = tradeTimestamp trade,
  tsCommission = toDouble $ tradeCommission trade,
  tsSignalId = tradeSignalId trade
}
  where
    toDouble = fromRational . toRational

getHMS :: UTCTime -> (Int, Int, Int, Int)
getHMS (UTCTime _ diff) = (intsec `div` 3600, (intsec `mod` 3600) `div` 60, intsec `mod` 60, msec)
  where
    intsec = floor diff
    msec = floor $ (diff - fromIntegral intsec) * 1000

formatTimestamp dt = [t|%04d-%02d-%02d %02d:%02d:%02d.%03d|] y m d hour min sec msec
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
  toJSON TradeSinkTrade { .. } = object ["trade" .= object ["account" .= tsAccountId,
    "security" .= tsSecurity,
    "price" .= tsPrice,
    "quantity" .= tsQuantity,
    "volume" .= tsVolume,
    "volume-currency" .= tsCurrency,
    "operation" .= tsOperation,
    "execution-time" .= formatTimestamp tsExecutionTime,
    "commission" .= tsCommission,
    "strategy" .= strategyId tsSignalId,
    "signal-id" .= signalName tsSignalId,
    "comment" .= comment tsSignalId]]

instance FromJSON TradeSinkMessage where
  parseJSON = withObject "object" (\obj ->
    case HM.lookup "command" obj of
      Nothing  -> parseTrade obj
      Just cmd -> return TradeSinkHeartBeat)
    where
      parseTrade obj = case HM.lookup "trade" obj of
        Just (Object v) -> do
          acc <- v .: "account"
          sec <- v .: "security"
          pr <- v .: "price"
          q <- v .: "quantity"
          vol <- v .: "volume"
          cur <- v .: "volume-currency"
          op <- v .: "operation"
          commission <- v .:? "commission" .!= 0
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
            tsCommission = commission,
            tsSignalId = SignalId strategy sid com }
        _ -> fail "Should've been trade object"
