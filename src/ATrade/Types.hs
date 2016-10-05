{-# LANGUAGE OverloadedStrings, TypeSynonymInstances, FlexibleInstances #-}

module ATrade.Types (
  TickerId,
  Tick(..),
  DataType(..),
  serializeTick,
  deserializeTick,
  SignalId(..),
  OrderPrice(..),
  Operation(..),
  OrderState(..),
  Order(..),
  mkOrder,
  Trade(..),
  OrderId(..)
) where

import Control.Monad
import Data.Aeson
import Data.Aeson.Types
import Data.Binary.Builder
import Data.Binary.Get
import Data.ByteString.Lazy as B
import Data.DateTime
import Data.Decimal
import Data.Int
import Data.List as L
import Data.Maybe
import Data.Ratio
import Data.Text as T
import Data.Text.Encoding as E
import Data.Time.Clock
import Data.Word

type TickerId = T.Text

data DataType = Unknown
  | Price
  | OpenInterest
  | BestBid
  | BestOffer
  | Depth
  | TheoryPrice
  | Volatility
  | TotalSupply
  | TotalDemand
  deriving (Show, Eq, Ord)

instance Enum DataType where
  fromEnum x
    | x == Price = 1
    | x == OpenInterest = 3
    | x == BestBid = 4
    | x == BestOffer = 5
    | x == Depth = 6
    | x == TheoryPrice = 7
    | x == Volatility = 8
    | x == TotalSupply = 9
    | x == TotalDemand = 10
    | x == Unknown = -1
    | otherwise = -1

  toEnum x
    | x == 1 = Price
    | x == 3 = OpenInterest
    | x == 4 = BestBid
    | x == 5 = BestOffer
    | x == 6 = Depth
    | x == 7 = TheoryPrice
    | x == 8 = Volatility
    | x == 9 = TotalSupply
    | x == 10 = TotalDemand
    | otherwise = Unknown

data Tick = Tick {
  security :: !T.Text,
  datatype :: !DataType,
  timestamp :: !UTCTime,
  value :: !Decimal,
  volume :: !Integer
} deriving (Show, Eq)

serializeTick :: Tick -> [ByteString]
serializeTick tick = header : [rawdata]
  where
    header = B.fromStrict . E.encodeUtf8 $ security tick
    rawdata = toLazyByteString $ mconcat [
      putWord32le 1,
      putWord64le $ fromIntegral . toSeconds' . timestamp $ tick,
      putWord32le $ fromIntegral . floor . (* 1000000) . fracSeconds . timestamp $ tick,
      putWord32le $ fromIntegral . fromEnum . datatype $ tick,
      putWord64le $ truncate . value $ tick,
      putWord32le $ truncate . (* 1000000000) . fractionalPart $ value tick,
      putWord32le $ fromIntegral $ volume tick ]
    floorPart :: (RealFrac a) => a -> a
    floorPart x = x - fromIntegral (floor x)
    fractionalPart :: (RealFrac a) => a -> a
    fractionalPart x = x - fromIntegral (truncate x)
    toSeconds' t = floor $ diffUTCTime t epoch
    fracSeconds t = floorPart $ diffUTCTime t epoch
    epoch = fromGregorian 1970 1 1 0 0 0


deserializeTick :: [ByteString] -> Maybe Tick
deserializeTick (header:rawData:_) = case runGetOrFail parseTick rawData of
  Left (_, _, _) -> Nothing
  Right (_, _, tick) -> Just $ tick { security = E.decodeUtf8 . B.toStrict $ header }
  where
    parseTick :: Get Tick
    parseTick = do
      packetType <- fromEnum <$> getWord32le
      when (packetType /= 1) $ fail "Expected packettype == 1"
      tsec <- getWord64le
      tusec <- getWord32le
      dt <- toEnum . fromEnum <$> getWord32le
      intpart <- (fromIntegral <$> getWord64le) :: Get Int64
      nanopart <- (fromIntegral <$> getWord32le) :: Get Int32
      volume <- fromIntegral <$> (fromIntegral <$> getWord32le :: Get Int32)
      return Tick { security = "",
        datatype = dt,
        timestamp = makeTimestamp tsec tusec,
        value = makeValue intpart nanopart,
        volume = volume }

    makeTimestamp :: Word64 -> Word32 -> UTCTime
    makeTimestamp sec usec = addUTCTime (fromRational $ toInteger usec % 1000000) (fromSeconds . toInteger $ sec)

    makeValue :: Int64 -> Int32 -> Decimal
    makeValue intpart nanopart = case eitherFromRational r of
      Right v -> v + convertedIntPart
      Left _ -> convertedIntPart
      where
        convertedIntPart = realFracToDecimal 10 (fromIntegral intpart) 
        r = toInteger nanopart % 1000000000

deserializeTick _ = Nothing

data SignalId = SignalId {
  strategyId :: T.Text,
  signalName :: T.Text,
  comment :: T.Text }
  deriving (Show, Eq)

instance FromJSON SignalId where
  parseJSON (Object o) = SignalId <$>
    o .: "strategy-id" .!= ""     <*>
    o .: "signal-name" .!= ""     <*>
    o .: "comment"      .!= ""
  parseJSON _ = fail "Should be object"

instance ToJSON SignalId where
  toJSON sid = object [ "strategy-id" .= strategyId sid,
    "signal-name" .= signalName sid,
    "comment" .= comment sid ]

instance FromJSON Decimal where
  parseJSON = withScientific "number" (return . realFracToDecimal 10 . toRational) 

instance ToJSON Decimal where
  toJSON = Number . fromRational . toRational

data OrderPrice = Market | Limit Decimal | Stop Decimal Decimal | StopMarket Decimal
  deriving (Show, Eq)

decimal :: (RealFrac r) => r -> Decimal
decimal = realFracToDecimal 10

instance FromJSON OrderPrice where
  parseJSON (String s) = when (s /= "market") (fail "If string, then should be 'market'") >>
    return Market

  parseJSON (Number n) = return $ Limit $ decimal n
  parseJSON (Object v) = do
    triggerPrice <- v .: "trigger" :: Parser Double
    execPrice <- v .: "execution"
    case execPrice of
      (String s) -> when (s /= "market") (fail "If string, then should be 'market'") >> return $ StopMarket (decimal triggerPrice)
      (Number n) -> return $ Stop (decimal triggerPrice) (decimal n)
      _ -> fail "Should be either number or 'market'"

  parseJSON _ = fail "OrderPrice"

instance ToJSON OrderPrice where
  toJSON op = case op of
    Market -> String "market"
    (Limit d) -> Number $ convert d
    (Stop t e) -> object [ "trigger" .= convert t, "execution" .= convert e ]
    (StopMarket t) -> object [ "trigger" .= convert t, "execution" .= ("market" :: Text) ]
    where
      convert = fromRational . toRational

data Operation = Buy | Sell
  deriving (Show, Eq)

instance FromJSON Operation where
  parseJSON (String s)
    | s == "buy" = return Buy
    | s == "sell" = return Sell
    | otherwise = fail "Should be either 'buy' or 'sell'"
  parseJSON _ = fail "Should be string"

instance ToJSON Operation where
  toJSON Buy = String "buy"
  toJSON Sell = String "sell"

data OrderState = Unsubmitted
  | Submitted
  | PartiallyExecuted
  | Executed
  | Cancelled
  | Rejected
  | OrderError
  deriving (Show, Eq)

instance FromJSON OrderState where
  parseJSON (String s)
    | s == "unsubmitted" = return Unsubmitted
    | s == "submitted" = return Submitted
    | s == "partially-executed" = return PartiallyExecuted
    | s == "executed" = return Executed
    | s == "cancelled" = return Cancelled
    | s == "rejected" = return Rejected
    | s == "error" = return OrderError
    | otherwise = fail "Invlaid state"

  parseJSON _ = fail "Should be string"

instance ToJSON OrderState where
  toJSON os = case os of
    Unsubmitted -> String "unsubmitted"
    Submitted -> String "submitted"
    PartiallyExecuted -> String "partially-executed"
    Executed -> String "executed"
    Cancelled -> String "cancelled"
    Rejected -> String "rejected"
    OrderError -> String "error"

type OrderId = Integer

data Order = Order {
  orderId :: OrderId,
  orderAccountId :: T.Text,
  orderSecurity :: T.Text,
  orderPrice :: OrderPrice,
  orderQuantity :: Integer,
  orderExecutedQuantity :: Integer,
  orderOperation :: Operation,
  orderState :: OrderState,
  orderSignalId :: SignalId }
  deriving (Show, Eq)

mkOrder = Order { orderId = 0,
  orderAccountId = "",
  orderSecurity = "",
  orderPrice = Market,
  orderQuantity = 0,
  orderExecutedQuantity = 0,
  orderOperation = Buy,
  orderState = Unsubmitted,
  orderSignalId = SignalId "" "" "" }

instance FromJSON Order where
  parseJSON (Object v) = Order      <$>
    v .:? "order-id" .!= 0          <*>
    v .:  "account"                 <*>
    v .:  "security"                <*>
    v .:  "price"                   <*>
    v .:  "quantity"                <*>
    v .:? "executed-quantity" .!= 0 <*>
    v .:  "operation"               <*>
    v .:  "state" .!= Unsubmitted   <*>
    v .:  "signal-id"

  parseJSON _ = fail "Should be string"

instance ToJSON Order where
  toJSON order = object $ base ++ catMaybes [ifMaybe "order-id" (/= 0) (orderId order), ifMaybe "executed-quantity" (/= 0) (orderExecutedQuantity order)]
    where
      base = [ "account" .= orderAccountId order,
        "security" .= orderSecurity order,
        "price" .= orderPrice order,
        "quantity" .= orderQuantity order,
        "operation" .= orderOperation order,
        "state" .= orderState order,
        "signal-id" .= orderSignalId order ]
      ifMaybe :: (ToJSON a, KeyValue b) => Text -> (a -> Bool) -> a -> Maybe b
      ifMaybe name pred val = if pred val then Just (name .= val) else Nothing

data Trade = Trade {
  tradeOrderId :: OrderId,
  tradePrice :: Decimal,
  tradeQuantity :: Integer,
  tradeVolume :: Decimal,
  tradeVolumeCurrency :: T.Text,
  tradeOperation :: Operation,
  tradeAccount :: T.Text,
  tradeSecurity :: T.Text,
  tradeTimestamp :: UTCTime,
  tradeSignalId :: SignalId }
  deriving (Show, Eq)

instance FromJSON Trade where
  parseJSON (Object trade) = Trade  <$>
    trade .: "order-id"             <*>
    trade .: "price"                <*>
    trade .: "quantity"             <*>
    trade .: "volume"               <*>
    trade .: "volume-currency"      <*>
    trade .: "operation"            <*>
    trade .: "account"              <*>
    trade .: "security"             <*>
    trade .: "execution-time"       <*>
    trade .: "signal-id"
  parseJSON _ = fail "Should be object"

instance ToJSON Trade where
  toJSON trade = object [ "order-id" .= tradeOrderId trade,
    "price" .= tradePrice trade,
    "quantity" .= tradeQuantity trade,
    "volume" .= tradeVolume trade,
    "volume-currency" .= tradeVolumeCurrency trade,
    "operation" .= tradeOperation trade,
    "account" .= tradeAccount trade,
    "security" .= tradeSecurity trade,
    "execution-time" .= tradeTimestamp trade,
    "signal-id" .= tradeSignalId trade]
