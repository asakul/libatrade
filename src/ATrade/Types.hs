{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TypeSynonymInstances #-}

module ATrade.Types (
  TickerId,
  Tick(..),
  Bar(..),
  serializeBar,
  serializeBarBody,
  deserializeBar,
  BarTimeframe(..),
  DataType(..),
  serializeTick,
  serializeTickBody,
  deserializeTick,
  deserializeTickBody,
  SignalId(..),
  OrderPrice(..),
  Operation(..),
  OrderState(..),
  Order(..),
  mkOrder,
  Trade(..),
  OrderId(..),
  ServerSecurityParams(..),
  defaultServerSecurityParams,
  ClientSecurityParams(..),
  defaultClientSecurityParams,
  module ATrade.Price
) where

import           GHC.Generics

import           ATrade.Price

import           Control.Monad
import           Data.Aeson
import           Data.Aeson.Types
import           Data.Binary.Get
import           Data.Binary.Put
import           Data.ByteString.Lazy  as B
import           Data.DateTime
import           Data.Int
import           Data.List             as L
import           Data.Maybe
import           Data.Ratio
import           Data.Text             as T
import           Data.Text.Encoding    as E
import           Data.Time.Clock
import           Data.Time.Clock.POSIX
import           Data.Word

import           System.ZMQ4.ZAP

type TickerId = T.Text

data DataType = Unknown
  | LastTradePrice
  | OpenInterest
  | BestBid
  | BestOffer
  | Depth
  | TheoryPrice
  | Volatility
  | TotalSupply
  | TotalDemand
  deriving (Show, Eq, Ord, Generic)

instance Enum DataType where
  fromEnum x
    | x == LastTradePrice = 1
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
    | x == 1 = LastTradePrice
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
  security  :: !T.Text,
  datatype  :: !DataType,
  timestamp :: !UTCTime,
  value     :: !Price,
  volume    :: !Integer
} deriving (Show, Eq, Generic)

putPrice :: Price -> Put
putPrice price = do
  let (i, f) = decompose price
  putWord64le $ fromInteger . toInteger $ i
  putWord32le $ (* 1000) . fromInteger . toInteger $ f

parsePrice :: Get Price
parsePrice = do
  intpart <- (fromIntegral <$> getWord64le) :: Get Int64
  nanopart <- (fromIntegral <$> getWord32le) :: Get Int32
  return $ compose (intpart, nanopart `div` 1000)

serializeTickHeader :: Tick -> ByteString
serializeTickHeader tick = B.fromStrict . E.encodeUtf8 $ security tick

serializeTickBody :: Tick -> ByteString
serializeTickBody tick = runPut $ do
      putWord32le 1
      putWord64le $ fromIntegral . toSeconds' . timestamp $ tick
      putWord32le $ fromIntegral . fracSeconds . timestamp $ tick
      putWord32le $ fromIntegral . fromEnum . datatype $ tick
      putPrice $ value tick
      putWord32le $ fromIntegral $ volume tick
  where
    fractionalPart :: (RealFrac a) => a -> a
    fractionalPart x = x - fromIntegral (truncate x)
    toSeconds' = floor . utcTimeToPOSIXSeconds
    fracSeconds t = (truncate $ (* 1000000000000) $ utcTimeToPOSIXSeconds t) `mod` 1000000000000 `div` 1000000


serializeTick :: Tick -> [ByteString]
serializeTick tick = serializeTickHeader tick : [serializeTickBody tick]

parseTick :: Get Tick
parseTick = do
  packetType <- fromEnum <$> getWord32le
  when (packetType /= 1) $ fail "Expected packettype == 1"
  tsec <- getWord64le
  tusec <- getWord32le
  dt <- toEnum . fromEnum <$> getWord32le
  price <- parsePrice
  volume <- fromIntegral <$> (fromIntegral <$> getWord32le :: Get Int32)
  return Tick { security = "",
    datatype = dt,
    timestamp = makeTimestamp tsec tusec,
    value = price,
    volume = volume }

makeTimestamp :: Word64 -> Word32 -> UTCTime
makeTimestamp sec usec = addUTCTime (fromRational $ toInteger usec % 1000000) (fromSeconds . toInteger $ sec)

deserializeTick :: [ByteString] -> Maybe Tick
deserializeTick (header:rawData:_) = case runGetOrFail parseTick rawData of
  Left (_, _, _) -> Nothing
  Right (_, _, tick) -> Just $ tick { security = E.decodeUtf8 . B.toStrict $ header }

deserializeTick _ = Nothing

deserializeTickBody :: ByteString -> (ByteString, Maybe Tick)
deserializeTickBody bs = case runGetOrFail parseTick bs of
  Left (rest, _, _)     -> (rest, Nothing)
  Right (rest, _, tick) -> (rest, Just tick)

data Bar = Bar {
  barSecurity  :: !TickerId,
  barTimestamp :: !UTCTime,
  barOpen      :: !Price,
  barHigh      :: !Price,
  barLow       :: !Price,
  barClose     :: !Price,
  barVolume    :: !Integer
} deriving (Show, Eq, Generic)

-- | Stores timeframe in seconds
newtype BarTimeframe = BarTimeframe { unBarTimeframe :: Int }
  deriving (Show, Eq)

serializeBar :: BarTimeframe -> Bar -> [ByteString]
serializeBar tf bar = serializeBarHeader tf bar : [serializeBarBody tf bar]

-- | Encodes bar header as tickerid:timeframe_seconds;
-- Why ';' at the end? To support correct 0mq subscriptions. When we subscribe to topic,
-- we actually subscribe by all topics which has requested subscription as a prefix.
serializeBarHeader :: BarTimeframe -> Bar -> ByteString
serializeBarHeader tf bar =
  B.fromStrict . E.encodeUtf8 $ (barSecurity bar) `T.append` encodeTimeframe tf
  where
    encodeTimeframe tf = mconcat [ ":", (T.pack . show $ unBarTimeframe tf), ";" ]

serializeBarBody :: BarTimeframe -> Bar -> ByteString
serializeBarBody tf bar = runPut $ do
  putWord32le 2
  putWord32le $ fromIntegral $ unBarTimeframe tf
  putWord64le $ fromIntegral . toSeconds' . barTimestamp $ bar
  putWord32le $ fromIntegral . fracSeconds . barTimestamp $ bar
  putPrice $ barOpen bar
  putPrice $ barHigh bar
  putPrice $ barLow bar
  putPrice $ barClose bar
  putWord32le $ fromIntegral $ barVolume bar
  where
    fractionalPart :: (RealFrac a) => a -> a
    fractionalPart x = x - fromIntegral (truncate x)
    toSeconds' = floor . utcTimeToPOSIXSeconds
    fracSeconds t = (truncate $ (* 1000000000000) $ utcTimeToPOSIXSeconds t) `mod` 1000000000000 `div` 1000000

parseBar :: Get (BarTimeframe, Bar)
parseBar = do
  packetType <- fromEnum <$> getWord32le
  when (packetType /= 2) $ fail "Expected packettype == 2"
  tf <- fromIntegral <$> getWord32le
  tsec <- getWord64le
  tusec <- getWord32le
  open_ <- parsePrice
  high_ <- parsePrice
  low_ <- parsePrice
  close_ <- parsePrice
  volume_ <- fromIntegral <$> getWord32le
  return (BarTimeframe tf, Bar { barSecurity = "",
               barTimestamp = makeTimestamp tsec tusec,
               barOpen = open_,
               barHigh = high_,
               barLow = low_,
               barClose = close_,
               barVolume = volume_ })

deserializeBar :: [ByteString] -> Maybe (BarTimeframe, Bar)
deserializeBar (header:rawData:_) = case runGetOrFail parseBar rawData of
  Left (_, _, _) -> Nothing
  Right (_, _, (tf, bar)) -> Just $ (tf, bar { barSecurity = T.takeWhile (/= ':') . E.decodeUtf8 . B.toStrict $ header })

deserializeBar _ = Nothing


data SignalId = SignalId {
  strategyId :: T.Text,
  signalName :: T.Text,
  comment    :: T.Text }
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

data OrderPrice = Market | Limit Price | Stop Price Price | StopMarket Price
  deriving (Show, Eq)

instance FromJSON OrderPrice where
  parseJSON (String s) = when (s /= "market") (fail "If string, then should be 'market'") >>
    return Market

  parseJSON a@(Number n) = do
    price <- parseJSON a
    return $ Limit price
  parseJSON (Object v) = do
    triggerPrice <- v .: "trigger"
    case triggerPrice of
      a@(Number trpr) -> do
        trprice <- parseJSON a
        execPrice <- v .: "execution"
        case execPrice of
          (String s) -> if s /= "market"
              then (fail "If string, then should be 'market'")
              else return $ StopMarket trprice
          (Number n) -> return $ Stop trprice (fromScientific n)
          _ -> fail "Should be either number or 'market'"
      _ -> fail "Should be a number"

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
  toJSON Buy  = String "buy"
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
    Unsubmitted       -> String "unsubmitted"
    Submitted         -> String "submitted"
    PartiallyExecuted -> String "partially-executed"
    Executed          -> String "executed"
    Cancelled         -> String "cancelled"
    Rejected          -> String "rejected"
    OrderError        -> String "error"

type OrderId = Integer

data Order = Order {
  orderId               :: OrderId,
  orderAccountId        :: T.Text,
  orderSecurity         :: T.Text,
  orderPrice            :: OrderPrice,
  orderQuantity         :: Integer,
  orderExecutedQuantity :: Integer,
  orderOperation        :: Operation,
  orderState            :: OrderState,
  orderSignalId         :: SignalId }
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
  tradeOrderId        :: OrderId,
  tradePrice          :: Price,
  tradeQuantity       :: Integer,
  tradeVolume         :: Price,
  tradeVolumeCurrency :: T.Text,
  tradeOperation      :: Operation,
  tradeAccount        :: T.Text,
  tradeSecurity       :: T.Text,
  tradeTimestamp      :: UTCTime,
  tradeCommission     :: Price,
  tradeSignalId       :: SignalId }
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
    trade .:? "commission" .!= 0    <*>
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
    "commission" .= tradeCommission trade,
    "signal-id" .= tradeSignalId trade]

data ServerSecurityParams = ServerSecurityParams {
  sspDomain      :: Maybe T.Text,
  sspCertificate :: Maybe CurveCertificate
} deriving (Show, Eq)

defaultServerSecurityParams = ServerSecurityParams {
  sspDomain = Nothing,
  sspCertificate = Nothing
}

data ClientSecurityParams = ClientSecurityParams {
  cspDomain            :: Maybe T.Text,
  cspCertificate       :: Maybe CurveCertificate,
  cspServerCertificate :: Maybe CurveCertificate
} deriving (Show, Eq)

defaultClientSecurityParams = ClientSecurityParams {
  cspCertificate = Nothing,
  cspServerCertificate = Nothing
}

data TickerInfo = TickerInfo {
  tiTicker   :: TickerId,
  tiClass    :: T.Text,
  tiBase     :: Maybe TickerId,
  tiLotSize  :: Integer,
  tiTickSize :: Price
} deriving (Show, Eq)

