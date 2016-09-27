{-# LANGUAGE OverloadedStrings #-}

module ATrade.Types (
  Tick(..),
  DataType(..),
  serializeTick,
  deserializeTick
) where

import Data.Decimal
import Data.Time.Clock
import Data.DateTime
import Data.ByteString.Lazy as B
import Data.Text as T
import Data.Text.Encoding as E
import Data.List as L
import Data.Binary.Builder
import Data.Binary.Get
import Data.Int
import Data.Word
import Data.Ratio
import Control.Monad

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
  security :: T.Text,
  datatype :: DataType,
  timestamp :: UTCTime,
  value :: Decimal,
  volume :: Integer
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
