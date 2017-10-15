{-# LANGUAGE MultiWayIf #-}

module ATrade.Price (
  Price(..),
  fromDouble,
  toDouble,
  decompose
) where

import Data.Int

data Price = Price {
  priceQuants :: !Int64
} deriving (Eq, Show, Ord)

giga :: Int64
giga = 1000000000

mega :: Int64
mega = 1000000

instance Num Price where
  a + b = Price {
    priceQuants = priceQuants a + priceQuants b }

  a * b = Price {
    priceQuants = (priceQuants a * priceQuants b) `div` mega }

  abs a = a { priceQuants = abs (priceQuants a) }

  signum a = a { priceQuants = signum (priceQuants a)}

  fromInteger int = Price { priceQuants = mega * fromInteger int} 

  negate a = a { priceQuants = negate (priceQuants a) }

toDouble :: Price -> Double
toDouble p = fromIntegral (priceQuants p) / fromIntegral mega

fromDouble :: Double -> Price
fromDouble d = Price { priceQuants = truncate (d * fromIntegral mega) }

decompose :: Price -> (Int64, Int32)
decompose Price{priceQuants = p} = (p `div` mega, (fromInteger . toInteger) $ p `mod` mega)

