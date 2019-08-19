{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}

module ATrade.Price (
  Price(..),
  fromDouble,
  toDouble,
  decompose,
  compose,
  fromScientific,
  toScientific
) where

import           Data.Int
import           Data.Ratio

import           Data.Aeson
import           Data.Scientific

import           Text.Printf

data Price = Price {
  priceQuants :: !Int64
} deriving (Eq, Ord)

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
fromDouble d
  | d >= 0 = Price { priceQuants = truncate ((d * fromIntegral mega) + 0.5) }
  | otherwise = Price { priceQuants = truncate ((d * fromIntegral mega) - 0.5) }

toScientific :: Price -> Scientific
toScientific p = normalize $ scientific (toInteger $ priceQuants p) (-6)

fromScientific :: Scientific -> Price
fromScientific d = Price { priceQuants = if base10Exponent nd >= -6 then fromInteger $ coefficient nd * (10 ^ (base10Exponent nd + 6)) else 0 }
  where
    nd = normalize d

decompose :: Price -> (Int64, Int32)
decompose Price{priceQuants = p} = (p `div` mega, (fromInteger . toInteger) $ p `mod` mega)

compose :: (Int64, Int32) -> Price
compose (int, frac) = Price { priceQuants = int * mega + (fromInteger . toInteger) frac }

instance FromJSON Price where
  parseJSON = withScientific "number" (\x -> let nx = normalize x in
    return Price { priceQuants = if base10Exponent nx >= -6 then fromInteger $ coefficient nx * (10 ^ (base10Exponent nx + 6)) else 0 })

instance ToJSON Price where
  toJSON x = Number (normalize $ scientific (toInteger $ priceQuants x) (-6))

instance Real Price where
  toRational a = (toInteger . priceQuants $ a) % toInteger mega

instance Fractional Price where
  fromRational a = fromInteger (numerator a) / fromInteger (denominator a)
  a / b = fromDouble $ toDouble a / toDouble b

instance Show Price where
  show = printf "%.8f" . toDouble

