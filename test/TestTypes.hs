{-# LANGUAGE OverloadedStrings, TypeSynonymInstances, FlexibleInstances #-}
{-# LANGUAGE MultiWayIf #-}

module TestTypes (
  properties
) where

import Test.Tasty
import Test.Tasty.SmallCheck as SC
import Test.Tasty.QuickCheck as QC
import Test.Tasty.HUnit

import ATrade.Types
import ATrade.Price as P
import ArbitraryInstances

import Data.Aeson
import Data.Aeson.Types
import Data.Decimal
import Data.Scientific
import Data.Text
import Data.Time.Calendar
import Data.Time.Clock
import Data.Tuple.Select
import qualified Data.ByteString.Lazy as B

properties = testGroup "Types" [
    testTickSerialization
  , testTickBodySerialization
  , testSignalIdSerialization
  , testOrderPriceSerialization
  , testOperationSerialization
  , testOrderStateSerialization
  , testOrderSerialization
  , testTradeSerialization
  , testPrice1
  , testPrice2
  ]

testTickSerialization = QC.testProperty "Deserialize serialized tick"
  (\tick -> case (deserializeTick . serializeTick) tick of
    Just t -> tick == t
    Nothing -> False)

-- Adjust arbitrary instances of ticks, because body doesn't store security name
testTickBodySerialization = QC.testProperty "Deserialize serialized bunch of tick" $
  QC.forAll (arbitrary >>= (\t -> return t { security = "" })) (\tick1 -> 
    QC.forAll (arbitrary >>= (\t -> return t { security = "" })) (\tick2 ->
      case deserializeTickBody (serialized tick1 tick2) of
        (rest, Just t1) -> case deserializeTickBody rest of
          (_, Just t2) -> tick1 == t1 && tick2 == t2
          _ -> False
        _ -> False))
  where
    serialized t1 t2 = serializeTickBody t1 `B.append` serializeTickBody t2

testSignalIdSerialization = QC.testProperty "Deserialize serialized SignalId"
  (\sid -> case (decode . encode $ sid :: Maybe SignalId) of
    Just s -> s == sid
    Nothing -> False)

testOrderPriceSerialization = QC.testProperty "Deserialize serialized OrderPrice"
  (\v -> case (decode . encode $ v :: Maybe OrderPrice) of
    Just s -> s == v
    Nothing -> False)

testOperationSerialization = QC.testProperty "Deserialize serialized Operation"
  (\v -> case (decode . encode $ v :: Maybe Operation) of
    Just s -> s == v
    Nothing -> False)

testOrderStateSerialization = QC.testProperty "Deserialize serialized OrderState"
  (\v -> case (decode . encode $ v :: Maybe OrderState) of
    Just s -> s == v
    Nothing -> False)

testOrderSerialization = QC.testProperty "Deserialize serialized Order"
  (\v -> case (decode . encode $ v :: Maybe Order) of
    Just s -> s == v
    Nothing -> False)

testTradeSerialization = QC.testProperty "Deserialize serialized Trade"
  (\v -> case (decode . encode $ v :: Maybe Trade) of
    Just s -> s == v
    Nothing -> False)

testPrice1 = QC.testProperty "fromDouble . toDouble $ Price"
  (\p -> let newp = (P.fromDouble . P.toDouble) p in
    (abs (priceQuants newp - priceQuants p) < 1000))

testPrice2 = QC.testProperty "toDouble . fromDouble $ Price" $
  QC.forAll (arbitrary `suchThat` (< 1000000000)) (\d -> let newd = (P.toDouble . P.fromDouble) d in
    (abs (newd - d) < 0.000001))

