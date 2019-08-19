{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE MultiWayIf           #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TypeSynonymInstances #-}

module TestTypes (
  properties
) where

import           Test.Tasty
import           Test.Tasty.QuickCheck as QC

import           ATrade.Price          as P
import           ATrade.Types

import           ArbitraryInstances    ()
import           Data.Aeson
import qualified Data.ByteString.Lazy  as B

import           Debug.Trace

properties :: TestTree
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
  , testPriceDecompose
  , testPriceAddition
  , testPriceMultiplication
  , testPriceSubtraction
  , testBarSerialization
  ]

testTickSerialization :: TestTree
testTickSerialization = QC.testProperty "Deserialize serialized tick"
  (\tick -> case (deserializeTick . serializeTick) tick of
    Just t  -> tick == t
    Nothing -> False)

-- Adjust arbitrary instances of ticks, because body doesn't store security name
testTickBodySerialization :: TestTree
testTickBodySerialization = QC.testProperty "Deserialize serialized bunch of tick" $
  QC.forAll (arbitrary >>= (\t -> return t { security = "" })) (\tick1 ->
    QC.forAll (arbitrary >>= (\t -> return t { security = "" })) (\tick2 ->
      case deserializeTickBody (serialized tick1 tick2) of
        (rest, Just t1) -> case deserializeTickBody rest of
          (_, Just t2) -> tick1 == t1 && tick2 == t2
          _            -> False
        _ -> False))
  where
    serialized t1 t2 = serializeTickBody t1 `B.append` serializeTickBody t2

testSignalIdSerialization :: TestTree
testSignalIdSerialization = QC.testProperty "Deserialize serialized SignalId"
  (\sid -> case (decode . encode $ sid :: Maybe SignalId) of
    Just s  -> s == sid
    Nothing -> False)

testOrderPriceSerialization :: TestTree
testOrderPriceSerialization = QC.testProperty "Deserialize serialized OrderPrice"
  (\v -> case (decode . encode $ v :: Maybe OrderPrice) of
    Just s  -> s == v
    Nothing -> False)

testOperationSerialization :: TestTree
testOperationSerialization = QC.testProperty "Deserialize serialized Operation"
  (\v -> case (decode . encode $ v :: Maybe Operation) of
    Just s  -> s == v
    Nothing -> False)

testOrderStateSerialization :: TestTree
testOrderStateSerialization = QC.testProperty "Deserialize serialized OrderState"
  (\v -> case (decode . encode $ v :: Maybe OrderState) of
    Just s  -> s == v
    Nothing -> False)

testOrderSerialization :: TestTree
testOrderSerialization = QC.testProperty "Deserialize serialized Order"
  (\v -> case (decode . encode $ v :: Maybe Order) of
    Just s  -> s == v
    Nothing -> False)

testTradeSerialization :: TestTree
testTradeSerialization = QC.testProperty "Deserialize serialized Trade"
  (\v -> case (decode . encode $ v :: Maybe Trade) of
    Just s  -> s == v
    Nothing -> False)

testPrice1 :: TestTree
testPrice1 = QC.testProperty "fromDouble . toDouble $ Price" $
  QC.forAll (arbitrary `suchThat` (\x -> abs x < 100000000)) (\p -> let newp = (P.fromDouble . P.toDouble) p in
    (priceQuants newp == priceQuants p))

testPrice2 :: TestTree
testPrice2 = QC.testProperty "toDouble . fromDouble $ Price" $
  QC.forAll (arbitrary `suchThat` (< 1000000000)) (\d -> let newd = (P.toDouble . P.fromDouble) d in
    (abs (newd - d) < 0.000001))

testPriceDecompose :: TestTree
testPriceDecompose = QC.testProperty "Price decompose"
  (\p -> let (i, f) = decompose p in
    i * 1000000 + (fromInteger . fromIntegral) f == priceQuants p)

testPriceAddition :: TestTree
testPriceAddition = QC.testProperty "Price addition"
  (\(p1, p2) -> abs (toDouble p1 + toDouble p2 - toDouble (p1 + p2)) < 0.00001)

testPriceMultiplication :: TestTree
testPriceMultiplication = QC.testProperty "Price multiplication" $
  QC.forAll (arbitrary `suchThat` (\(p1, p2) -> p1 < 100000 && p2 < 100000))
    (\(p1, p2) -> abs (toDouble p1 + toDouble p2 - toDouble (p1 + p2)) < 0.00001)

testPriceSubtraction :: TestTree
testPriceSubtraction = QC.testProperty "Price subtraction"
  (\(p1, p2) -> abs (toDouble p1 - toDouble p2 - toDouble (p1 - p2)) < 0.00001)

testBarSerialization :: TestTree
testBarSerialization = QC.testProperty "Deserialize serialized bar"
  (\(tf, bar) -> case deserializeBar (serializeBar tf bar) of
    Just (tf', bar') -> bar == bar' && tf == tf'
    Nothing          -> False)
