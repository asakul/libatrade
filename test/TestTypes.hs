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
import ArbitraryInstances

import Data.Aeson
import Data.Aeson.Types
import Data.Decimal
import Data.Scientific
import Data.Text
import Data.Time.Calendar
import Data.Time.Clock
import Data.Tuple.Select

properties = testGroup "Types" [
    testTickSerialization
  , testSignalIdSerialization
  , testOrderPriceSerialization
  , testOperationSerialization
  , testOrderStateSerialization
  , testOrderSerialization
  , testTradeSerialization
  ]

testTickSerialization = QC.testProperty "Deserialize serialized tick"
  (\tick -> case (deserializeTick . serializeTick) tick of
    Just t -> tick == t
    Nothing -> False)

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
