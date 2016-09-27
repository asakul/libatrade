{-# LANGUAGE OverloadedStrings, TypeSynonymInstances, FlexibleInstances #-}
{-# LANGUAGE MultiWayIf #-}

module TestTypes (
  properties
) where

import Test.Tasty
import Test.Tasty.SmallCheck as SC
import Test.Tasty.QuickCheck as QC
import Test.Tasty.HUnit
import Test.QuickCheck.Instances hiding (Text)

import ATrade.Types

import Data.Aeson
import Data.Aeson.Types
import Data.Decimal
import Data.Scientific
import Data.Text
import Data.Time.Calendar
import Data.Time.Clock
import Data.Tuple.Select

notTooBig x = abs x < 1000000000000

instance Arbitrary Tick where
  arbitrary = Tick <$>
    arbitrary <*>
    arbitrary <*>
    arbitraryTimestamp <*>
    (roundTo 9 <$> (arbitrary `suchThat` notTooBig)) <*>
    arbitrary
    where
      arbitraryTimestamp = do
        y <- choose (1970, 2050)
        m <- choose (1, 12)
        d <- choose (1, 31)

        sec <- secondsToDiffTime <$> choose (0, 86399)

        return $ UTCTime (fromGregorian y m d) sec

instance Arbitrary DataType where
  arbitrary = toEnum <$> choose (1, 10)

instance Arbitrary Decimal where
  arbitrary = realFracToDecimal 10 <$> (arbitrary :: Gen Scientific)

instance Arbitrary SignalId where
  arbitrary = SignalId <$> arbitrary <*> arbitrary <*> arbitrary

instance Arbitrary OrderPrice where
  arbitrary = do
    v <- choose (1, 4) :: Gen Int
    if | v == 1 -> return Market
       | v == 2 -> Limit <$> arbitrary `suchThat` notTooBig
       | v == 3 -> Stop <$> arbitrary `suchThat` notTooBig <*> arbitrary `suchThat` notTooBig
       | v == 4 -> StopMarket <$> arbitrary `suchThat` notTooBig
       | otherwise -> fail "Invalid case"

instance Arbitrary Operation where
  arbitrary = elements [Buy, Sell]

instance Arbitrary OrderState where
  arbitrary = elements [Unsubmitted,
    Submitted,
    PartiallyExecuted,
    Executed,
    Cancelled,
    Rejected,
    OrderError ]

instance Arbitrary Order where
  arbitrary = Order <$>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary

instance Arbitrary Trade where
  arbitrary = Trade <$>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary

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
