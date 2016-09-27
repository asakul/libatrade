{-# LANGUAGE TypeSynonymInstances, FlexibleInstances #-}

module TestTypes (
  properties
) where

import Test.Tasty
import Test.Tasty.SmallCheck as SC
import Test.Tasty.QuickCheck as QC
import Test.Tasty.HUnit

import ATrade.Types
import Data.Decimal
import Data.Time.Clock
import Data.Time.Calendar
import Data.Scientific
import Data.Tuple.Select

import Test.QuickCheck.Instances hiding (Text)

instance Arbitrary Tick where
  arbitrary = Tick <$>
    arbitrary <*>
    arbitrary <*>
    arbitraryTimestamp <*>
    (roundTo 9 <$> (arbitrary `suchThat` (\x -> abs x < 1000000000000))) <*>
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

properties = testGroup "Types" [ testTickSerialization ]

testTickSerialization = QC.testProperty "Deserialize serialized tick"
  (\tick -> case (deserializeTick . serializeTick) tick of
    Just t -> tick == t
    Nothing -> False)

