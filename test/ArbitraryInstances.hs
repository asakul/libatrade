{-# LANGUAGE OverloadedStrings, TypeSynonymInstances, FlexibleInstances #-}
{-# LANGUAGE MultiWayIf #-}

module ArbitraryInstances (
) where


import Test.Tasty.QuickCheck as QC
import Test.QuickCheck.Instances ()

import ATrade.Types
import ATrade.Price as P
import qualified Data.Text as T
import ATrade.Broker.Protocol

import Data.Time.Clock
import Data.Time.Calendar

notTooBig :: (Num a, Ord a) => a -> Bool
notTooBig x = abs x < 100000000

arbitraryTickerId = arbitrary `suchThat` (T.all (/= ':'))

instance Arbitrary Tick where
  arbitrary = Tick <$>
    arbitraryTickerId <*>
    arbitrary <*>
    arbitraryTimestamp <*>
    arbitrary <*>
    arbitrary

arbitraryTimestamp = do
  y <- choose (1970, 2050)
  m <- choose (1, 12)
  d <- choose (1, 31)

  sec <- secondsToDiffTime <$> choose (0, 86399)

  return $ UTCTime (fromGregorian y m d) sec

instance Arbitrary DataType where
  arbitrary = toEnum <$> choose (1, 10)

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
    arbitrary <*>
    arbitrary

instance Arbitrary Notification where
  arbitrary = do
    t <- choose (1, 2) :: Gen Int
    if t == 1
      then do
        oid <- arbitrary
        state <- arbitrary
        return $ OrderNotification oid state
      else TradeNotification <$> arbitrary

instance Arbitrary BrokerServerRequest where
  arbitrary = do
    t <- choose (1, 3) :: Gen Int
    if | t == 1 -> RequestSubmitOrder <$> arbitrary <*> arbitrary <*> arbitrary
       | t == 2 -> RequestCancelOrder <$> arbitrary <*> arbitrary <*> arbitrary
       | t == 3 -> RequestNotifications <$> arbitrary <*> arbitrary

instance Arbitrary BrokerServerResponse where
  arbitrary = do
    t <- choose (1, 4) :: Gen Int
    if | t == 1 -> ResponseOrderSubmitted <$> arbitrary
       | t == 2 -> ResponseOrderCancelled <$> arbitrary
       | t == 3 -> ResponseNotifications <$> arbitrary
       | t == 4 -> ResponseError <$> arbitrary

instance Arbitrary P.Price where
  arbitrary = P.Price <$> (arbitrary `suchThat` (\p -> abs p < 1000000000 * 10000000))

instance Arbitrary Bar where
  arbitrary = Bar <$>
    arbitraryTickerId <*>
    arbitraryTimestamp <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary `suchThat` (> 0)

instance Arbitrary BarTimeframe where
  arbitrary = BarTimeframe <$> (arbitrary `suchThat` (\p -> p > 0 && p < 86400 * 365))

    
