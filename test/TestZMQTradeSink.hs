{-# LANGUAGE OverloadedStrings #-}

module TestZMQTradeSink (
  unitTests
) where

import Test.Tasty
import Test.Tasty.HUnit

import ATrade.Types
import ATrade.Broker.Protocol
import ATrade.Broker.TradeSinks.ZMQTradeSink
import System.ZMQ4
import Data.Aeson
import Data.Time.Calendar
import Data.Time.Clock
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL

unitTests :: TestTree
unitTests = testGroup "Broker.Server.TradeSinks.ZMQTradeSink" [ testZMQTradeSink ]

testZMQTradeSink :: TestTree
testZMQTradeSink = testCase "Test ZMQTradeSink trade serialization" $
  withContext (\ctx -> withSocket ctx Rep (\insock -> do
    bind insock "inproc://test-sink"
    withZMQTradeSink ctx "inproc://test-sink" (\f -> do
      f trade
      raw <- receive insock
      send insock [] B.empty
      case decode $ BL.fromStrict raw of
        Just t -> mkTradeMessage trade @?= t
        Nothing -> assertFailure "Unable to decode incoming message")))
  where
    trade = Trade {
      tradeOrderId = 0,
      tradePrice = 10,
      tradeQuantity = 20,
      tradeVolume = 30,
      tradeVolumeCurrency = "TEST",
      tradeOperation = Buy,
      tradeAccount = "FOO",
      tradeSecurity = "BAR",
      tradeTimestamp = UTCTime (fromGregorian 1970 1 1) 0,
      tradeCommission = 0,
      tradeSignalId = SignalId "foo" "bar" "" }

