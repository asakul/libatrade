{-# LANGUAGE OverloadedStrings, TypeSynonymInstances, FlexibleInstances #-}
{-# LANGUAGE MultiWayIf #-}

module TestBrokerProtocol (
  properties
) where

import Test.Tasty
import Test.Tasty.SmallCheck as SC
import Test.Tasty.QuickCheck as QC
import Test.Tasty.HUnit
import Test.QuickCheck.Instances hiding (Text)

import ATrade.Broker.Protocol
import ArbitraryInstances

import Data.Aeson
import Data.Decimal
import Data.Scientific

properties = testGroup "Broker.Protocol" [
    testNotificationEncoding
  , testBrokerServerRequestEncoding
  , testBrokerServerResponseEncoding
  ]

testNotificationEncoding = QC.testProperty "Notification encoding"
  (\v -> case (decode . encode $ v :: Maybe Notification) of
    Just s -> s == v
    Nothing -> False)

testBrokerServerRequestEncoding = QC.testProperty "BrokerServerRequest encoding"
  (\v -> case (decode . encode $ v :: Maybe BrokerServerRequest) of
    Just s -> s == v
    Nothing -> False)

testBrokerServerResponseEncoding = QC.testProperty "BrokerServerResponse encoding"
  (\v -> case (decode . encode $ v :: Maybe BrokerServerResponse) of
    Just s -> s == v
    Nothing -> False)

