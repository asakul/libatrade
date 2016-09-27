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

import ATrade.Types
import ATrade.Broker.Protocol

properties = testGroup "Broker.Protocol" []

