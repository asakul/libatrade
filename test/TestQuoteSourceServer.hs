{-# LANGUAGE OverloadedStrings #-}

module TestQuoteSourceServer (
  unitTests
) where

import Test.Tasty
import Test.Tasty.HUnit

import ATrade.Types
import qualified Data.ByteString.Lazy as BL
import ATrade.QuoteSource.Server
import Control.Concurrent.BoundedChan
import Control.Exception
import System.ZMQ4
import Data.Time.Clock
import Data.Time.Calendar

unitTests :: TestTree
unitTests = testGroup "QuoteSource.Server" [testStartStop, testTickStream]

testStartStop :: TestTree
testStartStop = testCase "QuoteSource Server starts and stops" $ withContext (\ctx -> do
  chan <- newBoundedChan 1000
  qss <- startQuoteSourceServer chan ctx "inproc://quotesource-server" Nothing
  stopQuoteSourceServer qss)

testTickStream :: TestTree
testTickStream = testCase "QuoteSource Server sends ticks" $ withContext (\ctx -> do
  chan <- newBoundedChan 1000
  bracket (startQuoteSourceServer chan ctx "inproc://quotesource-server" Nothing) stopQuoteSourceServer (\_ ->
    withSocket ctx Sub (\s -> do
      connect s "inproc://quotesource-server"
      subscribe s "FOOBAR"
      let tick = Tick {
          security = "FOOBAR",
          datatype = LastTradePrice,
          timestamp = UTCTime (fromGregorian 2016 9 27) 16000,
          value = 1000,
          volume = 1}
      writeChan chan (QSSTick tick)
      packet <- fmap BL.fromStrict <$> receiveMulti s
      case deserializeTick packet of
        Just recvdTick -> tick @=? recvdTick
        Nothing -> assertFailure "Unable to deserialize tick")))
  
