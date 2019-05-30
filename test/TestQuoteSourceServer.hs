{-# LANGUAGE OverloadedStrings #-}

module TestQuoteSourceServer (
  unitTests
) where

import           Test.Tasty
import           Test.Tasty.HUnit

import           ATrade.QuoteSource.Server
import           ATrade.Types
import           Control.Concurrent.BoundedChan
import           Control.Exception
import qualified Data.ByteString.Lazy           as BL
import           Data.Time.Calendar
import           Data.Time.Clock
import           System.ZMQ4

unitTests :: TestTree
unitTests = testGroup "QuoteSource.Server" [
    testStartStop
  , testTickStream
  , testBarStream ]

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
        Nothing        -> assertFailure "Unable to deserialize tick")))


testBarStream :: TestTree
testBarStream = testCase "QuoteSource Server sends bars" $ withContext (\ctx -> do
  chan <- newBoundedChan 1000
  bracket (startQuoteSourceServer chan ctx "inproc://quotesource-server" Nothing) stopQuoteSourceServer (\_ ->
    withSocket ctx Sub (\s -> do
      connect s "inproc://quotesource-server"
      subscribe s "FOOBAR"
      let bar = Bar {
          barSecurity = "FOOBAR",
          barTimestamp = UTCTime (fromGregorian 2016 9 27) 16000,
          barOpen = fromDouble 10.0,
          barHigh = fromDouble 15.0,
          barLow = fromDouble 8.0,
          barClose = fromDouble 11.0,
          barVolume = 1 }
      writeChan chan (QSSBar (BarTimeframe 60, bar))
      packet <- fmap BL.fromStrict <$> receiveMulti s
      case deserializeBar packet of
        Just (barTf, recvdBar) -> (bar @=? recvdBar) >> (barTf @=? (BarTimeframe 60))
        Nothing       -> assertFailure "Unable to deserialize bar")))
