{-# LANGUAGE OverloadedStrings #-}

module TestQuoteSourceClient (
  unitTests
) where

import           Test.Tasty
import           Test.Tasty.HUnit

import           ATrade.QuoteSource.Client
import           ATrade.QuoteSource.Server
import           ATrade.Types
import           Control.Concurrent             hiding (readChan, writeChan)
import           Control.Concurrent.BoundedChan
import           Control.Exception
import           Control.Monad
import qualified Data.Text                      as T
import           Data.Time.Calendar
import           Data.Time.Clock
import           Data.UUID                      as U
import           Data.UUID.V4                   as UV4
import           System.ZMQ4
import           System.ZMQ4.ZAP

makeEndpoint :: IO T.Text
makeEndpoint = do
  uid <- toText <$> UV4.nextRandom
  return $ "inproc://server" `T.append` uid

unitTests :: TestTree
unitTests = testGroup "QuoteSource.Client" [
    testStartStop
  , testTickStream
  , testBarStream
  , testDynamicSubscription ]

testStartStop :: TestTree
testStartStop = testCase "QuoteSource client connects and disconnects" $ withContext (\ctx -> do
  ep <- makeEndpoint
  chan <- newBoundedChan 1000
  clientChan <- newBoundedChan 1000
  bracket (startQuoteSourceServer chan ctx ep defaultServerSecurityParams) stopQuoteSourceServer (\_ ->
    bracket (startQuoteSourceClient clientChan [] ctx ep defaultClientSecurityParams) stopQuoteSourceClient (const yield)))

testTickStream :: TestTree
testTickStream = testCase "QuoteSource clients receives ticks" $ withContext (\ctx -> do
  ep <- makeEndpoint
  chan <- newBoundedChan 1000
  clientChan <- newBoundedChan 1000
  bracket (startQuoteSourceServer chan ctx ep defaultServerSecurityParams) stopQuoteSourceServer (\_ -> do
    threadDelay 20000
    bracket (startQuoteSourceClient clientChan ["FOOBAR"] ctx ep defaultClientSecurityParams) stopQuoteSourceClient (\_ -> do
      let tick = Tick {
          security = "FOOBAR",
          datatype = LastTradePrice,
          timestamp = UTCTime (fromGregorian 2016 9 27) 16000,
          value = 1000,
          volume = 1}
      forkIO $ forever $ writeChan chan (QSSTick tick)
      recvdData <- readChan clientChan
      QDTick tick @=? recvdData)))

testBarStream :: TestTree
testBarStream = testCase "QuoteSource clients receives bars" $ withContext (\ctx -> do
  ep <- makeEndpoint
  chan <- newBoundedChan 1000
  clientChan <- newBoundedChan 1000
  bracket (startQuoteSourceServer chan ctx ep defaultServerSecurityParams) stopQuoteSourceServer (\_ -> do
    threadDelay 20000
    bracket (startQuoteSourceClient clientChan ["FOOBAR"] ctx ep defaultClientSecurityParams) stopQuoteSourceClient (\_ -> do
      let bar = Bar {
          barSecurity = "FOOBAR",
          barTimestamp = UTCTime (fromGregorian 2016 9 27) 16000,
          barOpen = fromDouble 10.0,
          barHigh = fromDouble 15.0,
          barLow = fromDouble 8.0,
          barClose = fromDouble 11.0,
          barVolume = 42 }
      forkIO $ forever $ writeChan chan $ QSSBar (BarTimeframe 60, bar)
      recvdData <- readChan clientChan
      QDBar (BarTimeframe 60, bar) @=? recvdData)))

testDynamicSubscription :: TestTree
testDynamicSubscription = testCase "QuoteSource clients can subscribe dynamically" $ withContext (\ctx -> do
  ep <- makeEndpoint
  chan <- newBoundedChan 1000
  clientChan <- newBoundedChan 1000
  bracket (startQuoteSourceServer chan ctx ep defaultServerSecurityParams) stopQuoteSourceServer (\_ ->
    bracket (startQuoteSourceClient clientChan [] ctx ep defaultClientSecurityParams) stopQuoteSourceClient (\client -> do
      quoteSourceClientSubscribe client [("FOOBAR", BarTimeframe 60)]
      let bar = Bar {
          barSecurity = "FOOBAR",
          barTimestamp = UTCTime (fromGregorian 2021 11 18) 16000,
          barOpen = fromDouble 10.0,
          barHigh = fromDouble 15.0,
          barLow = fromDouble 8.0,
          barClose = fromDouble 11.0,
          barVolume = 42 }
      threadDelay 200000
      forkIO $ forever $ writeChan chan $ QSSBar (BarTimeframe 60, bar)
      recvdData <- readChan clientChan
      QDBar (BarTimeframe 60, bar) @=? recvdData)))
