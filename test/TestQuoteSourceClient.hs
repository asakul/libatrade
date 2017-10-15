{-# LANGUAGE OverloadedStrings #-}

module TestQuoteSourceClient (
  unitTests
) where

import Test.Tasty
import Test.Tasty.SmallCheck as SC
import Test.Tasty.QuickCheck as QC
import Test.Tasty.HUnit

import ATrade.Types
import ATrade.QuoteSource.Server
import ATrade.QuoteSource.Client
import Control.Monad
import Control.Monad.Loops
import Control.Concurrent.MVar
import Control.Concurrent.BoundedChan
import Control.Concurrent hiding (writeChan, readChan)
import Control.Exception
import System.ZMQ4
import Data.Time.Clock
import Data.Time.Calendar
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import Data.Maybe
import Data.UUID as U
import Data.UUID.V4 as UV4

makeEndpoint = do
  uid <- toText <$> UV4.nextRandom
  return $ "inproc://server" `T.append` uid

unitTests = testGroup "QuoteSource.Client" [testStartStop, testTickStream]

testStartStop = testCase "QuoteSource client connects and disconnects" $ withContext (\ctx -> do
  ep <- makeEndpoint
  chan <- newBoundedChan 1000
  clientChan <- newBoundedChan 1000
  bracket (startQuoteSourceServer chan ctx ep) stopQuoteSourceServer (\qs ->
    bracket (startQuoteSourceClient clientChan [] ctx ep) stopQuoteSourceClient (const yield)))

testTickStream = testCase "QuoteSource clients receives ticks" $ withContext (\ctx -> do
  ep <- makeEndpoint
  chan <- newBoundedChan 1000
  clientChan <- newBoundedChan 1000
  bracket (startQuoteSourceServer chan ctx ep) stopQuoteSourceServer (\qs ->
    bracket (startQuoteSourceClient clientChan ["FOOBAR"] ctx ep) stopQuoteSourceClient (\qc -> do
      let tick = Tick {
          security = "FOOBAR",
          datatype = LastTradePrice,
          timestamp = UTCTime (fromGregorian 2016 9 27) 16000,
          value = 1000,
          volume = 1}
      forkIO $ forever $ writeChan chan (QSSTick tick)
      recvdTick <- readChan clientChan
      tick @=? recvdTick)))
  
