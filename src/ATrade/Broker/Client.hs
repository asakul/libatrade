{-# LANGUAGE OverloadedStrings #-}

module ATrade.Broker.Client (
) where

import ATrade.Types
import ATrade.Broker.Protocol
import Control.Concurrent hiding (readChan, writeChan)
import Control.Concurrent.BoundedChan
import Control.Concurrent.MVar
import Control.Exception
import Data.List.NonEmpty
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as BL
import Data.Text.Encoding
import System.ZMQ4
import System.Log.Logger

data BrokerClientHandle = BrokerClientHandle {
  tid :: ThreadId,
  completionMvar :: compMv,
  submitOrder :: Order -> IO (Either T.Text OrderId),
  cancelOrder :: OrderId -> IO (Either T.Text ()),
  cmdVar :: MVar BrokerServerRequest,
  respVar :: MVar BrokerServerResponse
}

startBrokerClient :: Context -> T.Text -> IO BrokerClientHandle
startBrokerClient ctx endpoint = undefined

stopBrokerClient :: BrokerClientHandle -> IO ()
stopBrokerClient handle = undefined
