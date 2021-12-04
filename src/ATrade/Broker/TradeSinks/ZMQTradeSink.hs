
module ATrade.Broker.TradeSinks.ZMQTradeSink (
  withZMQTradeSink
) where

import Control.Exception
import Control.Concurrent
import qualified Control.Concurrent.BoundedChan as BC
import Data.Aeson
import Data.IORef
import Data.Maybe
import qualified Data.Text as T
import Data.List.NonEmpty
import qualified Data.List as L
import qualified Data.ByteString as B hiding (putStrLn)
import qualified Data.ByteString.Lazy as BL hiding (putStrLn)
import Control.Monad.Loops
import Control.Monad.Extra
import System.Timeout
import System.ZMQ4

import ATrade.Types
import ATrade.Broker.Protocol

withZMQTradeSink ctx tradeSinkEp f = do
  killMv <- newEmptyMVar
  chan <- BC.newBoundedChan 1000
  bracket (forkIO $ sinkThread ctx tradeSinkEp killMv chan) (stopSinkThread killMv) (\_ -> f $ sink chan)
  where
    sink = BC.writeChan

sinkThread ctx tradeSinkEp killMv chan = whileM_ (not <$> wasKilled) $
  handle (\e -> do
    when (isZMQError (e :: SomeException)) $ do
      throwIO e) sinkThread'
  where
    sinkThread' = withSocket ctx Dealer (\sock -> do
      connect sock $ T.unpack tradeSinkEp
      whenM (not <$> wasKilled) $ sinkThread'' sock)

    sinkThread'' sock = do
      maybeTrade <- BC.tryReadChan chan
      case maybeTrade of
        Just trade -> do
          sendMulti sock $ B.empty :| [encodeTrade trade]
          void $ receiveMulti sock
        Nothing -> do
          threadDelay 1000000
          sendMulti sock $ B.empty :| [BL.toStrict $ encode TradeSinkHeartBeat]
          events <- poll 5000 [Sock sock [In] Nothing]
          if L.null . L.head $ events
            then return ()
            else do
              void . receive $ sock -- anything will do
              sinkThread'' sock


    isZMQError e = "ZMQError" `L.isPrefixOf` show e
    wasKilled = isJust <$> tryReadMVar killMv
    encodeTrade :: Trade -> B.ByteString
    encodeTrade = BL.toStrict . encode . convertTrade
    convertTrade trade = TradeSinkTrade {
      tsAccountId = tradeAccount trade,
      tsSecurity = tradeSecurity trade,
      tsPrice = fromRational . toRational . tradePrice $ trade,
      tsQuantity = fromInteger $ tradeQuantity trade,
      tsVolume = fromRational . toRational . tradeVolume $ trade,
      tsCurrency = tradeVolumeCurrency trade,
      tsOperation = tradeOperation trade,
      tsExecutionTime = tradeTimestamp trade,
      tsCommission = toDouble (tradeCommission trade),
      tsSignalId = tradeSignalId trade
    }

stopSinkThread killMv threadId = putMVar killMv () >> killThread threadId
  
