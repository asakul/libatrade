{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module ATrade.Logging
  (
    Severity(..),
    Message(..),
    fmtMessage,
    showSeverity,
    log,
    logTrace,
    logDebug,
    logInfo,
    logWarning,
    logError,
    logWith,
    logTraceWith,
    logDebugWith,
    logInfoWith,
    logWarningWith,
    logErrorWith
  ) where

import           Colog                    (LogAction (unLogAction), WithLog,
                                           logMsg)
import           Control.Monad.IO.Class   (MonadIO (liftIO))
import qualified Data.Text                as T
import           Data.Time                (UTCTime, defaultTimeLocale,
                                           formatTime, getCurrentTime)
import           Data.Time.Format.ISO8601 (iso8601Show)
import           Prelude                  hiding (log)
import           System.Console.ANSI      (Color (Cyan, Green, Red, White, Yellow),
                                           ColorIntensity (Dull, Vivid),
                                           ConsoleLayer (Foreground),
                                           SGR (Reset, SetColor), setSGRCode)


data Severity =
    Trace
  | Debug
  | Info
  | Warning
  | Error
  deriving (Show, Eq, Ord)

data Message =
  Message
  {
    msgTimestamp :: UTCTime,
    msgComponent :: T.Text,
    msgSeverity  :: Severity,
    msgText      :: T.Text
  } deriving (Show, Eq)

fmtMessage :: Message -> T.Text
fmtMessage Message{..} =
  (bracketed . T.pack . formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S") msgTimestamp <>
  showSeverity msgSeverity <>
  bracketed msgComponent <>
  " " <>
  msgText
  where
    bracketed txt = "[" <> txt <> "]"

showSeverity :: Severity -> T.Text
showSeverity = \case
  Trace -> color White Dull "[Trace  ]"
  Debug -> color Green Dull "[Debug  ]"
  Info -> color Green Vivid "[Info   ]"
  Warning -> color Yellow Vivid "[Warning]"
  Error -> color Red Vivid "[Error  ]"
  where
    color c h txt = T.pack (setSGRCode [SetColor Foreground h c])
        <> txt
        <> T.pack (setSGRCode [Reset])

logWith :: (MonadIO m) => LogAction m Message -> Severity -> T.Text -> T.Text -> m ()
logWith act sev comp txt = do
  now <- liftIO getCurrentTime
  unLogAction act $ Message now comp sev txt

logTraceWith :: (MonadIO m) => LogAction m Message -> T.Text -> T.Text -> m ()
logTraceWith act = logWith act Trace

logDebugWith :: (MonadIO m) => LogAction m Message -> T.Text -> T.Text -> m ()
logDebugWith act = logWith act Debug

logInfoWith :: (MonadIO m) => LogAction m Message -> T.Text -> T.Text -> m ()
logInfoWith act = logWith act Info

logWarningWith :: (MonadIO m) => LogAction m Message -> T.Text -> T.Text -> m ()
logWarningWith act = logWith act Warning

logErrorWith :: (MonadIO m) => LogAction m Message -> T.Text -> T.Text -> m ()
logErrorWith act = logWith act Error

log :: (MonadIO m, WithLog env Message m) => Severity -> T.Text -> T.Text -> m ()
log sev comp txt = do
  now <- liftIO getCurrentTime
  logMsg $ Message now comp sev txt

logTrace :: (MonadIO m, WithLog env Message m) => T.Text -> T.Text -> m ()
logTrace = log Trace

logDebug :: (MonadIO m, WithLog env Message m) => T.Text -> T.Text -> m ()
logDebug = log Debug

logInfo :: (MonadIO m, WithLog env Message m) => T.Text -> T.Text -> m ()
logInfo = log Info

logWarning :: (MonadIO m, WithLog env Message m) => T.Text -> T.Text -> m ()
logWarning = log Warning

logError :: (MonadIO m, WithLog env Message m) => T.Text -> T.Text -> m ()
logError = log Error
