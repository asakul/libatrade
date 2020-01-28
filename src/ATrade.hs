{-# LANGUAGE TemplateHaskell #-}

module ATrade
(
  libatrade_version
) where
import           Data.Version
import           Paths_libatrade

import           Development.GitRev

libatrade_version :: Version
libatrade_version = version

libatrade_gitrev :: String
libatrade_gitrev = concat [ "libatrade-",
                            $(gitBranch),
                            "@",
                            $(gitHash),
                            dirty ]
  where
    dirty | $(gitDirty) = "+"
          | otherwise   = ""


