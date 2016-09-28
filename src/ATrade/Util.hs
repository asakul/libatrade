
module ATrade.Util (
  atomicMapIORef
) where

import Data.IORef

atomicMapIORef :: IORef a -> (a -> a) -> IO ()
atomicMapIORef ioref f = atomicModifyIORef' ioref (\s -> (f s, ()))
