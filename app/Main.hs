{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Data.Binary.Get qualified as BG
import Data.Bits (Bits (shiftL))
import Data.ByteString.Lazy qualified as BL
import System.Environment (getArgs)

main :: IO ()
main = do
  (dbFilePath : cmd : _) <- getArgs
  case cmd of
    ".dbinfo" -> do
      content <- BL.readFile dbFilePath
      let size = BG.runGet (BG.skip 16 >> BG.getWord16be) content
      print $ "database page size: " <> show size
    _ -> print "Unknown command"
