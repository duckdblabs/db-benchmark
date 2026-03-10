{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module BenchmarkCommon (
    BenchConfig (..),
    getMemoryUsage,
    timeIt,
    writeLog,
    writeToLogFile,
    roundTo,
) where

import Control.Exception (SomeException, catch, evaluate)
import Control.Monad (when)
import qualified Data.ByteString.Lazy as BL
import Data.Char (toLower)
import Data.Csv (defaultEncodeOptions, encUseCrLf, encodeWith)
import Data.Functor ((<&>))
import Data.List (intercalate)
import Data.Maybe (fromMaybe)
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.Vector as V
import GHC.Stats (getRTSStats, getRTSStatsEnabled, max_live_bytes)
import Numeric (showFFloat)
import System.Directory (doesFileExist, removeFile)
import System.Environment (lookupEnv)
import System.Posix.Files (fileSize, getFileStatus)
import System.Posix.Process (getProcessID)
import System.Process (readProcess)
import Text.Read (readMaybe)

-- | Configuration context for the benchmark.
data BenchConfig = BenchConfig
    { cfgTask :: String
    , cfgDataName :: String
    , cfgMachineType :: String
    , cfgSolution :: String
    , cfgVer :: String
    , cfgGit :: String
    , cfgFun :: String
    , cfgCache :: String
    , cfgOnDisk :: String
    , cfgInRows :: Int
    }

getMemoryUsage :: IO Double
getMemoryUsage = do
    isStats <- getRTSStatsEnabled
    if isStats
        then do
            s <- getRTSStats
            return $ fromIntegral (max_live_bytes s) / (1024 * 1024)
        else do
            pid <- getProcessID
            raw <- readProcess "ps" ["-p", show pid, "-o", "rss="] ""
            case readMaybe @Double (filter (/= '\n') raw) of
                Just kb -> return (kb / 1024)
                Nothing -> return 0.0

timeIt :: IO a -> IO (a, Double)
timeIt action = do
    start <- getPOSIXTime
    result <- action
    _ <- evaluate result
    end <- getPOSIXTime
    return (result, realToFrac (end - start))

writeLog ::
    BenchConfig ->
    String ->
    Int ->
    Int ->
    Int ->
    Double ->
    Double ->
    [Double] ->
    Double ->
    IO ()
writeLog BenchConfig{..} question outRows outCols run timeSec memGb chkValues chkTime = do
    batch <- lookupEnv "BATCH" <&> fromMaybe ""
    timestamp <- getPOSIXTime
    csvFile <- lookupEnv "CSV_TIME_FILE" <&> fromMaybe "time.csv"
    csvVerbose <- lookupEnv "CSV_VERBOSE" <&> fromMaybe "false"
    nodename <- fmap init (readProcess "hostname" [] "")

    let formatNum x
            | isNaN x = ""
            | otherwise = show (roundTo 3 x)

    let chkStr =
            intercalate ";" $
                map (map (\c -> if c == ',' then '_' else c) . formatNum) chkValues

    let logRow =
            V.fromList
                [ nodename
                , batch
                , showFFloat (Just 5) (realToFrac timestamp) ""
                , cfgTask
                , cfgDataName
                , show cfgInRows
                , question
                , show outRows
                , show outCols
                , cfgSolution
                , cfgVer
                , cfgGit
                , cfgFun
                , show run
                , formatNum timeSec
                , formatNum memGb
                , cfgCache
                , chkStr
                , formatNum chkTime
                , ""
                , cfgOnDisk
                , cfgMachineType
                ]

    let logHeader =
            V.fromList
                [ "nodename"
                , "batch"
                , "timestamp"
                , "task"
                , "data"
                , "in_rows"
                , "question"
                , "out_rows"
                , "out_cols"
                , "solution"
                , "version"
                , "git"
                , "fun"
                , "run"
                , "time_sec"
                , "mem_gb"
                , "cache"
                , "chk"
                , "chk_time_sec"
                , "comment"
                , "on_disk"
                , "machine_type"
                ]

    -- Remove empty file
    catch
        ( do
            exists <- doesFileExist csvFile
            when exists $ do
                size <- fileSize <$> getFileStatus csvFile
                when (size == 0) $ removeFile csvFile
        )
        (\(_ :: SomeException) -> return ())

    append <- doesFileExist csvFile

    when (map toLower csvVerbose == "true") $
        putStrLn $
            "# " ++ intercalate "," (V.toList logRow)

    let csvEncodeOptions = defaultEncodeOptions{encUseCrLf = False}
    let csvData =
            if append
                then encodeWith csvEncodeOptions [logRow]
                else encodeWith csvEncodeOptions [logHeader, logRow]

    if append
        then BL.appendFile csvFile csvData
        else BL.writeFile csvFile csvData

    return ()

writeToLogFile :: BenchConfig -> String -> IO ()
writeToLogFile BenchConfig{..} action = do
    logFile <- lookupEnv "CSV_LOG_FILE" <&> fromMaybe "logs.csv"
    batch <- lookupEnv "BATCH" <&> fromMaybe ""
    timestamp <- getPOSIXTime
    nodename <- fmap init (readProcess "hostname" [] "")
    let logFileHeader =
            V.fromList
                [ "nodename"
                , "batch"
                , "solution"
                , "version"
                , "git"
                , "task"
                , "data"
                , "timestamp"
                , "action"
                , "stderr"
                , "ret"
                , "machine_type"
                ]
    let logFileRow =
            V.fromList
                [ nodename
                , batch
                , cfgSolution
                , cfgVer
                , cfgGit
                , cfgTask
                , cfgDataName
                , showFFloat (Just 5) (realToFrac timestamp) ""
                , action
                , if action == "finish" then "0" else ""
                , if action == "finish" then "0" else ""
                , cfgMachineType
                ]
    append <- doesFileExist logFile

    let csvEncodeOptions = defaultEncodeOptions{encUseCrLf = False}
    let csvData =
            if append
                then encodeWith csvEncodeOptions [logFileRow]
                else encodeWith csvEncodeOptions [logFileHeader, logFileRow]

    if append
        then BL.appendFile logFile csvData
        else BL.writeFile logFile csvData

roundTo :: Int -> Double -> Double
roundTo n x = fromInteger (round $ x * 10 ^ n) / 10.0 ^^ n
