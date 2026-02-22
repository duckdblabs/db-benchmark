{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

import Control.Exception (SomeException, catch, evaluate)
import Control.Monad (forM_, when)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.Char
import Data.Csv (
    defaultEncodeOptions,
    encode,
    encodeWith,
    record,
    toField,
    toRecord,
 )
import Data.Functor
import Data.List (intercalate)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import DataFrame.Functions ((.=))
import qualified DataFrame.Functions as F
import GHC.Stats (getRTSStats, getRTSStatsEnabled, max_live_bytes)
import Numeric (showFFloat)
import System.Directory (doesFileExist, removeFile)
import System.Environment (getEnv, lookupEnv)
import System.IO (
    BufferMode (..),
    IOMode (..),
    hFlush,
    hPutStr,
    hPutStrLn,
    hSetBuffering,
    stderr,
    stdout,
    withFile,
 )
import System.Posix.Files (fileSize, getFileStatus, removeLink)
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

main :: IO ()
main = do
    hSetBuffering stdout NoBuffering
    putStrLn "# groupby-haskell.hs"

    dataName <- getEnv "SRC_DATANAME"
    machineType <- getEnv "MACHINE_TYPE"
    let srcFile = "./data/" ++ dataName ++ ".csv"

    -- Check NA Flag
    let parts = T.splitOn "_" (T.pack dataName)
    let naFlag = if length parts > 3 then read (T.unpack $ parts !! 3) :: Int else 0

    if naFlag > 0
        then hPutStrLn stderr "skip due to na_flag>0"
        else runBenchmark srcFile dataName machineType

runBenchmark :: String -> String -> String -> IO ()
runBenchmark srcFile dataName machineType = do
    putStrLn $ "loading dataset " ++ dataName

    df <- D.readCsv srcFile
    let (inRows, _) = D.dimensions df
    print $ D.summarize df

    -- Columns
    let id1 = F.col @Text "id1"
        id2 = F.col @Text "id2"
        id3 = F.col @Text "id3"
        id4 = F.col @Int "id4"
        id5 = F.col @Int "id5"
        id6 = F.col @Int "id6"
        v1 = F.col @Int "v1"
        v2 = F.col @Int "v2"
        v3 = F.col @Double "v3"

    let config =
            BenchConfig
                { cfgTask = "groupby"
                , cfgDataName = dataName
                , cfgMachineType = machineType
                , cfgSolution = "haskell"
                , cfgVer = "0.4.1"
                , cfgGit = "NA"
                , cfgFun = "groupBy"
                , cfgCache = "TRUE"
                , cfgOnDisk = "FALSE"
                , cfgInRows = inRows
                }

    writeToLogFile config "start"
    putStrLn "grouping..."

    -- Q1: Sum v1 by id1
    runQuestion
        config
        df
        "sum v1 by id1"
        (D.groupBy [F.name id1])
        (D.aggregate [F.sum v1 `F.as` "v1_sum"])
        (\res -> [chkSumInt "v1_sum" res])

    -- Q2: Sum v1 by id1:id2
    runQuestion
        config
        df
        "sum v1 by id1:id2"
        (D.groupBy [F.name id1, F.name id2])
        (D.aggregate ["v1_sum" .= F.sum v1])
        (\res -> [chkSumInt "v1_sum" res])

    -- Q3: Sum v1, Mean v3 by id3
    runQuestion
        config
        df
        "sum v1 mean v3 by id3"
        (D.groupBy [F.name id3])
        ( D.aggregate
            [ "v1_sum" .= F.sum v1
            , "v3_mean" .= F.mean v3
            ]
        )
        (\res -> [chkSumInt "v1_sum" res, chkSumDbl "v3_mean" res])

    -- Q4: Mean v1, v2, v3 by id4
    runQuestion
        config
        df
        "mean v1:v3 by id4"
        (D.groupBy [F.name id4])
        ( D.aggregate
            [ "v1_mean" .= F.mean v1
            , "v2_mean" .= F.mean v2
            , "v3_mean" .= F.mean v3
            ]
        )
        ( \res -> [chkSumDbl "v1_mean" res, chkSumDbl "v2_mean" res, chkSumDbl "v3_mean" res]
        )

    -- Q5 (Question 6 in original): Sum v1, v2, v3 by id6
    runQuestion
        config
        df
        "sum v1:v3 by id6"
        (D.groupBy [F.name id6])
        ( D.aggregate
            [ "v1_sum" .= F.sum v1
            , "v2_sum" .= F.sum v2
            , "v3_sum" .= F.sum v3
            ]
        )
        (\res -> [chkSumInt "v1_sum" res, chkSumInt "v2_sum" res, chkSumDbl "v3_sum" res])

    -- Q6: median v3, sd v3 by id4, id5
    runQuestion
        config
        df
        "median v3 sd v3 by id4 id5"
        (D.groupBy [F.name id4, F.name id5])
        ( D.aggregate
            [ "v3_median" .= F.median v3
            , "v3_sd" .= F.stddev v3
            ]
        )
        (\res -> [chkSumDbl "v3_median" res, chkSumDbl "v3_sd" res])

    -- Q7: max v1 - min v2 by id3
    runQuestion
        config
        df
        "max v1 - min v2 by id3"
        (D.groupBy [F.name id3])
        ( D.aggregate
            [ "diff" .= F.maximum v1 - F.minimum v2
            ]
        )
        (\res -> [chkSumInt "diff" res])

    -- Q10: sum v3 count by id1:id6
    runQuestion
        config
        df
        "sum v3 count by id1:id6"
        (D.groupBy (zipWith (\i n -> i <> (T.pack . show) n) (cycle ["id"]) [1 .. 6]))
        (D.aggregate [F.sum v3 `F.as` "v3_sum"])
        (\res -> [chkSumDbl "v3_sum" res])

    writeToLogFile config "finish"
    putStrLn "Haskell dataframe groupby benchmark completed!"

runQuestion ::
    BenchConfig ->
    D.DataFrame ->
    -- | Question label
    String ->
    -- | Grouping logic
    (D.DataFrame -> D.GroupedDataFrame) ->
    -- | Aggregation logic
    (D.GroupedDataFrame -> D.DataFrame) ->
    -- | Checksum extractor
    (D.DataFrame -> [Double]) ->
    IO ()
runQuestion cfg inputDF qLabel groupFn aggFn chkFn = do
    forM_ [1, 2] $ \runNum -> do
        (resultDF, calcTime) <- timeIt $ do
            let grouped = groupFn inputDF
            let aggregated = aggFn grouped
            print aggregated
            return aggregated

        memUsage <- getMemoryUsage

        let (outRows, outCols) = D.dimensions resultDF

        (chkValues, chkTime) <- timeIt $ do
            let vals = chkFn resultDF
            print vals
            return vals

        writeLog cfg qLabel outRows outCols runNum calcTime memUsage chkValues chkTime

    putStrLn $ qLabel ++ " completed."

chkSumInt :: String -> D.DataFrame -> Double
chkSumInt col df =
    case D.columnAsIntVector (F.col @Int (T.pack col)) df of
        Right vec -> fromIntegral $ VU.sum vec
        Left _ -> 0.0

chkSumDbl :: String -> D.DataFrame -> Double
chkSumDbl col df =
    case D.columnAsDoubleVector (F.col @Double (T.pack col)) df of
        Right vec -> VU.sum vec
        Left _ -> 0.0

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
                , show timestamp
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

    let csvData =
            if append
                then encodeWith defaultEncodeOptions [logRow]
                else encodeWith defaultEncodeOptions [logHeader, logRow]

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

    let csvData =
            if append
                then encodeWith defaultEncodeOptions [logFileRow]
                else encodeWith defaultEncodeOptions [logFileHeader, logFileRow]

    if append
        then BL.appendFile logFile csvData
        else BL.writeFile logFile csvData

roundTo :: Int -> Double -> Double
roundTo n x = fromInteger (round $ x * 10 ^ n) / 10.0 ^^ n
