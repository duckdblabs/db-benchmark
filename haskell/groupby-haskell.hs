{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Control.Exception (evaluate)
import Control.Monad (forM_, when)
import Data.List (intercalate)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import GHC.Stats (getRTSStats, max_live_bytes, getRTSStatsEnabled)
import System.Directory (doesFileExist)
import System.Environment (getEnv, lookupEnv)
import System.IO (hFlush, hPutStrLn, stderr, stdout, withFile, IOMode(..), hSetBuffering, BufferMode(..), hPutStr)
import System.Posix.Process (getProcessID)
import System.Process (readProcess)
import Text.Read (readMaybe)

-- | Configuration context for the benchmark.
data BenchConfig = BenchConfig
    { cfgTask        :: String
    , cfgDataName    :: String
    , cfgMachineType :: String
    , cfgSolution    :: String
    , cfgVer         :: String
    , cfgGit         :: String
    , cfgFun         :: String
    , cfgCache       :: String
    , cfgOnDisk      :: String
    , cfgInRows      :: Int
    }

main :: IO ()
main = do
    hSetBuffering stdout NoBuffering
    putStrLn "# groupby-haskell.hs"

    dataName    <- getEnv "SRC_DATANAME"
    machineType <- getEnv "MACHINE_TYPE"
    let srcFile = "../data/" ++ dataName ++ ".csv"

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
    print inRows

    let config = BenchConfig 
            { cfgTask = "groupby"
            , cfgDataName = dataName
            , cfgMachineType = machineType
            , cfgSolution = "haskell"
            , cfgVer = "0.3.3"
            , cfgGit = "dataframe"
            , cfgFun = "groupBy"
            , cfgCache = "TRUE"
            , cfgOnDisk = "FALSE"
            , cfgInRows = inRows
            }

    putStrLn "grouping..."
    
    -- Q1: Sum v1 by id1
    runQuestion config df "sum v1 by id1" 
        (\d -> D.groupBy ["id1"] d)
        (\g -> D.aggregate [F.sum (F.col @Int "v1") `F.as` "v1_sum"] g)
        (\res -> [chkSumInt "v1_sum" res])

    -- Q2: Sum v1 by id1:id2
    runQuestion config df "sum v1 by id1:id2"
        (\d -> D.groupBy ["id1", "id2"] d)
        (\g -> D.aggregate [F.sum (F.col @Int "v1") `F.as` "v1_sum"] g)
        (\res -> [chkSumInt "v1_sum" res])

    -- Q3: Sum v1, Mean v3 by id3
    runQuestion config df "sum v1 mean v3 by id3"
        (\d -> D.groupBy ["id3"] d)
        (\g -> D.aggregate 
                [ F.sum (F.col @Int "v1")   `F.as` "v1_sum"
                , F.mean (F.col @Double "v3") `F.as` "v3_mean"
                ] g)
        (\res -> [chkSumInt "v1_sum" res, chkSumDbl "v3_mean" res])

    -- Q4: Mean v1, v2, v3 by id4
    runQuestion config df "mean v1:v3 by id4"
        (\d -> D.groupBy ["id4"] d)
        (\g -> D.aggregate
                [ F.mean (F.col @Int "v1")    `F.as` "v1_mean"
                , F.mean (F.col @Int "v2")    `F.as` "v2_mean"
                , F.mean (F.col @Double "v3") `F.as` "v3_mean"
                ] g)
        (\res -> [chkSumDbl "v1_mean" res, chkSumDbl "v2_mean" res, chkSumDbl "v3_mean" res])

    -- Q5 (Question 6 in original): Sum v1, v2, v3 by id6
    runQuestion config df "sum v1:v3 by id6"
        (\d -> D.groupBy ["id6"] d)
        (\g -> D.aggregate
                [ F.sum (F.col @Int "v1")    `F.as` "v1_sum"
                , F.sum (F.col @Int "v2")    `F.as` "v2_sum"
                , F.sum (F.col @Double "v3") `F.as` "v3_sum"
                ] g)
        (\res -> [chkSumInt "v1_sum" res, chkSumInt "v2_sum" res, chkSumDbl "v3_sum" res])

    -- Q6: median v3, sd v3 by id4, id5
    runQuestion config df "median v3 sd v3 by id4 id5"
        (\d -> D.groupBy ["id4", "id5"] d)
        (\g -> D.aggregate
                [ F.median (F.col @Double "v3") `F.as` "v3_median"
                , F.stddev (F.col @Double "v3") `F.as` "v3_sd"
                ] g)
        (\res -> [chkSumDbl "v3_median" res, chkSumDbl "v3_sd" res])

    -- Q7: max v1 - min v2 by id3
    runQuestion config df "max v1 - min v2 by id3"
        (\d -> D.groupBy ["id3"] d)
        (\g -> D.aggregate
                [ (F.maximum (F.col @Int "v1") - F.minimum (F.col @Int "v2")) `F.as` "diff" 
                ] g)
        (\res -> [chkSumInt "diff" res])

    -- Q10: sum v3 count by id1:id6
    runQuestion config df "sum v3 count by id1:id6"
        (\d -> D.groupBy (zipWith (\i n -> i <> (T.pack . show) n) (cycle ["id"]) [1 .. 6]) d)
        (\g -> D.aggregate [F.sum (F.col @Double "v3") `F.as` "v3_sum"] g)
        (\res -> [chkSumDbl "v3_sum" res])

    putStrLn "Haskell dataframe groupby benchmark completed!"

runQuestion :: BenchConfig 
            -> D.DataFrame 
            -> String                               -- ^ Question label
            -> (D.DataFrame -> D.GroupedDataFrame)  -- ^ Grouping logic
            -> (D.GroupedDataFrame -> D.DataFrame)  -- ^ Aggregation logic
            -> (D.DataFrame -> [Double])            -- ^ Checksum extractor
            -> IO ()
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
    case D.columnAsIntVector (T.pack col) df of
        Right vec -> fromIntegral $ VU.sum vec
        Left _    -> 0.0

chkSumDbl :: String -> D.DataFrame -> Double
chkSumDbl col df = 
    case D.columnAsDoubleVector (T.pack col) df of
        Right vec -> VU.sum vec
        Left _    -> 0.0

getMemoryUsage :: IO Double
getMemoryUsage = do
    isStats <- getRTSStatsEnabled
    if isStats then do
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

writeLog :: BenchConfig -> String -> Int -> Int -> Int -> Double -> Double -> [Double] -> Double -> IO ()
writeLog BenchConfig{..} question outRows outCols run timeSec memGb chkValues chkTime = do
    batch <- lookupEnv "BATCH" >>= return . fromMaybe ""
    timestamp <- getPOSIXTime
    csvFile <- lookupEnv "CSV_TIME_FILE" >>= return . fromMaybe "time.csv"
    nodename <- fmap init (readProcess "hostname" [] "")

    let formatNum x = show (roundTo 3 x)
    let chkStr = intercalate ";" $ map (map (\c -> if c == ',' then '_' else c) . formatNum) chkValues

    let logRow = intercalate ","
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
            , "" -- comment
            , cfgOnDisk
            , cfgMachineType
            ]

    exists <- doesFileExist csvFile
    let header = "nodename,batch,timestamp,task,data,in_rows,question,out_rows,out_cols,solution,version,git,fun,run,time_sec,mem_gb,cache,chk,chk_time_sec,comment,on_disk,machine_type\n"
    
    forceAppend csvFile $ (if not exists then header else "") ++ logRow ++ "\n"

roundTo :: Int -> Double -> Double
roundTo n x = (fromInteger $ round $ x * (10 ^ n)) / (10.0 ^^ n)

forceAppend :: FilePath -> String -> IO ()
forceAppend path content = 
    withFile path AppendMode $ \h -> do
        hSetBuffering h NoBuffering
        hPutStr h content
        hFlush h
