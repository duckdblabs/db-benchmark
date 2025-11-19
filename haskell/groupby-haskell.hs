{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import Control.Exception (evaluate)
import Data.List (intercalate)
import Data.Maybe
import qualified Data.Text as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import GHC.Stats
import System.Directory (doesFileExist)
import System.Environment (getEnv, lookupEnv)
import System.IO (hFlush, hPutStrLn, stderr, stdout)
import System.Posix.Process (getProcessID)
import System.Process (readProcess)
import Text.Read


main :: IO ()
main = do
    putStrLn "# groupby-haskell.hs"
    hFlush stdout

    let ver = "0.3.3"
    let git = "dataframe"
    let task = "groupby"
    let solution = "haskell"
    let fun = "groupBy"
    let cache = "TRUE"
    let onDisk = "FALSE"

    dataName <- getEnv "SRC_DATANAME"
    machineType <- getEnv "MACHINE_TYPE"
    let srcFile = "../data/" ++ dataName ++ ".csv"

    putStrLn $ "loading dataset " ++ dataName
    hFlush stdout

    -- Check if data has NAs
    let parts = T.splitOn "_" (T.pack dataName)
    let naFlag = if length parts > 3 then read (T.unpack $ parts !! 3) :: Int else 0

    if naFlag > 0
        then do
            hPutStrLn stderr "skip due to na_flag>0"
            return ()
        else do
            -- Load CSV data using dataframe
            x <- D.readCsv srcFile

            let (inRows, _) = D.dimensions x
            putStrLn $ show inRows
            hFlush stdout

            putStrLn "grouping..."
            hFlush stdout

            -- Question 1: sum v1 by id1
            let question1 = "sum v1 by id1"
            (ans1, t1_1) <- timeIt $ do
                let grouped = D.groupBy ["id1"] x
                let result = D.aggregate [F.sum (F.col @Int "v1") `F.as` "v1_sum"] grouped
                return result
            m1_1 <- getMemoryUsage
            let (outRows1, outCols1) = D.dimensions ans1
            (chk1, chkt1_1) <- timeIt $ do
                let sumV1 = D.sum (F.col @Int "v1_sum") ans1
                print sumV1
                return sumV1
            writeLog
                task
                dataName
                inRows
                question1
                outRows1
                outCols1
                solution
                ver
                git
                fun
                1
                t1_1
                m1_1
                cache
                (makeChk [fromIntegral chk1])
                chkt1_1
                onDisk
                machineType

            -- Run 2
            (ans1_2, t1_2) <- timeIt $ do
                let grouped = D.groupBy ["id1"] x
                let result = D.aggregate [F.sum (F.col @Int "v1") `F.as` "v1_sum"] grouped
                return result
            m1_2 <- getMemoryUsage
            let (outRows1_2, outCols1_2) = D.dimensions ans1_2
            (chk1_2, chkt1_2) <- timeIt $ do
                let sumV1 = D.sum (F.col @Int "v1_sum") ans1_2
                print sumV1
                return sumV1
            writeLog
                task
                dataName
                inRows
                question1
                outRows1_2
                outCols1_2
                solution
                ver
                git
                fun
                2
                t1_2
                m1_2
                cache
                (makeChk [fromIntegral chk1_2])
                chkt1_2
                onDisk
                machineType
            putStrLn $ "Question 1 completed: " ++ show outRows1_2 ++ " groups"

            -- Question 2: sum v1 by id1:id2
            let question2 = "sum v1 by id1:id2"
            (ans2, t2_1) <- timeIt $ do
                let grouped = D.groupBy ["id1", "id2"] x
                let result = D.aggregate [F.sum (F.col @Int "v1") `F.as` "v1_sum"] grouped
                return result
            m2_1 <- getMemoryUsage
            let (outRows2, outCols2) = D.dimensions ans2
            (chk2, chkt2_1) <- timeIt $ do
                let sumV1 = D.sum (F.col @Int "v1_sum") ans2
                print sumV1
                return sumV1
            writeLog
                task
                dataName
                inRows
                question2
                outRows2
                outCols2
                solution
                ver
                git
                fun
                1
                t2_1
                m2_1
                cache
                (makeChk [fromIntegral chk2])
                chkt2_1
                onDisk
                machineType

            -- Run 2
            (ans2_2, t2_2) <- timeIt $ do
                let grouped = D.groupBy ["id1", "id2"] x
                let result = D.aggregate [F.sum (F.col @Int "v1") `F.as` "v1_sum"] grouped
                return result
            m2_2 <- getMemoryUsage
            let (outRows2_2, outCols2_2) = D.dimensions ans2_2
            (chk2_2, chkt2_2) <- timeIt $ do
                let sumV1 = D.sum (F.col @Int "v1_sum") ans2_2
                print sumV1
                return sumV1
            writeLog
                task
                dataName
                inRows
                question2
                outRows2_2
                outCols2_2
                solution
                ver
                git
                fun
                2
                t2_2
                m2_2
                cache
                (makeChk [fromIntegral chk2_2])
                chkt2_2
                onDisk
                machineType
            putStrLn $ "Question 2 completed: " ++ show outRows2_2 ++ " groups"

            -- Question 3: sum v1 mean v3 by id3
            let question3 = "sum v1 mean v3 by id3"
            (ans3, t3_1) <- timeIt $ do
                let grouped = D.groupBy ["id3"] x
                let result =
                        D.aggregate
                            [ F.sum (F.col @Int "v1") `F.as` "v1_sum"
                            , F.mean (F.col @Double "v3") `F.as` "v3_mean"
                            ]
                            grouped
                return result
            m3_1 <- getMemoryUsage
            let (outRows3, outCols3) = D.dimensions ans3
            (chk3, chkt3_1) <- timeIt $ do
                let sumV1 = D.sum (F.col @Int "v1_sum") ans3
                let sumV3 = D.sum (F.col @Double "v3_mean") ans3
                print (sumV1, sumV3)
                return (sumV1, sumV3)
            writeLog
                task
                dataName
                inRows
                question3
                outRows3
                outCols3
                solution
                ver
                git
                fun
                1
                t3_1
                m3_1
                cache
                (makeChk [fromIntegral (fst chk3), snd chk3])
                chkt3_1
                onDisk
                machineType

            -- Run 2
            (ans3_2, t3_2) <- timeIt $ do
                let grouped = D.groupBy ["id3"] x
                let result =
                        D.aggregate
                            [ F.sum (F.col @Int "v1") `F.as` "v1_sum"
                            , F.mean (F.col @Double "v3") `F.as` "v3_mean"
                            ]
                            grouped
                return result
            m3_2 <- getMemoryUsage
            let (outRows3_2, outCols3_2) = D.dimensions ans3_2
            (chk3_2, chkt3_2) <- timeIt $ do
                let sumV1 = D.sum (F.col @Int "v1_sum") ans3_2
                let sumV3 = D.sum (F.col @Double "v3_mean") ans3_2
                print (sumV1, sumV3)
                return (sumV1, sumV3)
            writeLog
                task
                dataName
                inRows
                question3
                outRows3_2
                outCols3_2
                solution
                ver
                git
                fun
                2
                t3_2
                m3_2
                cache
                (makeChk [fromIntegral (fst chk3_2), snd chk3_2])
                chkt3_2
                onDisk
                machineType
            putStrLn $ "Question 3 completed: " ++ show outRows3_2 ++ " groups"

            -- Question 4: mean v1:v3 by id4
            let question4 = "mean v1:v3 by id4"
            (ans4, t4_1) <- timeIt $ do
                let grouped = D.groupBy ["id4"] x
                let result =
                        D.aggregate
                            [ F.mean (F.col @Int "v1") `F.as` "v1_mean"
                            , F.mean (F.col @Int "v2") `F.as` "v2_mean"
                            , F.mean (F.col @Double "v3") `F.as` "v3_mean"
                            ]
                            grouped
                return result
            m4_1 <- getMemoryUsage
            let (outRows4, outCols4) = D.dimensions ans4
            (chk4, chkt4_1) <- timeIt $ do
                let sumV1 = case D.columnAsDoubleVector "v1_mean" ans4 of
                        Right vec -> VU.sum vec
                        Left _ -> 0
                let sumV2 = case D.columnAsDoubleVector "v2_mean" ans4 of
                        Right vec -> VU.sum vec
                        Left _ -> 0
                let sumV3 = case D.columnAsDoubleVector "v3_mean" ans4 of
                        Right vec -> VU.sum vec
                        Left _ -> 0
                print (sumV1, sumV2, sumV3)
                return (sumV1, sumV2, sumV3)
            writeLog
                task
                dataName
                inRows
                question4
                outRows4
                outCols4
                solution
                ver
                git
                fun
                1
                t4_1
                m4_1
                cache
                (makeChk [(\(a, _, _) -> a) chk4, (\(_, b, _) -> b) chk4, (\(_, _, c) -> c) chk4])
                chkt4_1
                onDisk
                machineType

            -- Run 2
            (ans4_2, t4_2) <- timeIt $ do
                let grouped = D.groupBy ["id4"] x
                let result =
                        D.aggregate
                            [ F.mean (F.col @Int "v1") `F.as` "v1_mean"
                            , F.mean (F.col @Int "v2") `F.as` "v2_mean"
                            , F.mean (F.col @Double "v3") `F.as` "v3_mean"
                            ]
                            grouped
                return result
            m4_2 <- getMemoryUsage
            let (outRows4_2, outCols4_2) = D.dimensions ans4_2
            (chk4_2, chkt4_2) <- timeIt $ do
                let sumV1 = case D.columnAsDoubleVector "v1_mean" ans4_2 of
                        Right vec -> VU.sum vec
                        Left _ -> 0
                let sumV2 = case D.columnAsDoubleVector "v2_mean" ans4_2 of
                        Right vec -> VU.sum vec
                        Left _ -> 0
                let sumV3 = case D.columnAsDoubleVector "v3_mean" ans4_2 of
                        Right vec -> VU.sum vec
                        Left _ -> 0
                print (sumV1, sumV2, sumV3)
                return (sumV1, sumV2, sumV3)
            writeLog
                task
                dataName
                inRows
                question4
                outRows4_2
                outCols4_2
                solution
                ver
                git
                fun
                2
                t4_2
                m4_2
                cache
                ( makeChk
                    [(\(a, _, _) -> a) chk4_2, (\(_, b, _) -> b) chk4_2, (\(_, _, c) -> c) chk4_2]
                )
                chkt4_2
                onDisk
                machineType
            putStrLn $ "Question 4 completed: " ++ show outRows4_2 ++ " groups"

            -- Question 6: sum v1:v3 by id6
            let question5 = "sum v1:v3 by id6"
            (ans5, t5_1) <- timeIt $ do
                let grouped = D.groupBy ["id6"] x
                let result =
                        D.aggregate
                            [ F.sum (F.col @Int "v1") `F.as` "v1_sum"
                            , F.sum (F.col @Int "v2") `F.as` "v2_sum"
                            , F.sum (F.col @Double "v3") `F.as` "v3_sum"
                            ]
                            grouped
                return result
            m5_1 <- getMemoryUsage
            let (outRows5, outCols5) = D.dimensions ans5
            (chk5, chkt5_1) <- timeIt $ do
                let sumV1 = D.sum (F.col @Int "v1_sum") ans5
                let sumV2 = D.sum (F.col @Int "v2_sum") ans5
                let sumV3 = D.sum (F.col @Double "v3_sum") ans5
                print (sumV1, sumV2, sumV3)
                return (sumV1, sumV2, sumV3)
            writeLog
                task
                dataName
                inRows
                question5
                outRows5
                outCols5
                solution
                ver
                git
                fun
                1
                t5_1
                m5_1
                cache
                ( makeChk
                    [ (\(a, _, _) -> fromIntegral a) chk5
                    , (\(_, b, _) -> fromIntegral b) chk5
                    , (\(_, _, c) -> c) chk5
                    ]
                )
                chkt5_1
                onDisk
                machineType

            -- Run 2
            (ans5_2, t5_2) <- timeIt $ do
                let grouped = D.groupBy ["id6"] x
                let result =
                        D.aggregate
                            [ F.sum (F.col @Int "v1") `F.as` "v1_sum"
                            , F.sum (F.col @Int "v2") `F.as` "v2_sum"
                            , F.sum (F.col @Double "v3") `F.as` "v3_sum"
                            ]
                            grouped
                return result
            m5_2 <- getMemoryUsage
            let (outRows5_2, outCols5_2) = D.dimensions ans5_2
            (chk5_2, chkt5_2) <- timeIt $ do
                let sumV1 = D.sum (F.col @Int "v1_sum") ans5_2
                let sumV2 = D.sum (F.col @Int "v2_sum") ans5_2
                let sumV3 = D.sum (F.col @Double "v3_sum") ans5_2
                print (sumV1, sumV2, sumV3)
                return (sumV1, sumV2, sumV3)
            writeLog
                task
                dataName
                inRows
                question5
                outRows5_2
                outCols5_2
                solution
                ver
                git
                fun
                2
                t5_2
                m5_2
                cache
                ( makeChk
                    [ (\(a, _, _) -> fromIntegral a) chk5_2
                    , (\(_, b, _) -> fromIntegral b) chk5_2
                    , (\(_, _, c) -> c) chk5_2
                    ]
                )
                chkt5_2
                onDisk
                machineType
            putStrLn $ "Question 5 completed: " ++ show outRows5_2 ++ " groups"

            -- Question 6: median v3 sd v3 by id4 id5
            let question6 = "median v3 sd v3 by id4 id5"
            (ans6, t6_1) <- timeIt $ do
                let grouped = D.groupBy ["id4", "id5"] x
                let result =
                        D.aggregate
                            [ F.median (F.col @Double "v3") `F.as` "v3_median"
                            , F.stddev (F.col @Double "v3") `F.as` "v3_sd"
                            ]
                            grouped
                return result
            m6_1 <- getMemoryUsage
            let (outRows6, outCols6) = D.dimensions ans6
            (chk6, chkt6_1) <- timeIt $ do
                let sumMedianV3 = D.sum (F.col @Double "v3_median") ans6
                let sumSdV3 = D.sum (F.col @Double "v3_sd") ans6
                print (sumMedianV3, sumSdV3)
                return (sumMedianV3, sumSdV3)
            writeLog
                task
                dataName
                inRows
                question6
                outRows6
                outCols6
                solution
                ver
                git
                fun
                1
                t6_1
                m6_1
                cache
                (makeChk [fst chk6, snd chk6])
                chkt6_1
                onDisk
                machineType

            -- Run 2
            (ans6_2, t6_2) <- timeIt $ do
                let grouped = D.groupBy ["id4", "id5"] x
                let result =
                        D.aggregate
                            [ F.median (F.col @Double "v3") `F.as` "v3_median"
                            , F.stddev (F.col @Double "v3") `F.as` "v3_sd"
                            ]
                            grouped
                return result
            m6_2 <- getMemoryUsage
            let (outRows6_2, outCols6_2) = D.dimensions ans6_2
            (chk6_2, chkt6_2) <- timeIt $ do
                let sumMedianV3 = D.sum (F.col @Double "v3_median") ans6
                let sumSdV3 = D.sum (F.col @Double "v3_sd") ans6
                print (sumMedianV3, sumSdV3)
                return (sumMedianV3, sumSdV3)
            writeLog
                task
                dataName
                inRows
                question6
                outRows6_2
                outCols6_2
                solution
                ver
                git
                fun
                2
                t6_2
                m6_2
                cache
                (makeChk [fst chk6_2, snd chk6_2])
                chkt6_2
                onDisk
                machineType
            putStrLn $ "Question 6 completed: " ++ show outRows6_2 ++ " groups"

            -- "max v1 - min v2 by id3"
            let question7 = "median v3 sd v3 by id4 id5"
            (ans7, t7_1) <- timeIt $ do
                let grouped = D.groupBy ["id3"] x
                let result =
                        D.aggregate
                            [(F.maximum (F.col @Int "v1") - F.minimum (F.col @Int "v2")) `F.as` "diff"]
                            grouped
                return result
            m7_1 <- getMemoryUsage
            let (outRows7, outCols7) = D.dimensions ans7
            (chk7, chkt7_1) <- timeIt $ do
                let sumDiff = D.sum (F.col @Int "diff") ans7
                print sumDiff
                return sumDiff
            writeLog
                task
                dataName
                inRows
                question7
                outRows7
                outCols7
                solution
                ver
                git
                fun
                1
                t7_1
                m7_1
                cache
                (makeChk [fromIntegral chk7])
                chkt7_1
                onDisk
                machineType

            -- Run 2
            (ans7_2, t7_2) <- timeIt $ do
                let grouped = D.groupBy ["id3"] x
                let result =
                        D.aggregate
                            [(F.maximum (F.col @Int "v1") - F.minimum (F.col @Int "v2")) `F.as` "diff"]
                            grouped
                return result
            m7_2 <- getMemoryUsage
            let (outRows7_2, outCols7_2) = D.dimensions ans7_2
            (chk7_2, chkt7_2) <- timeIt $ do
                let sumDiff = D.sum (F.col @Int "diff") ans7
                print sumDiff
                return sumDiff
            writeLog
                task
                dataName
                inRows
                question7
                outRows7_2
                outCols7_2
                solution
                ver
                git
                fun
                2
                t7_2
                m7_2
                cache
                (makeChk [fromIntegral chk7_2])
                chkt7_2
                onDisk
                machineType
            putStrLn $ "Question 7 completed: " ++ show outRows7_2 ++ " groups"

            -- "largest two v3 by id6"
            putStrLn "largest two v3 by id6 unimplemented"

            -- "regression v1 v2 by id2 id4"
            putStrLn "regression v1 v2 by id2 id4 unimplemented"

            -- "sum v3 count by id1:id6"
            let question10 = "sum v3 count by id1:id6"
            (ans10, t10_1) <- timeIt $ do
                let grouped =
                        D.groupBy (zipWith (\i n -> i <> (T.pack . show) n) (cycle ["id"]) [1 .. 6]) x
                let result =
                        D.aggregate
                            [F.sum (F.col @Double "v3") `F.as` "v3_sum"]
                            grouped
                return result
            m10_1 <- getMemoryUsage
            let (outRows10, outCols10) = D.dimensions ans10
            (chk10, chkt10_1) <- timeIt $ do
                let sumV3 = D.sum (F.col @Double "v3_sum") ans10
                print sumV3
                return sumV3
            writeLog
                task
                dataName
                inRows
                question10
                outRows10
                outCols10
                solution
                ver
                git
                fun
                1
                t10_1
                m10_1
                cache
                (makeChk [chk10])
                chkt10_1
                onDisk
                machineType

            -- Run 2
            (ans10_2, t10_2) <- timeIt $ do
                let grouped =
                        D.groupBy (zipWith (\i n -> i <> (T.pack . show) n) (cycle ["id"]) [1 .. 6]) x
                let result =
                        D.aggregate
                            [F.sum (F.col @Double "v3") `F.as` "v3_sum"]
                            grouped
                return result
            m10_2 <- getMemoryUsage
            let (outRows10_2, outCols10_2) = D.dimensions ans10_2
            (chk10_2, chkt10_2) <- timeIt $ do
                let sumDiff = D.sum (F.col @Double "v3_sum") ans10
                print sumDiff
                return sumDiff
            writeLog
                task
                dataName
                inRows
                question10
                outRows10_2
                outCols10_2
                solution
                ver
                git
                fun
                2
                t10_2
                m10_2
                cache
                (makeChk [chk10_2])
                chkt10_2
                onDisk
                machineType
            putStrLn $ "Question 10 completed: " ++ show outRows7_2 ++ " groups"

            putStrLn
                "Haskell dataframe groupby benchmark completed (8 questions implemented)!"

-- Helper functions for logging
writeLog ::
    String ->
    String ->
    Int ->
    String ->
    Int ->
    Int ->
    String ->
    String ->
    String ->
    String ->
    Int ->
    Double ->
    Double ->
    String ->
    String ->
    Double ->
    String ->
    String ->
    IO ()
writeLog task dataName inRows question outRows outCols solution version git fun run timeSec memGb cache chk chkTimeSec onDisk machineType = do
    batch <- lookupEnv "BATCH" >>= return . maybe "" id
    timestamp <- getPOSIXTime
    csvFile <- lookupEnv "CSV_TIME_FILE" >>= return . maybe "time.csv" id
    nodename <- fmap init (readProcess "hostname" [] "")

    let comment = ""
    let timeSecRound = roundTo 3 timeSec
    let chkTimeSecRound = roundTo 3 chkTimeSec
    let memGbRound = roundTo 3 memGb

    let logRow =
            intercalate
                ","
                [ nodename
                , batch
                , show timestamp
                , task
                , dataName
                , show inRows
                , question
                , show outRows
                , show outCols
                , solution
                , version
                , git
                , fun
                , show run
                , show timeSecRound
                , show memGbRound
                , cache
                , chk
                , show chkTimeSecRound
                , comment
                , onDisk
                , machineType
                ]

    fileExists <- doesFileExist csvFile
    if fileExists
        then appendFile csvFile (logRow ++ "\n")
        else do
            let header =
                    "nodename,batch,timestamp,task,data,in_rows,question,out_rows,out_cols,solution,version,git,fun,run,time_sec,mem_gb,cache,chk,chk_time_sec,comment,on_disk,machine_type\n"
            writeFile csvFile (header ++ logRow ++ "\n")

roundTo :: Int -> Double -> Double
roundTo n x = (fromInteger $ round $ x * (10 ^ n)) / (10.0 ^^ n)

makeChk :: [Double] -> String
makeChk values = intercalate ";" (map formatVal values)
  where
    formatVal x = map (\c -> if c == ',' then '_' else c) (show $ roundTo 3 x)

getMemoryUsage :: IO Double
getMemoryUsage = do
    pid <- getProcessID
    mem <-
        fmap (filter (/= ' ') . init) (readProcess "ps" ["-o", "rss", show pid] "")
    let rssKb = if null mem then 0 else fromMaybe 0 (readMaybe @Double mem)
    return (rssKb / (1024 * 1024))

timeIt :: (Show a) => IO a -> IO (a, Double)
timeIt action = do
    start <- getPOSIXTime
    result <- action
    _ <- print result
    end <- getPOSIXTime
    return (result, realToFrac (end - start))
