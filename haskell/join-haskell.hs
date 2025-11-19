{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import Control.Exception (evaluate)
import Data.List (intercalate)
import qualified Data.Text as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Operations.Join as DJ
import System.Directory (doesFileExist)
import System.Environment (getEnv, lookupEnv)
import System.IO (hFlush, stdout)
import System.Process (readProcess)

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

timeIt :: IO a -> IO (a, Double)
timeIt action = do
    start <- getPOSIXTime
    result <- action
    _ <- evaluate result
    end <- getPOSIXTime
    return (result, realToFrac (end - start))

-- Parse join_to_tbls logic to get table names
joinToTbls :: String -> [String]
joinToTbls dataName =
    let parts = T.splitOn "_" (T.pack dataName)
        xnStr = if length parts > 1 then T.unpack (parts !! 1) else "1e7"
        xn = read xnStr :: Double
        yn1 = show (floor (xn / 1e6) :: Int) ++ "e4"
        yn2 = show (floor (xn / 1e3) :: Int) ++ "e3"
        yn3 = show (floor xn :: Int)
     in [ T.unpack $ T.replace "NA" (T.pack yn1) (T.pack dataName)
        , T.unpack $ T.replace "NA" (T.pack yn2) (T.pack dataName)
        , T.unpack $ T.replace "NA" (T.pack yn3) (T.pack dataName)
        ]

main :: IO ()
main = do
    putStrLn "# join-haskell.hs"
    hFlush stdout

    let ver = "0.3.3"
    let git = "dataframe"
    let task = "join"
    let solution = "haskell"
    let fun = "innerJoin"
    let cache = "TRUE"
    let onDisk = "FALSE"

    dataName <- getEnv "SRC_DATANAME"
    machineType <- getEnv "MACHINE_TYPE"

    let yDataNames = joinToTbls dataName
    let srcJnX = "../data/" ++ dataName ++ ".csv"
    let srcJnY =
            [ "../data/" ++ yDataNames !! 0 ++ ".csv"
            , "../data/" ++ yDataNames !! 1 ++ ".csv"
            , "../data/" ++ yDataNames !! 2 ++ ".csv"
            ]

    putStrLn $
        "loading datasets "
            ++ dataName
            ++ ", "
            ++ yDataNames !! 0
            ++ ", "
            ++ yDataNames !! 1
            ++ ", "
            ++ yDataNames !! 2
    hFlush stdout

    -- Load all datasets using dataframe
    x <- D.readCsv srcJnX
    small <- D.readCsv (srcJnY !! 0)
    medium <- D.readCsv (srcJnY !! 1)
    big <- D.readCsv (srcJnY !! 2)

    let (xRows, _) = D.dimensions x
    let (smallRows, _) = D.dimensions small
    let (mediumRows, _) = D.dimensions medium
    let (bigRows, _) = D.dimensions big

    putStrLn $ show xRows
    putStrLn $ show smallRows
    putStrLn $ show mediumRows
    putStrLn $ show bigRows
    hFlush stdout

    putStrLn "joining..."
    hFlush stdout

    -- Question 1: small inner on int
    let question1 = "small inner on int"
    (ans1, t1_1) <- timeIt $ do
        let result = DJ.innerJoin ["id1"] x small
        return result
    m1_1 <- getMemoryUsage
    let (outRows1, outCols1) = D.dimensions ans1
    (chk1, chkt1_1) <- timeIt $ do
        let sumV1 = case D.columnAsDoubleVector "v1" ans1 of
                Right vec -> VU.sum vec
                Left _ -> 0
        let sumV2 = case D.columnAsDoubleVector "v2" ans1 of
                Right vec -> VU.sum vec
                Left _ -> 0
        evaluate (sumV1, sumV2)
        return (sumV1, sumV2)
    writeLog
        task
        dataName
        xRows
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
        (makeChk [fst chk1, snd chk1])
        chkt1_1
        onDisk
        machineType

    -- Run 2
    (ans1_2, t1_2) <- timeIt $ do
        let result = DJ.innerJoin ["id1"] x small
        return result
    m1_2 <- getMemoryUsage
    let (outRows1_2, outCols1_2) = D.dimensions ans1_2
    (chk1_2, chkt1_2) <- timeIt $ do
        let sumV1 = case D.columnAsDoubleVector "v1" ans1_2 of
                Right vec -> VU.sum vec
                Left _ -> 0
        let sumV2 = case D.columnAsDoubleVector "v2" ans1_2 of
                Right vec -> VU.sum vec
                Left _ -> 0
        evaluate (sumV1, sumV2)
        return (sumV1, sumV2)
    writeLog
        task
        dataName
        xRows
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
        (makeChk [fst chk1_2, snd chk1_2])
        chkt1_2
        onDisk
        machineType
    putStrLn $ "Question 1 completed: " ++ show outRows1_2 ++ " rows"

    -- Question 2: medium inner on int
    let question2 = "medium inner on int"
    (ans2, t2_1) <- timeIt $ do
        let result = DJ.innerJoin ["id1"] x medium
        return result
    m2_1 <- getMemoryUsage
    let (outRows2, outCols2) = D.dimensions ans2
    (chk2, chkt2_1) <- timeIt $ do
        let sumV1 = case D.columnAsDoubleVector "v1" ans2 of
                Right vec -> VU.sum vec
                Left _ -> 0
        let sumV2 = case D.columnAsDoubleVector "v2" ans2 of
                Right vec -> VU.sum vec
                Left _ -> 0
        evaluate (sumV1, sumV2)
        return (sumV1, sumV2)
    writeLog
        task
        dataName
        xRows
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
        (makeChk [fst chk2, snd chk2])
        chkt2_1
        onDisk
        machineType

    -- Run 2
    (ans2_2, t2_2) <- timeIt $ do
        let result = DJ.innerJoin ["id1"] x medium
        return result
    m2_2 <- getMemoryUsage
    let (outRows2_2, outCols2_2) = D.dimensions ans2_2
    (chk2_2, chkt2_2) <- timeIt $ do
        let sumV1 = case D.columnAsDoubleVector "v1" ans2_2 of
                Right vec -> VU.sum vec
                Left _ -> 0
        let sumV2 = case D.columnAsDoubleVector "v2" ans2_2 of
                Right vec -> VU.sum vec
                Left _ -> 0
        evaluate (sumV1, sumV2)
        return (sumV1, sumV2)
    writeLog
        task
        dataName
        xRows
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
        (makeChk [fst chk2_2, snd chk2_2])
        chkt2_2
        onDisk
        machineType
    putStrLn $ "Question 2 completed: " ++ show outRows2_2 ++ " rows"

    -- Question 3: medium outer on int
    let question3 = "medium outer on int"
    (ans3, t3_1) <- timeIt $ do
        let result = DJ.leftJoin ["id1"] x medium
        return result
    m3_1 <- getMemoryUsage
    let (outRows3, outCols3) = D.dimensions ans3
    (chk3, chkt3_1) <- timeIt $ do
        let sumV1 = case D.columnAsDoubleVector "v1" ans3 of
                Right vec -> VU.sum vec
                Left _ -> 0
        let sumV2 = case D.columnAsDoubleVector "v2" ans3 of
                Right vec -> VU.sum vec
                Left _ -> 0
        evaluate (sumV1, sumV2)
        return (sumV1, sumV2)
    writeLog
        task
        dataName
        xRows
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
        (makeChk [fst chk3, snd chk3])
        chkt3_1
        onDisk
        machineType

    -- Run 2
    (ans3_2, t3_2) <- timeIt $ do
        let result = DJ.leftJoin ["id1"] x medium
        return result
    m3_2 <- getMemoryUsage
    let (outRows3_2, outCols3_2) = D.dimensions ans3_2
    (chk3_2, chkt3_2) <- timeIt $ do
        let sumV1 = case D.columnAsDoubleVector "v1" ans3_2 of
                Right vec -> VU.sum vec
                Left _ -> 0
        let sumV2 = case D.columnAsDoubleVector "v2" ans3_2 of
                Right vec -> VU.sum vec
                Left _ -> 0
        evaluate (sumV1, sumV2)
        return (sumV1, sumV2)
    writeLog
        task
        dataName
        xRows
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
        (makeChk [fst chk3_2, snd chk3_2])
        chkt3_2
        onDisk
        machineType
    putStrLn $ "Question 3 completed: " ++ show outRows3_2 ++ " rows"

    -- Question 4: medium inner on factor
    let question4 = "medium inner on factor"
    (ans4, t4_1) <- timeIt $ do
        let result = DJ.innerJoin ["id4"] x medium
        return result
    m4_1 <- getMemoryUsage
    let (outRows4, outCols4) = D.dimensions ans4
    (chk4, chkt4_1) <- timeIt $ do
        let sumV1 = case D.columnAsDoubleVector "v1" ans4 of
                Right vec -> VU.sum vec
                Left _ -> 0
        let sumV2 = case D.columnAsDoubleVector "v2" ans4 of
                Right vec -> VU.sum vec
                Left _ -> 0
        evaluate (sumV1, sumV2)
        return (sumV1, sumV2)
    writeLog
        task
        dataName
        xRows
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
        (makeChk [fst chk4, snd chk4])
        chkt4_1
        onDisk
        machineType

    -- Run 2
    (ans4_2, t4_2) <- timeIt $ do
        let result = DJ.innerJoin ["id4"] x medium
        return result
    m4_2 <- getMemoryUsage
    let (outRows4_2, outCols4_2) = D.dimensions ans4_2
    (chk4_2, chkt4_2) <- timeIt $ do
        let sumV1 = case D.columnAsDoubleVector "v1" ans4_2 of
                Right vec -> VU.sum vec
                Left _ -> 0
        let sumV2 = case D.columnAsDoubleVector "v2" ans4_2 of
                Right vec -> VU.sum vec
                Left _ -> 0
        evaluate (sumV1, sumV2)
        return (sumV1, sumV2)
    writeLog
        task
        dataName
        xRows
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
        (makeChk [fst chk4_2, snd chk4_2])
        chkt4_2
        onDisk
        machineType
    putStrLn $ "Question 4 completed: " ++ show outRows4_2 ++ " rows"

    -- Question 5: big inner on int
    let question5 = "big inner on int"
    (ans5, t5_1) <- timeIt $ do
        let result = DJ.innerJoin ["id1"] x big
        return result
    m5_1 <- getMemoryUsage
    let (outRows5, outCols5) = D.dimensions ans5
    (chk5, chkt5_1) <- timeIt $ do
        let sumV1 = case D.columnAsDoubleVector "v1" ans5 of
                Right vec -> VU.sum vec
                Left _ -> 0
        let sumV2 = case D.columnAsDoubleVector "v2" ans5 of
                Right vec -> VU.sum vec
                Left _ -> 0
        evaluate (sumV1, sumV2)
        return (sumV1, sumV2)
    writeLog
        task
        dataName
        xRows
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
        (makeChk [fst chk5, snd chk5])
        chkt5_1
        onDisk
        machineType

    -- Run 2
    (ans5_2, t5_2) <- timeIt $ do
        let result = DJ.innerJoin ["id1"] x big
        return result
    m5_2 <- getMemoryUsage
    let (outRows5_2, outCols5_2) = D.dimensions ans5_2
    (chk5_2, chkt5_2) <- timeIt $ do
        let sumV1 = case D.columnAsDoubleVector "v1" ans5_2 of
                Right vec -> VU.sum vec
                Left _ -> 0
        let sumV2 = case D.columnAsDoubleVector "v2" ans5_2 of
                Right vec -> VU.sum vec
                Left _ -> 0
        evaluate (sumV1, sumV2)
        return (sumV1, sumV2)
    writeLog
        task
        dataName
        xRows
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
        (makeChk [fst chk5_2, snd chk5_2])
        chkt5_2
        onDisk
        machineType
    putStrLn $ "Question 5 completed: " ++ show outRows5_2 ++ " rows"

    putStrLn "Haskell dataframe join benchmark completed (5 questions implemented)!"
