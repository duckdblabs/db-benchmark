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
    encUseCrLf,
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
import qualified DataFrame.Functions as F
import qualified DataFrame.Operations.Join as DJ
import GHC.Stats (getRTSStats, getRTSStatsEnabled, max_live_bytes)
import Numeric (showEFloat, showFFloat)
import System.Directory (doesFileExist, removeFile)
import System.Environment (getEnv, lookupEnv)
import System.IO (
    BufferMode (..),
    IOMode (..),
    hFlush,
    hPutStr,
    hPutStrLn,
    hSetBuffering,
    stdout,
    withFile,
 )
import System.Posix.Files (fileSize, getFileStatus, removeLink)
import System.Posix.Process (getProcessID)
import System.Process (readProcess)
import Text.Read (readMaybe)

-- | Configuration context
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

-- TODO: Renable join for later versions.
main :: IO ()
main = do
    hSetBuffering stdout NoBuffering
    putStrLn "# join-haskell.hs"

    dataName <- getEnv "SRC_DATANAME"
    machineType <- getEnv "MACHINE_TYPE"

    let auxTableNames = determineAuxTables dataName
    let srcMain = "./data/" ++ dataName ++ ".csv"
    let srcAux = map (\n -> "./data/" ++ n ++ ".csv") auxTableNames

    putStrLn $
        "loading datasets: " ++ dataName ++ ", " ++ intercalate ", " auxTableNames

    dfX <- D.readCsvWithOpts
            ( D.defaultReadOptions
                { D.typeSpec =
                    D.SpecifyTypes
                        [ D.schemaType @Int
                        , D.schemaType @Int
                        , D.schemaType @Int
                        , D.schemaType @Text
                        , D.schemaType @Text
                        , D.schemaType @Text
                        , D.schemaType @Double
                        ]
                }
            ) srcMain
    dfSmall <- D.readCsvWithOpts
                ( D.defaultReadOptions
                    { D.typeSpec =
                        D.SpecifyTypes
                            [ D.schemaType @Int
                            , D.schemaType @Text
                            , D.schemaType @Double
                            ]
                    }
                )  (head srcAux)
    dfMedium <- D.readCsvWithOpts
                ( D.defaultReadOptions
                    { D.typeSpec =
                        D.SpecifyTypes
                            [ D.schemaType @Int
                            , D.schemaType @Int
                            , D.schemaType @Text
                            , D.schemaType @Text
                            , D.schemaType @Double
                            ]
                    }
                ) (srcAux !! 1)
    dfBig <- D.readCsvWithOpts
                ( D.defaultReadOptions
                    { D.typeSpec =
                        D.SpecifyTypes
                            [ D.schemaType @Int
                            , D.schemaType @Int
                            , D.schemaType @Int
                            , D.schemaType @Text
                            , D.schemaType @Text
                            , D.schemaType @Text
                            , D.schemaType @Double
                            ]
                    }
                ) (srcAux !! 2)

    let (rowsX, _) = D.dimensions dfX
    print
        ( rowsX
        , fst (D.dimensions dfSmall)
        , fst (D.dimensions dfMedium)
        , fst (D.dimensions dfBig)
        )

    let config =
            BenchConfig
                { cfgTask = "join"
                , cfgDataName = dataName
                , cfgMachineType = machineType
                , cfgSolution = "haskell"
                , cfgVer = "0.4.1"
                , cfgGit = "dataframe"
                , cfgFun = "innerJoin"
                , cfgCache = "TRUE"
                , cfgOnDisk = "FALSE"
                , cfgInRows = rowsX
                }

    putStrLn "joining..."

    writeToLogFile config "start"

    -- Q1: small inner on int
    runJoin
        config
        dfX
        dfSmall
        "small inner on int"
        (\l r -> DJ.innerJoin ["id1"] r l)

    -- Q2: medium inner on int
    runJoin
        config
        dfX
        dfMedium
        "medium inner on int"
        (\l r -> DJ.innerJoin ["id1"] r l)

    -- Q3: medium outer on int
    -- Note: We update the function name in the config for logging accuracy
    runJoin
        config{cfgFun = "leftJoin"}
        dfX
        dfMedium
        "medium outer on int"
        (\l r -> DJ.leftJoin ["id1"] r l)

    -- Q4: medium inner on factor (id4)
    runJoin
        config
        dfX
        dfMedium
        "medium inner on factor"
        (\l r -> DJ.innerJoin ["id4"] r l)

    -- Q5: big inner on int
    runJoin
        config
        dfX
        dfBig
        "big inner on int"
        (\l r -> DJ.innerJoin ["id1"] r l)

    writeToLogFile config "finish"
    putStrLn "Haskell dataframe join benchmark completed!"

runJoin ::
    BenchConfig ->
    -- | Left Table
    D.DataFrame ->
    -- | Right Table
    D.DataFrame ->
    -- | Question Label
    String ->
    -- | Join Function
    (D.DataFrame -> D.DataFrame -> D.DataFrame) ->
    IO ()
runJoin cfg leftDF rightDF qLabel joinFn = do
    forM_ [1, 2] $ \runNum -> do
        (resultDF, calcTime) <- timeIt $ do
            let res = joinFn leftDF rightDF
            _ <- evaluate res
            return res

        memUsage <- getMemoryUsage
        let (outRows, outCols) = D.dimensions resultDF

        (chkValues, chkTime) <- timeIt $ do
            let sumV1 = sumCol "v1" resultDF
            let sumV2 = sumCol "v2" resultDF
            let res = (sumV1, sumV2)
            _ <- evaluate res
            return [sumV1, sumV2]

        writeLog cfg qLabel outRows outCols runNum calcTime memUsage chkValues chkTime

    putStrLn $ qLabel ++ " completed."

sumCol :: String -> D.DataFrame -> Double
sumCol name df =
    case D.columnAsDoubleVector (F.col @Double (T.pack name)) df of
        Right vec -> VU.sum vec
        Left _ -> 0.0

determineAuxTables :: String -> [String]
determineAuxTables dataName =
    let parts = T.splitOn "_" (T.pack dataName)
        raw = parts !! 1
        x_n :: Int
        x_n = truncate (read (T.unpack raw) :: Double)

        fmtE0 :: Double -> T.Text
        fmtE0 x =
            let s = T.pack (showEFloat (Just 0) x "")
             in T.replace "+0" "" s

        y0 = fmtE0 (fromIntegral x_n / 1e6)
        y1 = fmtE0 (fromIntegral x_n / 1e3)
        y2 = fmtE0 (fromIntegral x_n)

        replaceNA :: T.Text -> String
        replaceNA rep = T.unpack $ T.replace "NA" rep (T.pack dataName)
     in [replaceNA y0, replaceNA y1, replaceNA y2]

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
    batch <- lookupEnv "BATCH" >>= return . fromMaybe ""
    timestamp <- getPOSIXTime
    csvFile <- lookupEnv "CSV_TIME_FILE" >>= return . fromMaybe "time.csv"
    csvVerbose <- lookupEnv "CSV_VERBOSE" >>= return . fromMaybe "false"
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
                then encodeWith defaultEncodeOptions [logFileRow]
                else encodeWith defaultEncodeOptions [logFileHeader, logFileRow]

    if append
        then BL.appendFile logFile csvData
        else BL.writeFile logFile csvData

roundTo :: Int -> Double -> Double
roundTo n x = (fromInteger $ round $ x * (10 ^ n)) / (10.0 ^^ n)
