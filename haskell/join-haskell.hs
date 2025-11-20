{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeApplications #-}

import Control.Exception (evaluate)
import Control.Monad (forM_)
import Data.List (intercalate)
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Operations.Join as DJ
import GHC.Stats (getRTSStats, max_live_bytes, getRTSStatsEnabled)
import System.Directory (doesFileExist)
import System.Environment (getEnv, lookupEnv)
import System.IO (hFlush, hPutStrLn, hPutStr, stdout, withFile, IOMode(..), hSetBuffering, BufferMode(..))
import System.Process (readProcess)
import System.Posix.Process (getProcessID)
import Text.Read (readMaybe)

-- | Configuration context
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
    putStrLn "# join-haskell.hs"

    dataName    <- getEnv "SRC_DATANAME"
    machineType <- getEnv "MACHINE_TYPE"
    
    let auxTableNames = determineAuxTables dataName
    let srcMain = "../data/" ++ dataName ++ ".csv"
    let srcAux  = map (\n -> "../data/" ++ n ++ ".csv") auxTableNames

    putStrLn $ "loading datasets: " ++ dataName ++ ", " ++ intercalate ", " auxTableNames

    dfX      <- D.readCsv srcMain
    dfSmall  <- D.readCsv (srcAux !! 0)
    dfMedium <- D.readCsv (srcAux !! 1)
    dfBig    <- D.readCsv (srcAux !! 2)

    let (rowsX, _) = D.dimensions dfX
    print (rowsX, fst (D.dimensions dfSmall), fst (D.dimensions dfMedium), fst (D.dimensions dfBig))

    let config = BenchConfig 
            { cfgTask = "join"
            , cfgDataName = dataName
            , cfgMachineType = machineType
            , cfgSolution = "haskell"
            , cfgVer = "0.3.3"
            , cfgGit = "dataframe"
            , cfgFun = "innerJoin"
            , cfgCache = "TRUE"
            , cfgOnDisk = "FALSE"
            , cfgInRows = rowsX
            }

    putStrLn "joining..."

    -- Q1: small inner on int
    runJoin config dfX dfSmall "small inner on int"
        (\l r -> DJ.innerJoin ["id1"] l r)

    -- Q2: medium inner on int
    runJoin config dfX dfMedium "medium inner on int"
        (\l r -> DJ.innerJoin ["id1"] l r)

    -- Q3: medium outer on int
    -- Note: We update the function name in the config for logging accuracy
    runJoin config{cfgFun="leftJoin"} dfX dfMedium "medium outer on int"
        (\l r -> DJ.leftJoin ["id1"] l r)

    -- Q4: medium inner on factor (id4)
    runJoin config dfX dfMedium "medium inner on factor"
        (\l r -> DJ.innerJoin ["id4"] l r)

    -- Q5: big inner on int
    runJoin config dfX dfBig "big inner on int"
        (\l r -> DJ.innerJoin ["id1"] l r)

    putStrLn "Haskell dataframe join benchmark completed!"

runJoin :: BenchConfig 
        -> D.DataFrame -- ^ Left Table
        -> D.DataFrame -- ^ Right Table
        -> String      -- ^ Question Label
        -> (D.DataFrame -> D.DataFrame -> D.DataFrame) -- ^ Join Function
        -> IO ()
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
    case D.columnAsDoubleVector (T.pack name) df of
        Right vec -> VU.sum vec
        Left _    -> 0.0

determineAuxTables :: String -> [String]
determineAuxTables dataName =
    let parts = T.splitOn "_" (T.pack dataName)
        xnStr = if length parts > 1 then T.unpack (parts !! 1) else "1e7"
        xn = read xnStr :: Double

        yn1 = show (floor (xn / 1e6) :: Int) ++ "e4"
        yn2 = show (floor (xn / 1e3) :: Int) ++ "e3"
        yn3 = show (floor xn :: Int)
        
        replaceNA s = T.unpack $ T.replace "NA" (T.pack s) (T.pack dataName)
    in [ replaceNA yn1, replaceNA yn2, replaceNA yn3 ]

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
    _ <- evaluate result
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
