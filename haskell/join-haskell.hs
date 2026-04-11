{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

import BenchmarkCommon
import Control.Exception (evaluate)
import Control.Monad (forM_)
import Data.List (intercalate)
import qualified Data.Text as T
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Operations.Join as DJ
import Numeric (showEFloat)
import System.Environment (getEnv)
import System.IO (BufferMode (..), hSetBuffering, stdout)

-- TODO: Renable join for later versions.
main :: IO ()
main = pure ()

joinMain :: IO ()
joinMain = do
    hSetBuffering stdout NoBuffering
    putStrLn "# join-haskell.hs"

    dataName <- getEnv "SRC_DATANAME"
    machineType <- getEnv "MACHINE_TYPE"

    let auxTableNames = determineAuxTables dataName
    let srcMain = "./data/" ++ dataName ++ ".csv"
    let srcAux = map (\n -> "./data/" ++ n ++ ".csv") auxTableNames

    putStrLn $
        "loading datasets: " ++ dataName ++ ", " ++ intercalate ", " auxTableNames

    dfX <- D.readCsv srcMain
    dfSmall <- D.readCsv (head srcAux)
    dfMedium <- D.readCsv (srcAux !! 1)
    dfBig <- D.readCsv (srcAux !! 2)

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
    runJoin config dfX dfSmall "small inner on int" (DJ.innerJoin ["id1"])

    -- Q2: medium inner on int
    runJoin config dfX dfMedium "medium inner on int" (DJ.innerJoin ["id1"])

    -- Q3: medium outer on int
    runJoin
        config{cfgFun = "leftJoin"}
        dfX
        dfMedium
        "medium outer on int"
        (DJ.leftJoin ["id1"])

    -- Q4: medium inner on factor (id4)
    runJoin config dfX dfMedium "medium inner on factor" (DJ.innerJoin ["id4"])

    -- Q5: big inner on int
    runJoin config dfX dfBig "big inner on int" (DJ.innerJoin ["id1"])

    writeToLogFile config "finish"
    putStrLn "Haskell dataframe join benchmark completed!"

runJoin ::
    BenchConfig ->
    D.DataFrame ->
    D.DataFrame ->
    String ->
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
