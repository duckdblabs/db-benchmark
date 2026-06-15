{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

import BenchmarkCommon
import Control.Exception (evaluate)
import Control.Monad (forM_)
import Data.Functor ((<&>))
import Data.List (intercalate)
import Data.Maybe (catMaybes, fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame.Functions as F
import DataFrame.IO.CSV (
    ReadOptions (..),
    TypeSpec (..),
    defaultReadOptions,
 )
import DataFrame.IO.CSV.Fast (fastReadCsvWithOpts)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Schema (SchemaType, schemaType)
import qualified DataFrame.Operations.Core as D
import qualified DataFrame.Operations.Join as DJ
import Numeric (showEFloat)
import System.Environment (getEnv, lookupEnv)
import System.IO (BufferMode (..), hSetBuffering, stdout)

main :: IO ()
main = do
    hSetBuffering stdout NoBuffering
    putStrLn "# join-haskell.hs"

    dataName <- getEnv "SRC_DATANAME"
    machineType <- lookupEnv "MACHINE_TYPE" <&> fromMaybe "UNSET"

    let auxTableNames = determineAuxTables dataName
    let srcMain = "./data/" ++ dataName ++ ".csv"
    let srcAux = map (\n -> "./data/" ++ n ++ ".csv") auxTableNames

    putStrLn $
        "loading datasets: " ++ dataName ++ ", " ++ intercalate ", " auxTableNames

    dfX <- fastReadCsvWithOpts (optsFor mainCols) srcMain
    dfSmall <- fastReadCsvWithOpts (optsFor smallCols) (head srcAux)
    dfMedium <- fastReadCsvWithOpts (optsFor mediumCols) (srcAux !! 1)
    dfBig <- fastReadCsvWithOpts (optsFor bigCols) (srcAux !! 2)

    let (rowsX, _) = D.dimensions dfX
    print
        ( rowsX
        , fst (D.dimensions dfSmall)
        , fst (D.dimensions dfMedium)
        , fst (D.dimensions dfBig)
        )

    ver <- lookupEnv "HASKELL_DF_VERSION" <&> fromMaybe ""

    let config =
            BenchConfig
                { cfgTask = "join"
                , cfgDataName = dataName
                , cfgMachineType = machineType
                , cfgSolution = "haskell"
                , cfgVer = ver
                , cfgGit = "NA"
                , cfgFun = "innerJoin"
                , cfgCache = "TRUE"
                , cfgOnDisk = "FALSE"
                , cfgInRows = rowsX
                }

    putStrLn "joining..."

    writeToLogFile config "start"

    -- Q1: small inner on id1
    runJoin config dfX dfSmall "small inner on int" (DJ.innerJoin ["id1"])

    -- Q2: medium inner on id2
    runJoin config dfX dfMedium "medium inner on int" (DJ.innerJoin ["id2"])

    -- Q3: medium outer (left) on id2
    runJoin
        config{cfgFun = "leftJoin"}
        dfX
        dfMedium
        "medium outer on int"
        (DJ.leftJoin ["id2"])

    -- Q4: medium inner on factor (id5)
    runJoin config dfX dfMedium "medium inner on factor" (DJ.innerJoin ["id5"])

    -- Q5: big inner on id3
    runJoin config dfX dfBig "big inner on int" (DJ.innerJoin ["id3"])

    writeToLogFile config "finish"
    putStrLn "Haskell dataframe join benchmark completed!"

{- | Schema columns: (name, SchemaType). id1/id2/id3 are Int, id4/id5/id6 Text,
v1/v2 Double, matching the J1 table layouts used by the polars solution.
-}
intCol :: T.Text -> (T.Text, SchemaType)
intCol n = (n, schemaType @Int)

txtCol :: T.Text -> (T.Text, SchemaType)
txtCol n = (n, schemaType @Text)

dblCol :: T.Text -> (T.Text, SchemaType)
dblCol n = (n, schemaType @Double)

mainCols :: [(T.Text, SchemaType)]
mainCols =
    [ intCol "id1"
    , intCol "id2"
    , intCol "id3"
    , txtCol "id4"
    , txtCol "id5"
    , txtCol "id6"
    , dblCol "v1"
    ]

smallCols :: [(T.Text, SchemaType)]
smallCols = [intCol "id1", txtCol "id4", dblCol "v2"]

mediumCols :: [(T.Text, SchemaType)]
mediumCols = [intCol "id1", intCol "id2", txtCol "id4", txtCol "id5", dblCol "v2"]

bigCols :: [(T.Text, SchemaType)]
bigCols =
    [ intCol "id1"
    , intCol "id2"
    , intCol "id3"
    , txtCol "id4"
    , txtCol "id5"
    , txtCol "id6"
    , dblCol "v2"
    ]

optsFor :: [(T.Text, SchemaType)] -> ReadOptions
optsFor cols = defaultReadOptions{typeSpec = SpecifyTypes cols NoInference}

runJoin ::
    BenchConfig ->
    DataFrame ->
    DataFrame ->
    String ->
    (DataFrame -> DataFrame -> DataFrame) ->
    IO ()
runJoin cfg leftDF rightDF qLabel joinFn = do
    forM_ [1, 2] $ \runNum -> do
        (resultDF, calcTime) <- timeIt $ do
            let res = freshRun runNum (uncurry joinFn) (leftDF, rightDF)
            _ <- evaluate res
            return res

        memUsage <- getMemoryUsage
        let (outRows, outCols) = D.dimensions resultDF

        (chkValues, chkTime) <- timeIt $ do
            let sumV1 = sumCol "v1" resultDF
            let sumV2 = sumCol "v2" resultDF
            let res = [sumV1, sumV2]
            _ <- evaluate res
            return res

        writeLog cfg qLabel outRows outCols runNum calcTime memUsage chkValues chkTime

    putStrLn $ qLabel ++ " completed."

{- | Sum a Double column, skipping nulls/NaN. A left join surfaces the
right-side value column as @Maybe Double@; 'columnAsDoubleVector' coerces
nulls to NaN, so we drop NaN (matching polars sum, which skips nulls).
-}
sumCol :: Text -> DataFrame -> Double
sumCol name df =
    case D.columnAsDoubleVector (F.col @Double name) df of
        Right vec -> VU.sum (VU.filter (not . isNaN) vec)
        Left _ ->
            case D.columnAsVector (F.col @(Maybe Double) name) df of
                Right vec -> Prelude.sum (catMaybes (V.toList vec))
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
