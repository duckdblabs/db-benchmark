{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

import BenchmarkCommon
import Control.Arrow ((>>>))
import Control.Monad (forM_)
import Data.Functor ((<&>))
import qualified Data.List as L
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import DataFrame.Functions ((.=))
import qualified DataFrame.Functions as F
import DataFrame.IO.CSV (
    ReadOptions (..),
    TypeSpec (..),
    defaultReadOptions,
 )
import DataFrame.IO.CSV.Fast
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (AggStrategy (..), Expr (..))
import DataFrame.Internal.Schema (schemaType)
import qualified DataFrame.Operations.Aggregation as D
import qualified DataFrame.Operations.Core as D
import qualified DataFrame.Operations.Subset as D
import qualified DataFrame.Operations.Transformations as D
import System.Environment (getEnv, lookupEnv)
import System.IO (BufferMode (..), hPutStrLn, hSetBuffering, stderr, stdout)

main :: IO ()
main = do
    hSetBuffering stdout NoBuffering
    putStrLn "# groupby-haskell.hs"

    dataName <- getEnv "SRC_DATANAME"
    machineType <- lookupEnv "MACHINE_TYPE" <&> fromMaybe "UNSET"
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
        dv1 = F.toDouble v1
        dv2 = F.toDouble v2

    df <-
        fastReadCsvWithOpts
            ( defaultReadOptions
                { typeSpec =
                    SpecifyTypes
                        [ (F.name id1, schemaType @Text)
                        , (F.name id2, schemaType @Text)
                        , (F.name id3, schemaType @Text)
                        , (F.name id4, schemaType @Int)
                        , (F.name id5, schemaType @Int)
                        , (F.name id6, schemaType @Int)
                        , (F.name v1, schemaType @Int)
                        , (F.name v2, schemaType @Int)
                        , (F.name v3, schemaType @Double)
                        ]
                        NoInference
                }
            )
            srcFile
    let (inRows, _) = D.dimensions df
    print inRows
    print df

    ver <- lookupEnv "HASKELL_DF_VERSION" <&> fromMaybe ""

    let config =
            BenchConfig
                { cfgTask = "groupby"
                , cfgDataName = dataName
                , cfgMachineType = machineType
                , cfgSolution = "haskell"
                , cfgVer = ver
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
        (D.groupBy [F.name id1] >>> D.aggregate [F.sum v1 `F.as` "v1_sum"])
        (\res -> [chkSumInt "v1_sum" res])

    -- Q2: Sum v1 by id1:id2
    runQuestion
        config
        df
        "sum v1 by id1:id2"
        (D.groupBy [F.name id1, F.name id2] >>> D.aggregate ["v1_sum" .= F.sum v1])
        (\res -> [chkSumInt "v1_sum" res])

    -- Q3: Sum v1, Mean v3 by id3
    runQuestion
        config
        df
        "sum v1 mean v3 by id3"
        ( D.groupBy [F.name id3]
            >>> D.aggregate ["v1_sum" .= F.sum v1, "v3_mean" .= F.mean v3]
        )
        (\res -> [chkSumInt "v1_sum" res, chkSumDbl "v3_mean" res])

    -- Q4: Mean v1, v2, v3 by id4
    runQuestion
        config
        df
        "mean v1:v3 by id4"
        ( D.groupBy [F.name id4]
            >>> D.aggregate
                ["v1_mean" .= F.mean v1, "v2_mean" .= F.mean v2, "v3_mean" .= F.mean v3]
        )
        ( \res -> [chkSumDbl "v1_mean" res, chkSumDbl "v2_mean" res, chkSumDbl "v3_mean" res]
        )

    -- Q5: Sum v1, v2, v3 by id6
    runQuestion
        config
        df
        "sum v1:v3 by id6"
        ( D.groupBy [F.name id6]
            >>> D.aggregate
                ["v1_sum" .= F.sum v1, "v2_sum" .= F.sum v2, "v3_sum" .= F.sum v3]
        )
        (\res -> [chkSumInt "v1_sum" res, chkSumInt "v2_sum" res, chkSumDbl "v3_sum" res])

    -- Q6: Median v3, sd v3 by id4, id5
    runQuestion
        config
        df
        "median v3 sd v3 by id4 id5"
        ( D.groupBy [F.name id4, F.name id5]
            >>> D.aggregate
                ["v3_median" .= F.median v3, "v3_sd" .= F.stddev v3]
        )
        (\res -> [chkSumDbl "v3_median" res, chkSumDbl "v3_sd" res])

    -- Q7: Max v1 - min v2 by id3
    runQuestion
        config
        df
        "max v1 - min v2 by id3"
        (D.groupBy [F.name id3] >>> D.aggregate ["diff" .= F.maximum v1 - F.minimum v2])
        (\res -> [chkSumInt "diff" res])

    -- Q8: Largest two v3 by id6.
    -- Polars takes top_k(2) per group then explodes and sums; the sum of the
    -- exploded values equals the sum of (max + 2nd-max) per group, which is
    -- what 'top2Sum' computes per group. Checksum parity is preserved.
    runQuestion
        config
        df
        "largest two v3 by id6"
        (D.groupBy [F.name id6] >>> D.aggregate ["largest2_v3" .= top2Sum v3])
        (\res -> [chkSumDbl "largest2_v3" res])

    -- Q9: Regression (r^2 of v1 vs v2) by id2, id4.
    -- corr(v1,v2)^2 from the per-group sums of v1, v2, v1*v2, v1^2, v2^2 and the
    -- group count, then summed. Matches polars pl.corr(...)**2 then .sum().
    runQuestion
        config
        ( D.derive
            "v2v2"
            (dv2 * dv2)
            (D.derive "v1v1" (dv1 * dv1) (D.derive "v1v2" (dv1 * dv2) df))
        )
        "regression v1 v2 by id2 id4"
        ( D.groupBy [F.name id2, F.name id4]
            >>> D.aggregate
                [ "n" .= F.count v1
                , "sx" .= F.sum dv1
                , "sy" .= F.sum dv2
                , "sxy" .= F.sum (F.col @Double "v1v2")
                , "sxx" .= F.sum (F.col @Double "v1v1")
                , "syy" .= F.sum (F.col @Double "v2v2")
                ]
            >>> D.derive "r2" r2Expr
        )
        (\res -> [chkSumDbl "r2" res])

    -- Q10: Sum v3 count by id1:id6
    runQuestion
        config
        df
        "sum v3 count by id1:id6"
        ( D.groupBy (map ((\i n -> i <> (T.pack . show) n) "id") [1 .. 6])
            >>> D.aggregate [F.sum v3 `F.as` "v3_sum"]
        )
        (\res -> [chkSumDbl "v3_sum" res])

    writeToLogFile config "finish"
    putStrLn "Haskell dataframe groupby benchmark completed!"

runQuestion ::
    BenchConfig ->
    DataFrame ->
    String ->
    (DataFrame -> DataFrame) ->
    (DataFrame -> [Double]) ->
    IO ()
runQuestion cfg inputDF qLabel transform chkFn = do
    forM_ [1, 2] $ \runNum -> do
        (resultDF, calcTime) <- timeIt $ do
            let result = freshRun runNum transform inputDF
            print result
            return result
        memUsage <- getMemoryUsage
        let (outRows, outCols) = D.dimensions resultDF
        (chkValues, chkTime) <- timeIt $ do
            let vals = chkFn resultDF
            print vals
            return vals
        writeLog cfg qLabel outRows outCols runNum calcTime memUsage chkValues chkTime
    putStrLn $ qLabel ++ " completed."

chkSumInt :: String -> DataFrame -> Double
chkSumInt col df =
    case D.columnAsIntVector (F.col @Int (T.pack col)) df of
        Right vec -> fromIntegral $ VU.sum vec
        Left _ -> 0.0

chkSumDbl :: String -> DataFrame -> Double
chkSumDbl col df =
    case D.columnAsDoubleVector (F.col @Double (T.pack col)) df of
        Right vec -> VU.sum vec
        Left _ -> 0.0

-- | Per-group sum of the two largest values (top_k(2) then sum, like polars Q8).
top2Sum :: Expr Double -> Expr Double
top2Sum = Agg (CollectAgg "top2Sum" f)
  where
    f :: V.Vector Double -> Double
    f v = Prelude.sum (take 2 (L.sortBy (flip compare) (V.toList v)))

{- | Squared Pearson correlation of v1,v2 from per-group moment sums (polars Q9).
Degenerate groups (zero variance) contribute 0 rather than poisoning the sum
with NaN; the 1e7 data has no such groups so the checksum is unaffected.
-}
r2Expr :: Expr Double
r2Expr =
    let n = F.toDouble (F.col @Int "n")
        sx = F.col @Double "sx"
        sy = F.col @Double "sy"
        sxy = F.col @Double "sxy"
        sxx = F.col @Double "sxx"
        syy = F.col @Double "syy"
        num = n * sxy - sx * sy
        den = (n * sxx - sx * sx) * (n * syy - sy * sy)
     in If (F.gt den (Lit 0)) ((num * num) / den) (Lit 0)
