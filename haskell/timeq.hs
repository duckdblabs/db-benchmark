{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE BangPatterns #-}

import Control.Arrow ((>>>))
import Control.Monad (forM_)
import Control.DeepSeq (force)
import Control.Exception (evaluate)
import Data.Functor ((<&>))
import qualified Data.List as L
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import DataFrame.Functions ((.=))
import qualified DataFrame.Functions as F
import DataFrame.IO.CSV (ReadOptions (..), TypeSpec (..), defaultReadOptions)
import DataFrame.IO.CSV.Fast
import DataFrame.Internal.DataFrame (DataFrame, rowToGroup)
import DataFrame.Internal.Expression (AggStrategy (..), Expr (..))
import DataFrame.Internal.Schema (schemaType)
import qualified DataFrame.Operations.Aggregation as D
import qualified DataFrame.Operations.Core as D
import qualified DataFrame.Operations.Subset as D
import qualified DataFrame.Operations.Transformations as D
import System.Environment (getEnv)
import Data.Time.Clock (getCurrentTime, diffUTCTime)

timeIt :: String -> IO Double -> IO ()
timeIt label act = do
  forM_ [1 :: Int, 2] $ \r -> do
    t0 <- getCurrentTime
    !chk <- act
    t1 <- getCurrentTime
    putStrLn $ label ++ " run" ++ show r ++ " " ++ show (realToFrac (diffUTCTime t1 t0) :: Double) ++ "s chk=" ++ show chk

chkSumInt :: String -> DataFrame -> Double
chkSumInt col df = case D.columnAsIntVector (F.col @Int (T.pack col)) df of
  Right vec -> fromIntegral $ VU.sum vec
  Left _ -> 0.0

chkSumDbl :: String -> DataFrame -> Double
chkSumDbl col df = case D.columnAsDoubleVector (F.col @Double (T.pack col)) df of
  Right vec -> VU.sum vec
  Left _ -> 0.0

top2Sum :: Expr Double -> Expr Double
top2Sum = Agg (CollectAgg "top2Sum" f)
  where f v = Prelude.sum (take 2 (L.sortBy (flip compare) (V.toList v)))

r2Expr :: Expr Double
r2Expr =
  let n = F.toDouble (F.col @Int "n")
      sx = F.col @Double "sx"; sy = F.col @Double "sy"
      sxy = F.col @Double "sxy"; sxx = F.col @Double "sxx"; syy = F.col @Double "syy"
      num = n * sxy - sx * sy
      den = (n * sxx - sx * sx) * (n * syy - sy * sy)
   in If (F.gt den (Lit 0)) ((num * num) / den) (Lit 0)

run :: DataFrame -> [Double] -> IO Double
run df chks = do
  _ <- evaluate (D.dimensions df)
  evaluate (sum chks)

main :: IO ()
main = do
  dataName <- getEnv "SRC_DATANAME"
  let srcFile = "./data/" ++ dataName ++ ".csv"
  let id1 = F.col @Text "id1"; id2 = F.col @Text "id2"; id3 = F.col @Text "id3"
      id4 = F.col @Int "id4"; id5 = F.col @Int "id5"; id6 = F.col @Int "id6"
      v1 = F.col @Int "v1"; v2 = F.col @Int "v2"; v3 = F.col @Double "v3"
      dv1 = F.toDouble v1; dv2 = F.toDouble v2
  df <- fastReadCsvWithOpts
          (defaultReadOptions { typeSpec = SpecifyTypes
              [ (F.name id1, schemaType @Text), (F.name id2, schemaType @Text)
              , (F.name id3, schemaType @Text), (F.name id4, schemaType @Int)
              , (F.name id5, schemaType @Int), (F.name id6, schemaType @Int)
              , (F.name v1, schemaType @Int), (F.name v2, schemaType @Int)
              , (F.name v3, schemaType @Double) ] NoInference }) srcFile
  _ <- evaluate (D.dimensions df)
  putStrLn $ "loaded " ++ show (D.dimensions df)

  -- Aggregate-only timing for Q1/Q4: pre-build & force the grouping, then time aggregate.
  let g1 = D.groupBy [F.name id1] df
  _ <- evaluate (VU.sum (rowToGroup g1))
  timeIt "Q1agg" $ let r = D.aggregate [F.sum v1 `F.as` "v1_sum"] g1 in run r [chkSumInt "v1_sum" r]
  let g4 = D.groupBy [F.name id4] df
  _ <- evaluate (VU.sum (rowToGroup g4))
  timeIt "Q4agg" $ let r = D.aggregate ["v1_mean" .= F.mean v1, "v2_mean" .= F.mean v2, "v3_mean" .= F.mean v3] g4 in run r [chkSumDbl "v1_mean" r, chkSumDbl "v2_mean" r, chkSumDbl "v3_mean" r]
  timeIt "Q4grp" $ let gg = D.groupBy [F.name id4] df in (evaluate (VU.sum (rowToGroup gg)) >> pure 0)
  timeIt "Q4int" $ let r = D.aggregate ["v1_mean" .= F.mean v1, "v2_mean" .= F.mean v2] g4 in run r [chkSumDbl "v1_mean" r, chkSumDbl "v2_mean" r]

  timeIt "Q1" $ let r = (D.groupBy [F.name id1] >>> D.aggregate [F.sum v1 `F.as` "v1_sum"]) df in run r [chkSumInt "v1_sum" r]
  timeIt "Q2" $ let r = (D.groupBy [F.name id1, F.name id2] >>> D.aggregate ["v1_sum" .= F.sum v1]) df in run r [chkSumInt "v1_sum" r]
  timeIt "Q3" $ let r = (D.groupBy [F.name id3] >>> D.aggregate ["v1_sum" .= F.sum v1, "v3_mean" .= F.mean v3]) df in run r [chkSumInt "v1_sum" r, chkSumDbl "v3_mean" r]
  timeIt "Q4" $ let r = (D.groupBy [F.name id4] >>> D.aggregate ["v1_mean" .= F.mean v1, "v2_mean" .= F.mean v2, "v3_mean" .= F.mean v3]) df in run r [chkSumDbl "v1_mean" r, chkSumDbl "v2_mean" r, chkSumDbl "v3_mean" r]
  timeIt "Q5" $ let r = (D.groupBy [F.name id6] >>> D.aggregate ["v1_sum" .= F.sum v1, "v2_sum" .= F.sum v2, "v3_sum" .= F.sum v3]) df in run r [chkSumInt "v1_sum" r, chkSumInt "v2_sum" r, chkSumDbl "v3_sum" r]
  timeIt "Q6" $ let r = (D.groupBy [F.name id4, F.name id5] >>> D.aggregate ["v3_median" .= F.median v3, "v3_sd" .= F.stddev v3]) df in run r [chkSumDbl "v3_median" r, chkSumDbl "v3_sd" r]
  timeIt "Q7" $ let r = (D.groupBy [F.name id3] >>> D.aggregate ["diff" .= F.maximum v1 - F.minimum v2]) df in run r [chkSumInt "diff" r]
  timeIt "Q8" $ let r = (D.groupBy [F.name id6] >>> D.aggregate ["largest2_v3" .= top2Sum v3]) df in run r [chkSumDbl "largest2_v3" r]
  let df9 = D.derive "v2v2" (dv2*dv2) (D.derive "v1v1" (dv1*dv1) (D.derive "v1v2" (dv1*dv2) df))
  timeIt "Q9" $ let r = (D.groupBy [F.name id2, F.name id4] >>> D.aggregate ["n" .= F.count v1, "sx" .= F.sum dv1, "sy" .= F.sum dv2, "sxy" .= F.sum (F.col @Double "v1v2"), "sxx" .= F.sum (F.col @Double "v1v1"), "syy" .= F.sum (F.col @Double "v2v2")] >>> D.derive "r2" r2Expr) df9 in run r [chkSumDbl "r2" r]
  timeIt "Q10" $ let r = (D.groupBy (map ((\i n -> i <> (T.pack . show) n) "id") [1..6]) >>> D.aggregate [F.sum v3 `F.as` "v3_sum"]) df in run r [chkSumDbl "v3_sum" r]
