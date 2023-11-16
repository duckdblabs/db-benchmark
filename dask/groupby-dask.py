#!/usr/bin/env python3

import os
import gc
import functools
import sys
import timeit
import logging

import pandas as pd
import dask as dk
import dask.dataframe as dd
from dask import distributed

try:
    from .helpers import memory_usage, write_log, make_chk
except ImportError:
    exec(open("./_helpers/helpers.py").read())


VER = dk.__version__
GIT = dk.__git_revision__
TASK = "groupby"
SOLUTION = "dask"
FUN = ".groupby"
CACHE = "TRUE"

DATA_NAME = os.environ["SRC_DATANAME"]
ON_DISK = False  # data_name.split("_")[1] == "1e9" # on-disk data storage #126
FEXT = "parquet" if ON_DISK else "csv"
SRC_GRP = os.path.join("data", DATA_NAME + "." + FEXT)

NA_FLAG = int(DATA_NAME.split("_")[3])
if NA_FLAG > 0:
    print("skip due to na_flag>0: #171", flush=True, file=sys.stderr)
    exit(
        0
    )  # not yet implemented #171, currently groupby's dropna=False argument is ignored

IN_ROWS = None


def bench(question):
    def _decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            print(f"{'-' * 10} {question} {'-' * 10}", flush=True)
            for run in range(1, 3):
                gc.collect()
                t_start = timeit.default_timer()

                result = f(*args, **kwargs)
                print(result.shape, flush=True)

                t = timeit.default_timer() - t_start
                m = memory_usage()

                t_start = timeit.default_timer()
                chk = [
                    result[c].sum()
                    for c in result.columns
                    if (isinstance(c, str) and c.startswith("v"))
                    or isinstance(c, tuple)
                    and c[0].startswith("v")
                ]
                chkt = timeit.default_timer() - t_start

                write_log(
                    task=TASK,
                    data=DATA_NAME,
                    in_rows=IN_ROWS,
                    question=question,
                    out_rows=result.shape[0],
                    out_cols=result.shape[1],
                    solution=SOLUTION,
                    version=VER,
                    git=GIT,
                    fun=FUN,
                    run=run,
                    time_sec=t,
                    mem_gb=m,
                    cache=CACHE,
                    chk=make_chk(chk),
                    chk_time_sec=chkt,
                    on_disk=ON_DISK,
                )
                if run == 2:
                    print(result.head(3), flush=True)
                    print(result.tail(3), flush=True)
                del result

        return wrapper

    return _decorator


@bench("sum v1 by id1")  # q1
def sum_v1_by_id1(x, client):
    ans = x.groupby("id1", dropna=False, observed=True).agg({"v1": "sum"}).compute()
    ans.reset_index(inplace=True)  # #68
    return ans


@bench("sum v1 by id1:id2")  # q2
def sum_v1_by_id1_id2(x, client):
    ans = (
        x.groupby(["id1", "id2"], dropna=False, observed=True)
        .agg({"v1": "sum"})
        .compute()
    )
    ans.reset_index(inplace=True)
    return ans


@bench("sum v1 mean v3 by id3")  # q3
def sum_v1_mean_v3_by_id3(x, client):
    ans = (
        x.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "sum", "v3": "mean"})
        .compute()
    )
    ans.reset_index(inplace=True)
    return ans


@bench("mean v1:v3 by id4")  # q4
def mean_v1_v3_by_id4(x, client):
    ans = (
        x.groupby("id4", dropna=False, observed=True)
        .agg({"v1": "mean", "v2": "mean", "v3": "mean"})
        .compute()
    )
    ans.reset_index(inplace=True)
    return ans


@bench("sum v1:v3 by id6")  # q5
def sum_v1_v3_by_id6(x, client):
    ans = (
        x.groupby("id6", dropna=False, observed=True)
        .agg({"v1": "sum", "v2": "sum", "v3": "sum"})
        .compute()
    )
    ans.reset_index(inplace=True)
    return ans


@bench("median v3 sd v3 by id4 id5")  # q6
def median_v3_sd_v3_by_id4_id5(x, client):
    ans = (
        x.groupby(["id4", "id5"], dropna=False, observed=True)
        .agg({"v3": ["median", "std"]}, shuffle="p2p")
        .compute()
    )
    ans.reset_index(inplace=True)
    return ans


@bench("max v1 - min v2 by id3")  # q7
def max_v1_minus_min_v2_by_id3(x, client):
    ans = (
        x.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "max", "v2": "min"})
        .assign(range_v1_v2=lambda x: x["v1"] - x["v2"])[["range_v1_v2"]]
        .compute()
    )
    ans.reset_index(inplace=True)
    return ans


@bench("largest two v3 by id6")  # q8
def largest_two_v3_by_id6(x, client):
    ans = (
        x[~x["v3"].isna()][["id6", "v3"]]
        .groupby("id6", dropna=False, observed=True)
        .apply(
            lambda x: x.nlargest(2, columns="v3"),
            meta={"id6": "Int64", "v3": "float64"},
        )[["v3"]]
        .compute()
    )
    ans.reset_index(level="id6", inplace=True)
    ans.reset_index(
        drop=True, inplace=True
    )  # drop because nlargest creates some extra new index field
    return ans


@bench("regression v1 v2 by id2 id4")  # q9
def regression_v1_v2_by_id2_id4(x, client):
    ans = (
        x[["id2", "id4", "v1", "v2"]]
        .groupby(["id2", "id4"], dropna=False, observed=False)[["v1", "v2"]]
        .apply(
            lambda x: pd.Series({"r2": x.corr()["v1"]["v2"] ** 2}),
            meta={"r2": "float64"},
        )
        .compute()
    )
    ans.reset_index(inplace=True)
    return ans


@bench("sum v3 count by id1:id6")  # q10
def sum_v3_count_by_id1_id6(x, client):
    ans = (
        x.groupby(
            ["id1", "id2", "id3", "id4", "id5", "id6"],
            dropna=False,
            observed=True,
        )
        .agg({"v3": "sum", "v1": "size"}, split_out=x.npartitions)
        .rename(columns={"v1": "count"})
        .compute()
    )
    ans.reset_index(inplace=True)
    return ans


def main():
    print("# groupby-dask.py", flush=True)

    # we use process-pool instead of thread-pool due to GIL cost
    from distributed import LocalCluster
    with LocalCluster(processes=True, silence_logs=logging.ERROR) as cluster:
        with cluster.get_client() as client:
            print(
                "using disk memory-mapped data storage"
                if ON_DISK
                else "using in-memory data storage",
                flush=True,
            )

            print("loading dataset %s" % DATA_NAME, flush=True)
            x = dd.read_csv(SRC_GRP, engine="pyarrow").persist()

            global IN_ROWS
            IN_ROWS = len(x)
            print(IN_ROWS, flush=True)

            print("grouping...", flush=True)
            task_init = timeit.default_timer()

            sum_v1_by_id1(x, client)
            sum_v1_by_id1_id2(x, client)
            sum_v1_mean_v3_by_id3(x, client)
            mean_v1_v3_by_id4(x, client)
            sum_v1_v3_by_id6(x, client)
            median_v3_sd_v3_by_id4_id5(x, client)
            max_v1_minus_min_v2_by_id3(x, client)
            # Missing API: SeriesGroupBy.nlargest
            #largest_two_v3_by_id6(x, client)  
            regression_v1_v2_by_id2_id4(x, client)
            sum_v3_count_by_id1_id6(x, client)

            print(
                "grouping finished, took %0.fs" % (timeit.default_timer() - task_init),
                flush=True,
            )


if __name__ == "__main__":
    main()
