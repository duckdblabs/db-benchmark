#!/usr/bin/env python3

print("# groupby-dask.py", flush=True)

import os
import gc
import sys
import timeit
import pandas as pd
import dask as dk
import dask.dataframe as dd
import logging
from abc import ABC, abstractmethod
from dask import distributed
from typing import Any

exec(open("./_helpers/helpers.py").read())

# TODO: Case
ver = dk.__version__
git = dk.__git_revision__
task = "groupby"
solution = "dask"
fun = ".groupby"
cache = "TRUE"

def dask_client() -> distributed.Client:
    # we use process-pool instead of thread-pool due to GIL cost
    return distributed.Client(processes=True, silence_logs=logging.ERROR)

def load_dataset(src_grp: str) -> dd.DataFrame:
    print("loading dataset %s" % data_name, flush=True)
    x = dd.read_csv(
        src_grp,
        dtype={"id1":"category","id2":"category","id3":"category","id4":"Int32","id5":"Int32","id6":"Int32","v1":"Int32","v2":"Int32","v3":"float64"},
        engine="pyarrow"
    )
    x = x.persist()
    return x

class Query(ABC):
    @staticmethod
    @abstractmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        pass

    @staticmethod
    @abstractmethod
    def check(ans: dd.DataFrame) -> Any:
        pass

class QueryOne(Query):
    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x.groupby('id1', dropna=False, observed=True).agg({'v1':'sum'}).compute()
        ans.reset_index(inplace=True) # #68
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans.v1.sum()]

class QueryTwo(Query):
    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x.groupby(['id1','id2'], dropna=False, observed=True).agg({'v1':'sum'}).compute()
        ans.reset_index(inplace=True) # #68
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans.v1.sum()]

class QueryThree(Query):
    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x.groupby('id3', dropna=False, observed=True).agg({'v1':'sum', 'v3':'mean'}).compute()
        ans.reset_index(inplace=True) # #68
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans.v1.sum(), ans.v3.sum()]

class QueryFour(Query):
    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x.groupby('id4', dropna=False, observed=True).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()
        ans.reset_index(inplace=True) # #68
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]

class QueryFive(Query):
    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x.groupby('id6', dropna=False, observed=True).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()
        ans.reset_index(inplace=True) # #68
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]

class QuerySix(Query):
    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x.groupby(['id4','id5'], dropna=False, observed=True).agg({'v3': ['median','std']}, shuffle='p2p').compute()
        ans.reset_index(inplace=True) # #68
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans['v3']['median'].sum(), ans['v3']['std'].sum()]

class QuerySeven(Query):
    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x.groupby('id3', dropna=False, observed=True).agg({'v1':'max', 'v2':'min'}).assign(range_v1_v2=lambda x: x['v1']-x['v2'])[['range_v1_v2']].compute()
        ans.reset_index(inplace=True) # #68
        return ans


    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans['range_v1_v2'].sum()]

class QueryEight(Query):
    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x[~x['v3'].isna()][['id6','v3']].groupby('id6', dropna=False, observed=True).apply(lambda x: x.nlargest(2, columns='v3'), meta={'id6':'Int64', 'v3':'float64'})[['v3']].compute()
        ans.reset_index(level='id6', inplace=True)
        ans.reset_index(drop=True, inplace=True) # drop because nlargest creates some extra new index field
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans['v3'].sum()]

class QueryNine(Query):
    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x[['id2','id4','v1','v2']].groupby(['id2','id4'], dropna=False, observed=True).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}), meta={'r2':'float64'}).compute()
        ans.reset_index(inplace=True)
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans['r2'].sum()]

class QueryTen(Query):
    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = (
            x.groupby(
                ['id1', 'id2', 'id3', 'id4', 'id5', 'id6'],
                dropna=False,
                observed=True,
            )
            .agg({'v3': 'sum', 'v1': 'size'}, split_out=x.npartitions)
            .rename(columns={"v1": "count"})
            .compute()
        )
        ans.reset_index(inplace=True)
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans.v3.sum(), ans["count"].sum()]

def run_query(
    data_name: str,
    in_rows: int,
    x: dd.DataFrame,
    query: Query,
    question: str,
    runs: int = 2,
):
    try:
        for run in range(1, runs+1):
            gc.collect() # TODO: Able to do this in worker processes? Want to?

            # Calculate ans
            t_start = timeit.default_timer()
            ans = query.query(x)
            print(ans.shape, flush=True)
            t = timeit.default_timer() - t_start
            m = memory_usage()

            # Calculate chk
            t_start = timeit.default_timer()
            chk = query.check(ans)
            chkt = timeit.default_timer() - t_start


            write_log(
                task=task,
                data=data_name,
                in_rows=in_rows,
                question=question,
                out_rows=ans.shape[0],
                out_cols=ans.shape[1],
                solution=solution,
                version=ver,
                git=git,
                fun=fun,
                run=run,
                time_sec=t,
                mem_gb=m,
                cache=cache,
                chk=make_chk(chk),
                chk_time_sec=chkt,
                on_disk=on_disk
            )
            if run == runs:
                # Print head / tail on last run
                print(ans.head(3), flush=True)
                print(ans.tail(3), flush=True)
            del ans
    except Exception as err:
        print("ERROR: Query '%s' failed!" % question)
        print(err)

def run_task(
    data_name: str,
    src_grp: str,
):
    client = dask_client()
    x = load_dataset(src_grp)
    in_rows = len(x)
    print(in_rows, flush=True)

    task_init = timeit.default_timer()
    print("grouping...", flush=True)

    run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryOne,
        question="sum v1 by id1", # q1
    )

    run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryTwo,
        question="sum v1 by id1:id2", # q2
    )

    run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryThree,
        question="sum v1 mean v3 by id3", # q3
    )

    run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryFour,
        question="mean v1:v3 by id4", # q4
    )

    run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryFive,
        question= "sum v1:v3 by id6", # q5
    )

    run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QuerySix,
        question="median v3 sd v3 by id4 id5", # q6
    )

    run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QuerySeven,
        question="max v1 - min v2 by id3", # q7
    )

    run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryEight,
        question="largest two v3 by id6", # q8
    )

    run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryNine,
        question="regression v1 v2 by id2 id4", # q9
    )

    run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryTen,
        question= "sum v3 count by id1:id6", # q10
    )

    print("grouping finished, took %0.fs" % (timeit.default_timer()-task_init), flush=True)

if __name__ == '__main__':
    data_name = os.environ['SRC_DATANAME']
    on_disk = False #data_name.split("_")[1] == "1e9" # on-disk data storage #126
    fext = "parquet" if on_disk else "csv"
    src_grp = os.path.join("data", data_name+"."+fext)

    na_flag = int(data_name.split("_")[3])
    if na_flag > 0:
        print("skip due to na_flag>0: #171", flush=True, file=sys.stderr)
        exit(0) # not yet implemented #171, currently groupby's dropna=False argument is ignored

    run_task(
        data_name=data_name,
        src_grp=src_grp
    )