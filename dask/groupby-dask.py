#!/usr/bin/env python3
#!/usr/bin/env python3


import os
import gc
import sys
import timeit
import pathlib
import logging

import pandas as pd
import dask as dk
import dask.dataframe as dd
from dask import distributed

try:
    from .helpers import memory_usage, write_log, make_chk
except ImportError:
    exec(open("./_helpers/helpers.py").read())


print("# groupby-dask.py", flush=True)

ver = dk.__version__
git = dk.__git_revision__
task = "groupby"
solution = "dask"
fun = ".groupby"
cache = "TRUE"

data_name = os.environ['SRC_DATANAME']
on_disk = False #data_name.split("_")[1] == "1e9" # on-disk data storage #126
fext = "parquet" if on_disk else "csv"
src_grp = os.path.join("data", data_name+"."+fext)
print("loading dataset %s" % data_name, flush=True)

na_flag = int(data_name.split("_")[3])
if na_flag > 0:
  print("skip due to na_flag>0: #171", flush=True, file=sys.stderr)
  exit(0) # not yet implemented #171, currently groupby's dropna=False argument is ignored

IN_ROWS = None

def sum_v1_by_id1(x, client):
    question = "sum v1 by id1" # q1
    gc.collect()
    t_start = timeit.default_timer()

    ans = x.groupby('id1', dropna=False, observed=True).agg({'v1':'sum'}).compute()
    ans.reset_index(inplace=True) # #68
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans.v1.sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    del ans
    gc.collect()

    t_start = timeit.default_timer()
    ans = x.groupby('id1', dropna=False, observed=True).agg({'v1':'sum'}).compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans.v1.sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    print(ans.head(3), flush=True)
    print(ans.tail(3), flush=True)
    del ans

def sum_v1_by_id1_id2(x, client):
    question = "sum v1 by id1:id2" # q2
    gc.collect()
    t_start = timeit.default_timer()
    ans = x.groupby(['id1','id2'], dropna=False, observed=True).agg({'v1':'sum'}).compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans.v1.sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    del ans
    gc.collect()

    t_start = timeit.default_timer()
    ans = x.groupby(['id1','id2'], dropna=False, observed=True).agg({'v1':'sum'}).compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans.v1.sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    print(ans.head(3), flush=True)
    print(ans.tail(3), flush=True)
    del ans

def sum_v1_mean_v3_by_id3(x, client):
    question = "sum v1 mean v3 by id3" # q3
    gc.collect()
    t_start = timeit.default_timer()
    ans = x.groupby('id3', dropna=False, observed=True).agg({'v1':'sum', 'v3':'mean'}).compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans.v1.sum(), ans.v3.sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    del ans
    gc.collect()

    t_start = timeit.default_timer()
    ans = x.groupby('id3', dropna=False, observed=True).agg({'v1':'sum', 'v3':'mean'}).compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans.v1.sum(), ans.v3.sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    print(ans.head(3), flush=True)
    print(ans.tail(3), flush=True)
    del ans

def mean_v1_v3_by_id4(x, client):
    question = "mean v1:v3 by id4" # q4
    gc.collect()
    t_start = timeit.default_timer()
    ans = x.groupby('id4', dropna=False, observed=True).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    del ans
    gc.collect()

    t_start = timeit.default_timer()
    ans = x.groupby('id4', dropna=False, observed=True).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    print(ans.head(3), flush=True)
    print(ans.tail(3), flush=True)
    del ans

def sum_v1_v3_by_id6(x, client):
    question = "sum v1:v3 by id6" # q5
    gc.collect()
    t_start = timeit.default_timer()
    ans = x.groupby('id6', dropna=False, observed=True).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    del ans
    gc.collect()

    t_start = timeit.default_timer()
    ans = x.groupby('id6', dropna=False, observed=True).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    print(ans.head(3), flush=True)
    print(ans.tail(3), flush=True)
    del ans

def median_v3_sd_v3_by_id4_id5(x, client):
    question = "median v3 sd v3 by id4 id5" # q6
    gc.collect()

    t_start = timeit.default_timer()
    ans = x.groupby(['id4','id5'], dropna=False, observed=True).agg({'v3': ['median','std']}, shuffle='p2p').compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    del ans
    gc.collect()

    t_start = timeit.default_timer()
    ans = x.groupby(['id4','id5'], dropna=False, observed=True).agg({'v3': ['median','std']}, shuffle='p2p').compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    print(ans.head(3), flush=True)
    print(ans.tail(3), flush=True)
    del ans

def max_v1_minus_min_v2_by_id3(x, client):
    question = "max v1 - min v2 by id3" # q7
    gc.collect()

    t_start = timeit.default_timer()
    ans = x.groupby('id3', dropna=False, observed=True).agg({'v1':'max', 'v2':'min'}).assign(range_v1_v2=lambda x: x['v1']-x['v2'])[['range_v1_v2']].compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans['range_v1_v2'].sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    del ans
    gc.collect()

    t_start = timeit.default_timer()
    ans = x.groupby('id3', dropna=False, observed=True).agg({'v1':'max', 'v2':'min'}).assign(range_v1_v2=lambda x: x['v1']-x['v2'])[['range_v1_v2']].compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans['range_v1_v2'].sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    print(ans.head(3), flush=True)
    print(ans.tail(3), flush=True)
    del ans

def largest_two_v3_by_id6(x, client):
    question = "largest two v3 by id6" # q8
    gc.collect()

    t_start = timeit.default_timer()
    ans = x[~x['v3'].isna()][['id6','v3']].groupby('id6', dropna=False, observed=True).apply(lambda x: x.nlargest(2, columns='v3'), meta={'id6':'Int64', 'v3':'float64'})[['v3']].compute()
    ans.reset_index(level='id6', inplace=True)
    ans.reset_index(drop=True, inplace=True) # drop because nlargest creates some extra new index field
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans['v3'].sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    del ans
    gc.collect()

    t_start = timeit.default_timer()
    ans = x[~x['v3'].isna()][['id6','v3']].groupby('id6', dropna=False, observed=True).apply(lambda x: x.nlargest(2, columns='v3'), meta={'id6':'Int64', 'v3':'float64'})[['v3']].compute()
    ans.reset_index(level='id6', inplace=True)
    ans.reset_index(drop=True, inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans['v3'].sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    print(ans.head(3), flush=True)
    print(ans.tail(3), flush=True)
    del ans

def regression_v1_v2_by_id2_id4(x, client):
    question = "regression v1 v2 by id2 id4" # q9
    gc.collect()

    t_start = timeit.default_timer()
    ans = x[['id2','id4','v1','v2']] \
        .groupby(['id2','id4'], dropna=False, observed=False)[['v1', 'v2']] \
        .apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}), meta={'r2':'float64'}) \
        .compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans['r2'].sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    del ans
    gc.collect()

    t_start = timeit.default_timer()
    ans = x[['id2','id4','v1','v2']] \
        .groupby(['id2','id4'], dropna=False, observed=False)[['v1', 'v2']] \
        .apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}), meta={'r2':'float64'}) \
        .compute()
    ans.reset_index(inplace=True)
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans['r2'].sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    print(ans.head(3), flush=True)
    print(ans.tail(3), flush=True)
    del ans

def sum_v3_count_by_id1_id6(x, client):
    question = "sum v3 count by id1:id6"  # q10
    print(question)
    gc.collect()

    t_start = timeit.default_timer()
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
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans.v3.sum(), ans["count"].sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    del ans
    gc.collect()

    t_start = timeit.default_timer()
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
    print(ans.shape, flush=True)
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans.v3.sum(), ans["count"].sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=IN_ROWS, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    print(ans.head(3), flush=True)
    print(ans.tail(3), flush=True)
    del ans


def main():
    # we use process-pool instead of thread-pool due to GIL cost
    client = distributed.Client(processes=True, silence_logs=logging.ERROR)
    print("using disk memory-mapped data storage" if on_disk else "using in-memory data storage", flush=True)
    x = dd.read_csv(
      src_grp,
      dtype={
        "id1": "category",
        "id2": "category",
        "id3": "category",
        "id4": "category",
        "id5": "category",
        "id6": "category",
        "v1": "Int32",
        "v2": "Int32",
        "v3": "float64"
      },
      engine="pyarrow"
    )
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
    largest_two_v3_by_id6(x, client)
    regression_v1_v2_by_id2_id4(x, client)
    sum_v3_count_by_id1_id6(x, client)

    print("grouping finished, took %0.fs" % (timeit.default_timer()-task_init), flush=True)
    exit(0)

if __name__ == "__main__":
    main()

