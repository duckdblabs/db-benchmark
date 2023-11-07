#!/usr/bin/env python

print("# groupby-modin.py", flush=True)

import gc
import timeit

exec(open("./modin/modin-helpers.py").read())

import modin as modin
import modin.pandas as pd
from modin.utils import execute

init_modin_on_hdk(pd)

exec(open("./_helpers/helpers.py").read())

ver = modin.__version__
git = ""
task = "groupby"
solution = "modin"
fun = ".groupby"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ["SRC_DATANAME"]
src_grp = os.path.join("data", data_name + ".csv")
print("loading dataset %s" % data_name, flush=True)

x = pd.read_csv(
    src_grp,
    dtype={
        "id1": "category",
        "id2": "category",
        "id3": "category",
        **{n: "int32" for n in ["id4", "id5", "id6", "v1", "v2"]},
        "v3": "float64",
    },
)
# To trigger non-lazy loading
execute(x, trigger_hdk_import=True)

gb_params = dict(as_index=False, sort=False, observed=True)

task_init = timeit.default_timer()
print("grouping...", flush=True)

question = "sum v1 by id1"  # q1
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id1"], **gb_params).agg({"v1": "sum"})
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id1"], **gb_params).agg({"v1": "sum"})
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=2,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v1 by id1:id2"  # q2
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id1", "id2"], **gb_params).agg({"v1": "sum"})
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id1", "id2"], **gb_params).agg({"v1": "sum"})
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=2,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v1 mean v3 by id3"  # q3
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id3"], **gb_params).agg({"v1": "sum", "v3": "mean"})
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v3"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id3"], **gb_params).agg({"v1": "sum", "v3": "mean"})
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v3"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=2,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "mean v1:v3 by id4"  # q4
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id4"], **gb_params).agg({"v1": "mean", "v2": "mean", "v3": "mean"})
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum(), ans["v3"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id4"], **gb_params).agg({"v1": "mean", "v2": "mean", "v3": "mean"})
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum(), ans["v3"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=2,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v1:v3 by id6"  # q5
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id6"], **gb_params).agg({"v1": "sum", "v2": "sum", "v3": "sum"})
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum(), ans["v3"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id6"], **gb_params).agg({"v1": "sum", "v2": "sum", "v3": "sum"})
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum(), ans["v3"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=2,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "median v3 sd v3 by id4 id5"  # q6
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id4", "id5"], **gb_params).agg({"v3": ["median", "std"]})
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans[("v3", "median")].sum(), ans[("v3", "std")].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id4", "id5"], **gb_params).agg({"v3": ["median", "std"]})
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans[("v3", "median")].sum(), ans[("v3", "std")].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=2,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "max v1 - min v2 by id3"  # q7
gc.collect()
t_start = timeit.default_timer()
ans = (
    x.groupby(["id3"], **gb_params)
    .agg({"v1": "max", "v2": "min"})
    .assign(range_v1_v2=lambda x: x["v1"] - x["v2"])[["id3", "range_v1_v2"]]
)
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["range_v1_v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = (
    x.groupby(["id3"], **gb_params)
    .agg({"v1": "max", "v2": "min"})
    .assign(range_v1_v2=lambda x: x["v1"] - x["v2"])[["id3", "range_v1_v2"]]
)
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["range_v1_v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=2,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "largest two v3 by id6"  # q8
gc.collect()
t_start = timeit.default_timer()
ans = (
    x.groupby("id6", sort=False, observed=True)["v3"]
    .nlargest(2)
    .reset_index()[["id6", "v3"]]
)
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v3"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = (
    x.groupby("id6", sort=False, observed=True)["v3"]
    .nlargest(2)
    .reset_index()[["id6", "v3"]]
)
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v3"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=2,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "regression v1 v2 by id2 id4"  # q9
# ans = x[['id2','id4','v1','v2']].groupby(['id2','id4']).corr().iloc[0::2][['v2']]**2 # slower, 76s vs 47s on 1e8 1e2
gc.collect()
t_start = timeit.default_timer()
from modin.experimental.sql import query

ans = query(
    "SELECT id2, id4, POWER(CORR(v1, v2), 2) AS r2 FROM df GROUP BY id2, id4;", df=x
)
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["r2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = query(
    "SELECT id2, id4, POWER(CORR(v1, v2), 2) AS r2 FROM df GROUP BY id2, id4;", df=x
)
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["r2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=2,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v3 count by id1:id6"  # q10
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id1", "id2", "id3", "id4", "id5", "id6"], **gb_params).agg(
    {"v3": "sum", "v1": "count"}
)
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v3"].sum(), ans["v1"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id1", "id2", "id3", "id4", "id5", "id6"], **gb_params).agg(
    {"v3": "sum", "v1": "count"}
)
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v3"].sum(), ans["v1"].sum()]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=x.shape[0],
    question=question,
    out_rows=ans.shape[0],
    out_cols=ans.shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=2,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk(chk),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

print(
    "grouping finished, took %0.fs" % (timeit.default_timer() - task_init), flush=True
)

exit(0)
