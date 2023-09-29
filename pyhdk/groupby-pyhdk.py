#!/usr/bin/env python

print("# groupby-pyhdk.py", flush=True)

import os
import gc
import sys
import timeit
import pyhdk

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "_helpers"))
from helpers import memory_usage, write_log, make_chk

# exec(open("./_helpers/helpers.py").read())

ver = pyhdk.__version__
git = pyhdk.__version__
task = "groupby"
solution = "pyhdk"
fun = ".groupby"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ["SRC_DATANAME"]
src_grp = os.path.join("data", data_name + ".csv")
print("loading dataset %s" % data_name, flush=True)

hdk = pyhdk.init(enable_cpu_groupby_multifrag_kernels=False)
x = hdk.import_csv(
    src_grp,
    schema={
        "id1": "dict",
        "id2": "dict",
        "id3": "dict",
        "id4": "int32",
        "id5": "int32",
        "id6": "int32",
        "v1": "int32",
        "v2": "int32",
        "v3": "fp64",
    },
)
# TODO: use warm-up SQL query if using SQL in bench

task_init = timeit.default_timer()
print("grouping...", flush=True)

question = "sum v1 by id1"  # q1
gc.collect()
t_start = timeit.default_timer()
ans = x.agg("id1", v1="sum(v1)").run()
t = timeit.default_timer() - t_start
print(ans.shape, flush=True)
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)").run().row(0)
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
ans = x.agg("id1", v1="sum(v1)").run()
t = timeit.default_timer() - t_start
print(ans.shape, flush=True)
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)").run().row(0)
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
ans = x.agg(["id1", "id2"], v1="sum(v1)").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)").run().row(0)
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
ans = x.agg(["id1", "id2"], v1="sum(v1)").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)").run().row(0)
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
ans = x.agg("id3", v1="sum(v1)", v3="avg(v3)").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v3="sum(v3)").run().row(0)
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
ans = x.agg("id3", v1="sum(v1)", v3="avg(v3)").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v3="sum(v3)").run().row(0)
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
ans = x.agg("id4", v1="avg(v1)", v2="avg(v2)", v3="avg(v3)").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v2="sum(v2)", v3="sum(v3)").run().row(0)
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
ans = x.agg("id4", v1="avg(v1)", v2="avg(v2)", v3="avg(v3)").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v2="sum(v2)", v3="sum(v3)").run().row(0)
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
ans = x.agg("id6", v1="sum(v1)", v2="sum(v2)", v3="sum(v3)").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v2="sum(v2)", v3="sum(v3)").run().row(0)
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
ans = x.agg("id6", v1="sum(v1)", v2="sum(v2)", v3="sum(v3)").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v2="sum(v2)", v3="sum(v3)").run().row(0)
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
ans = x.agg(
    ["id4", "id5"], v3_median="approx_quantile(v3, 0.5)", v3_stddev="stddev(v3)"
).run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], "sum(v3_median)", "sum(v3_stddev)").run().row(0)
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
ans = x.agg(
    ["id4", "id5"], v3_median="approx_quantile(v3, 0.5)", v3_stddev="stddev(v3)"
).run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], "sum(v3_median)", "sum(v3_stddev)").run().row(0)
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
tmp = x.agg("id3", "max(v1)", "min(v2)")
ans = tmp.proj("id3", range_v1_v2=tmp["v1_max"] - tmp["v2_min"]).run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], range_v1_v2="sum(range_v1_v2)").run().row(0)
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
tmp = x.agg("id3", "max(v1)", "min(v2)")
ans = tmp.proj("id3", range_v1_v2=tmp["v1_max"] - tmp["v2_min"]).run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], range_v1_v2="sum(range_v1_v2)").run().row(0)
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
tmp = x.proj(
    "id6",
    "v3",
    row_no=hdk.row_number().over(x.ref("id6")).order_by((x.ref("v3"), "desc")),
)
ans = tmp.filter(tmp.ref("row_no") < 3).proj("id6", "v3").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], "sum(v3)").run().row(0)
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
tmp = x.proj(
    "id6",
    "v3",
    row_no=hdk.row_number().over(x.ref("id6")).order_by((x.ref("v3"), "desc")),
)
ans = tmp.filter(tmp.ref("row_no") < 3).proj("id6", "v3").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], "sum(v3)").run().row(0)
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
gc.collect()
t_start = timeit.default_timer()
tmp = x.agg(["id2", "id4"], r2="corr(v1, v2)")
ans = tmp.proj(r2=tmp["r2"] * tmp["r2"]).run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], r2="sum(r2)").run().row(0)
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
tmp = x.agg(["id2", "id4"], r2="corr(v1, v2)")
ans = tmp.proj(r2=tmp["r2"] * tmp["r2"]).run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], r2="sum(r2)").run().row(0)
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
ans = x.agg(["id1", "id2", "id3", "id4", "id5", "id6"], v3="sum(v3)", v1="count").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v3="sum(v3)", v1="sum(v1)").run().row(0)
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
ans = x.agg(["id1", "id2", "id3", "id4", "id5", "id6"], v3="sum(v3)", v1="count").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v3="sum(v3)", v1="sum(v1)").run().row(0)
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
