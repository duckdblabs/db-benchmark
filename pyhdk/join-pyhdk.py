#!/usr/bin/env python

print("# join-pyhdk.py", flush=True)

import os
import gc
import timeit
import sys
import pyhdk

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "_helpers"))
from helpers import memory_usage, write_log, make_chk, join_to_tbls

# exec(open("./_helpers/helpers.py").read())

ver = pyhdk.__version__
git = pyhdk.__version__
task = "join"
solution = "pyhdk"
fun = ".join"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ["SRC_DATANAME"]
src_jn_x = os.path.join("data", data_name + ".csv")
y_data_name = join_to_tbls(data_name)
print("pyhdk data_name: ", data_name)
src_jn_y = [
    os.path.join("data", y_data_name[0] + ".csv"),
    os.path.join("data", y_data_name[1] + ".csv"),
    os.path.join("data", y_data_name[2] + ".csv"),
]
if len(src_jn_y) != 3:
    raise Exception("Something went wrong in preparing files used for join")

print(
    "loading datasets "
    + data_name
    + ", "
    + y_data_name[0]
    + ", "
    + y_data_name[1]
    + ", "
    + y_data_name[2],
    flush=True,
)
pyhdk_init_args = {}
# pyhdk_init_args["enable_debug_timer"] = True
# pyhdk_init_args["enable-non-lazy-data-import"] = True
pyhdk_init_args["enable_cpu_groupby_multifrag_kernels"] = False
pyhdk.initLogger(debug_logs=True)

fragment_size = int(os.environ["FRAGMENT_SIZE"])
print(f"Using fragment size {fragment_size}")

hdk = pyhdk.init(**pyhdk_init_args)
# TODO: use 32-bit integers for less memory consumption and better perf
x = hdk.import_csv(
    src_jn_x,
    schema={
        "id1": "int32",
        "id2": "int32",
        "id3": "int32",
        "id4": "dict",
        "id5": "dict",
        "id6": "dict",
        "v1": "fp64",
    },
    fragment_size=fragment_size,
)
small = hdk.import_csv(
    src_jn_y[0],
    schema={"id1": "int32", "id4": "dict", "v2": "fp64"},
    fragment_size=fragment_size,
)
medium = hdk.import_csv(
    src_jn_y[1],
    schema={"id1": "int32", "id2": "int32", "id4": "dict", "id5": "dict", "v2": "fp64"},
    fragment_size=fragment_size,
)
big = hdk.import_csv(
    src_jn_y[2],
    schema={
        "id1": "int32",
        "id2": "int32",
        "id3": "int32",
        "id4": "dict",
        "id5": "dict",
        "id6": "dict",
        "v2": "fp64",
    },
    fragment_size=fragment_size,
)
print(x.shape[0], flush=True)
print(small.shape[0], flush=True)
print(medium.shape[0], flush=True)
print(big.shape[0], flush=True)

task_init = timeit.default_timer()
print("joining...", flush=True)

question = "small inner on int"  # q1
gc.collect()
t_start = timeit.default_timer()
ans = x.join(small, "id1").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v2="sum(v2)").run().row(0)
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
ans = x.join(small, "id1").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v2="sum(v2)").run().row(0)
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
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
del ans

question = "medium inner on int"  # q2
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, "id2").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v2="sum(v2)").run().row(0)
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
ans = x.join(medium, "id2").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v2="sum(v2)").run().row(0)
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
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
del ans

question = "medium outer on int"  # q3
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, "id2", how="left").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v2="sum(v2)").run().row(0)
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
ans = x.join(medium, "id2", how="left").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v2="sum(v2)").run().row(0)
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
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
del ans

question = "medium inner on factor"  # q4
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, "id5").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v2="sum(v2)").run().row(0)
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
ans = x.join(medium, "id5").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v2="sum(v2)").run().row(0)
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
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
del ans

question = "big inner on int"  # q5
gc.collect()
t_start = timeit.default_timer()
ans = x.join(big, "id3").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v2="sum(v2)").run().row(0)
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
ans = x.join(big, "id3").run()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.agg([], v1="sum(v1)", v2="sum(v2)").run().row(0)
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
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
del ans

print("joining finished, took %0.fs" % (timeit.default_timer() - task_init), flush=True)

exit(0)
