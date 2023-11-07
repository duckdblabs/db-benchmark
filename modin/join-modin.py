#!/usr/bin/env python

print("# join-modin.py")

import os
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
task = "join"
solution = "modin"
fun = "merge"
cache = "TRUE"
on_disk = "FALSE"


data_name = os.environ["SRC_DATANAME"]
src_jn_x = os.path.join("data", data_name + ".csv")
y_data_name = join_to_tbls(data_name)
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

x = pd.read_csv(
    src_jn_x,
    dtype={
        **{n: "int32" for n in ["id1", "id2", "id3"]},
        **{n: "category" for n in ["id4", "id5", "id6"]},
        "v1": "float64",
    },
)

small = pd.read_csv(
    src_jn_y[0], dtype={"id1": "int32", "id4": "category", "v2": "float64"}
)
medium = pd.read_csv(
    src_jn_y[1],
    dtype={
        **{n: "int32" for n in ["id1", "id2"]},
        **{n: "category" for n in ["id4", "id5"]},
        "v2": "float64",
    },
)
big = pd.read_csv(
    src_jn_y[2],
    dtype={
        **{n: "int32" for n in ["id1", "id2", "id3"]},
        **{n: "category" for n in ["id4", "id5", "id6"]},
        "v2": "float64",
    },
)

# To trigger non-lazy loading
[execute(df, trigger_hdk_import=True) for df in [x, small, medium, big]]

task_init = timeit.default_timer()
print("joining...", flush=True)

question = "small inner on int"  # q1
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(small, on="id1")
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
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
ans = x.merge(small, on="id1")
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
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

question = "medium inner on int"  # q2
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(medium, on="id2")
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
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
ans = x.merge(medium, on="id2")
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
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

question = "medium outer on int"  # q3
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(medium, how="left", on="id2")
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
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
ans = x.merge(medium, how="left", on="id2")
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
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

question = "medium inner on factor"  # q4
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(medium, on="id5")
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
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
ans = x.merge(medium, on="id5")
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
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

question = "big inner on int"  # q5
gc.collect()
t_start = timeit.default_timer()
ans = x.merge(big, on="id3")
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
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
ans = x.merge(big, on="id3")
execute(ans)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
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

print("joining finished, took %0.fs" % (timeit.default_timer() - task_init), flush=True)

exit(0)
