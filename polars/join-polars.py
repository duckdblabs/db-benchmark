#!/usr/bin/env python3

print("# join-polars.py", flush=True)

import os
import gc
import timeit
import contextlib
import polars as pl

exec(open("./_helpers/helpers.py").read())

ver = pl.__version__
task = "join"
git = ""
solution = "polars"
fun = ".join"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ["SRC_DATANAME"]
machine_type = os.environ["MACHINE_TYPE"]
src_jn_x = os.path.join("data", data_name + ".csv")
y_data_name = join_to_tbls(data_name)
src_jn_y = [os.path.join("data", y_data_name[0] + ".csv"), os.path.join("data", y_data_name[1] + ".csv"), os.path.join("data", y_data_name[2] + ".csv")]
if len(src_jn_y) != 3:
  raise Exception("Something went wrong in preparing files used for join")


scale_factor = data_name.replace("J1_","")[:4].replace("_", "")
on_disk = 'TRUE' if (machine_type == "c6id.4xlarge" and float(scale_factor) >= 1e9) else 'FALSE'

print("loading datasets " + data_name + ", " + y_data_name[0] + ", " + y_data_name[2] + ", " + y_data_name[2], flush=True)

spill_dir = os.environ["SPILL_DIR"] + "/polars-join"
os.makedirs(spill_dir, exist_ok=True)

# Stream each CSV straight to IPC without ever materializing the full frame in RAM.
# sink_ipc runs the streaming engine, so peak memory stays bounded regardless of file size.
# All four sinks share one StringCache so the Categorical id columns are encoded consistently
# across tables for the joins below. compression="uncompressed" keeps the IPC files zero-copy
# mmap-able for the queries.
# Newer polars encodes Categoricals globally (pl.Categories) and deprecates StringCache.
with (contextlib.nullcontext() if hasattr(pl, "Categories") else pl.StringCache()):
  (pl.scan_csv(src_jn_x, schema_overrides={"id1":pl.Int32, "id2":pl.Int32, "id3":pl.Int32, "v1":pl.Float32})
     .with_columns(pl.col(["id4", "id5", "id6"]).cast(pl.Categorical))
     .sink_ipc(f"{spill_dir}/x.ipc", compression="uncompressed"))
  (pl.scan_csv(src_jn_y[0], schema_overrides={"id1":pl.Int32, "v2":pl.Float32})
     .with_columns(pl.col("id4").cast(pl.Categorical))
     .sink_ipc(f"{spill_dir}/small.ipc", compression="uncompressed"))
  (pl.scan_csv(src_jn_y[1], schema_overrides={"id1":pl.Int32, "id2":pl.Int32, "v2":pl.Float32})
     .with_columns(pl.col(["id4", "id5"]).cast(pl.Categorical))
     .sink_ipc(f"{spill_dir}/medium.ipc", compression="uncompressed"))
  (pl.scan_csv(src_jn_y[2], schema_overrides={"id1":pl.Int32, "id2":pl.Int32, "id3":pl.Int32, "v2":pl.Float32})
     .with_columns(pl.col(["id4", "id5", "id6"]).cast(pl.Categorical))
     .sink_ipc(f"{spill_dir}/big.ipc", compression="uncompressed"))

# Keep everything lazy and backed by the memory-mapped IPC files; the joins below collect with
# the streaming engine so join state can spill to disk instead of exhausting RAM.
x = pl.scan_ipc(f"{spill_dir}/x.ipc")
small = pl.scan_ipc(f"{spill_dir}/small.ipc")
medium = pl.scan_ipc(f"{spill_dir}/medium.ipc")
big = pl.scan_ipc(f"{spill_dir}/big.ipc")

# materialize row counts without pulling the whole frames into memory
in_rows = x.select(pl.len()).collect(engine="streaming").item()
print(in_rows, flush=True)
print(small.select(pl.len()).collect(engine="streaming").item(), flush=True)
print(medium.select(pl.len()).collect(engine="streaming").item(), flush=True)
print(big.select(pl.len()).collect(engine="streaming").item(), flush=True)

task_init = timeit.default_timer()
print("joining...", flush=True)

question = "small inner on int" # q1
gc.collect()
t_start = timeit.default_timer()
ans = x.join(small, on="id1").collect(engine="streaming")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(small, on="id1").collect(engine="streaming")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "medium inner on int" # q2
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, on="id2").collect(engine="streaming")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, on="id2").collect(engine="streaming")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "medium outer on int" # q3
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, how="left", on="id2").collect(engine="streaming")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, how="left", on="id2").collect(engine="streaming")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "medium inner on factor" # q4
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, on="id5").collect(engine="streaming")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, on="id5").collect(engine="streaming")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "big inner on int" # q5
gc.collect()
t_start = timeit.default_timer()
ans = x.join(big, on="id3").collect(engine="streaming")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(big, on="id3").collect(engine="streaming")
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

print("joining finished, took %0.fs" % (timeit.default_timer() - task_init), flush=True)

exit(0)
