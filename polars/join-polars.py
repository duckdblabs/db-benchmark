#!/usr/bin/env python3

print("# join-polars.py", flush=True)

import os
import gc
import timeit
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

with pl.StringCache():
  x = (pl.read_csv(src_jn_x, schema_overrides={"id1":pl.Int32, "id2":pl.Int32, "id3":pl.Int32, "v1":pl.Float32}, rechunk=True)
       .with_columns(
      pl.col(["id4", "id5", "id6"]).cast(pl.Categorical)
  )
   )
  small = pl.read_csv(src_jn_y[0], schema_overrides={"id1":pl.Int32, "v2":pl.Float32}, rechunk=True)
  small = small.with_columns(
    pl.col("id4").cast(pl.Categorical)
  )
  medium = (pl.read_csv(src_jn_y[1], schema_overrides={"id1":pl.Int32, "id2":pl.Int32, "v2":pl.Float32}, rechunk=True)
           .with_columns(
            pl.col(["id4", "id5"]).cast(pl.Categorical),
  ))
  big = (pl.read_csv(src_jn_y[2], schema_overrides={"id1":pl.Int32, "id2":pl.Int32, "id3":pl.Int32, "v2":pl.Float32}, rechunk=True)
         .with_columns(
    pl.col(["id4", "id5", "id6"]).cast(pl.Categorical)
  ))

print(len(x), flush=True)
print(len(small), flush=True)
print(len(medium), flush=True)
print(len(big), flush=True)

spill_dir = os.environ["SPILL_DIR"] + "/polars-join"
os.makedirs(spill_dir, exist_ok=True)

with pl.StringCache():
  x.write_ipc(f"{spill_dir}/x.ipc")
  del x
  x = pl.read_ipc(f"{spill_dir}/x.ipc") 
  x = x.lazy()

  small.write_ipc(f"{spill_dir}/small.ipc")
  del small
  small = pl.read_ipc(f"{spill_dir}/small.ipc")
  small = small.lazy()

  medium.write_ipc(f"{spill_dir}/medium.ipc")
  del medium
  medium = pl.read_ipc(f"{spill_dir}/medium.ipc")
  medium = medium.lazy()

  big.write_ipc(f"{spill_dir}/big.ipc")
  del big
  big = pl.read_ipc(f"{spill_dir}/big.ipc")
  big = big.lazy()

# materialize
print(len(x.collect()), flush=True)
print(len(small.collect()), flush=True)
print(len(medium.collect()), flush=True)
print(len(big.collect()), flush=True)

in_rows = x.collect().shape[0]

task_init = timeit.default_timer()
print("joining...", flush=True)

question = "small inner on int" # q1
gc.collect()
t_start = timeit.default_timer()
ans = x.join(small, on="id1").collect()
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
ans = x.join(small, on="id1").collect()
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
ans = x.join(medium, on="id2").collect()
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
ans = x.join(medium, on="id2").collect()
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
ans = x.join(medium, how="left", on="id2").collect()
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
ans = x.join(medium, how="left", on="id2").collect()
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
ans = x.join(medium, on="id5").collect()
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
ans = x.join(medium, on="id5").collect()
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
ans = x.join(big, on="id3").collect()
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
ans = x.join(big, on="id3").collect()
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
