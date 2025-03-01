#!/usr/bin/env python3

print("# groupby-polars.py", flush=True)

import os
import gc
import timeit
import polars as pl
from polars import col

exec(open("./_helpers/helpers.py").read())

ver = pl.__version__
git = ""
task = "groupby"
solution = "polars"
fun = ".groupby"
cache = "TRUE"
on_disk = "FALSE"

spill_dir = os.environ["SPILL_DIR"] + "/polars-groupby"
os.makedirs(spill_dir, exist_ok=True)

data_name = os.environ["SRC_DATANAME"]
machine_type = os.environ["MACHINE_TYPE"]
src_grp = os.path.join("data", data_name + ".csv")
print("loading dataset %s" % data_name, flush=True)

with pl.StringCache():
    x = (pl.read_csv(src_grp, schema_overrides={"id4":pl.Int32, "id5":pl.Int32, "id6":pl.Int32, "v1":pl.Int32, "v2":pl.Int32, "v3":pl.Float64}, low_memory=True, rechunk=True)
         .with_columns(pl.col(["id1", "id2", "id3"]).cast(pl.Categorical)))

scale_factor = data_name.replace("G1_","")[:4].replace("_", "")
on_disk = 'TRUE' if (machine_type == "c6id.4xlarge" and float(scale_factor) >= 1e9) else 'FALSE'

in_rows = x.shape[0]
x.write_ipc(f"{spill_dir}/tmp.ipc")
del x
x = pl.read_ipc(f"{spill_dir}/tmp.ipc", memory_map=True)
x = x.lazy()

# materialize
print(len(x.collect()), flush=True)
in_rows = x.collect().shape[0]

task_init = timeit.default_timer()
print("grouping...", flush=True)

question = "sum v1 by id1" # q1
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by("id1").agg(pl.sum("v1").alias("v1_sum")).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1_sum"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by("id1").agg(pl.sum("v1").alias("v1_sum")).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1_sum"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v1 by id1:id2" # q2
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by(["id1","id2"]).agg(pl.sum("v1").alias("v1_sum")).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1_sum"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by(["id1","id2"]).agg(pl.sum("v1").alias("v1_sum")).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1_sum"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v1 mean v3 by id3" # q3
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by("id3").agg([pl.sum("v1").alias("v1_sum"), pl.mean("v3").alias("v3_mean")]).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v1_sum").cast(pl.Int64).sum(), pl.col("v3_mean").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by("id3").agg([pl.sum("v1").alias("v1_sum"), pl.mean("v3").alias("v3_mean")]).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v1_sum").cast(pl.Int64).sum(), pl.col("v3_mean").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "mean v1:v3 by id4" # q4
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by("id4").agg([pl.mean("v1").alias("v1_mean"), pl.mean("v2").alias("v2_mean"), pl.mean("v3").alias("v3_mean")]).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v1_mean").sum(), pl.col("v2_mean").sum(), pl.col("v3_mean").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by("id4").agg([pl.mean("v1").alias("v1_mean"), pl.mean("v2").alias("v2_mean"), pl.mean("v3").alias("v3_mean")]).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v1_mean").sum(), pl.col("v2_mean").sum(), pl.col("v3_mean").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v1:v3 by id6" # q5
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by("id6").agg([pl.sum("v1").alias("v1_sum"), pl.sum("v2").alias("v2_sum"), pl.sum("v3").alias("v3_sum")]).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v1_sum").cast(pl.Int64).sum(), pl.col("v2_sum").cast(pl.Int64).sum(), pl.col("v3_sum").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by("id6").agg([pl.sum("v1").alias("v1_sum"), pl.sum("v2").alias("v2_sum"), pl.sum("v3").alias("v3_sum")]).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v1_sum").cast(pl.Int64).sum(), pl.col("v2_sum").cast(pl.Int64).sum(), pl.col("v3_sum").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "median v3 sd v3 by id4 id5" # q6
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by(["id4","id5"]).agg([pl.median("v3").alias("v3_median"), pl.std("v3").alias("v3_std")]).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v3_median").sum(), pl.col("v3_std").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by(["id4","id5"]).agg([pl.median("v3").alias("v3_median"), pl.std("v3").alias("v3_std")]).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v3_median").sum(), pl.col("v3_std").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "max v1 - min v2 by id3" # q7
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by("id3").agg([(pl.max("v1") - pl.min("v2")).alias("range_v1_v2")]).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["range_v1_v2"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by("id3").agg([(pl.max("v1") - pl.min("v2")).alias("range_v1_v2")]).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["range_v1_v2"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "largest two v3 by id6" # q8
gc.collect()
t_start = timeit.default_timer()
ans = x.drop_nulls("v3").group_by("id6").agg(col("v3").top_k(2).alias("largest2_v3")).explode("largest2_v3").collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["largest2_v3"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.drop_nulls("v3").group_by("id6").agg(col("v3").top_k(2).alias("largest2_v3")).explode("largest2_v3").collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["largest2_v3"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "regression v1 v2 by id2 id4" # q9
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by(["id2","id4"]).agg((pl.corr("v1","v2", method="pearson")**2).alias("r2")).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["r2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by(["id2","id4"]).agg((pl.corr("v1","v2", method="pearson")**2).alias("r2")).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["r2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "sum v3 count by id1:id6" # q10
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by(["id1","id2","id3","id4","id5","id6"]).agg([pl.sum("v3").alias("v3"), pl.len().alias("count")]).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v3").sum(), pl.col("count").cast(pl.Int64).sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.group_by(["id1","id2","id3","id4","id5","id6"]).agg([pl.sum("v3").alias("v3"), pl.len().alias("count")]).collect()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v3").sum(), pl.col("count").cast(pl.Int64).sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

print("grouping finished, took %0.3fs" % (timeit.default_timer() - task_init), flush=True)

exit(0)
