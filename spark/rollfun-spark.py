#!/usr/bin/env python3

print("# rollfun-spark.py", flush=True)

import os
import gc
import timeit
import pyspark
from pyspark.sql import SparkSession

exec(open("./_helpers/helpers.py").read())

ver = pyspark.__version__
git = "" # won't fix: https://issues.apache.org/jira/browse/SPARK-16864
task = "groupby"
solution = "spark"
fun = ".sql"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ['SRC_DATANAME']
src_grp = os.path.join("data", data_name+".csv")
print("loading dataset %s" % data_name, flush=True)

mem_usage = "100g"
if "TEST_RUN" in os.environ:
     mem_usage = "2g"

from pyspark.conf import SparkConf
spark = SparkSession.builder \
     .master("local[*]") \
     .appName("groupby-spark") \
     .config("spark.executor.memory", mem_usage) \
     .config("spark.driver.memory", mem_usage) \
     .config("spark.python.worker.memory", mem_usage) \
     .config("spark.driver.maxResultSize", mem_usage) \
     .config("spark.network.timeout", "2400") \
     .config("spark.executor.heartbeatInterval", "1200") \
     .config("spark.ui.showConsoleProgress", "false") \
     .getOrCreate()
#print(spark.sparkContext._conf.getAll(), flush=True)

x = spark.read.csv(src_grp, header=True, inferSchema='true').persist(pyspark.StorageLevel.MEMORY_ONLY)

print(x.count(), flush=True)

x.createOrReplaceTempView("x")

# window size
w = int(x.count()/1e3)
wsmall = int(x.count()/1e4)
wbig = int(x.count()/1e2)

task_init = timeit.default_timer()
print("rolling...", flush=True)

question = "mean" # q1
sql = f'select avg(v1) over (order by id1 rows between {w-1} preceding and current row) as v1 from x'
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans, sql

question = "window small" # q2
sql = f'select avg(v1) over (order by id1 rows between {wsmall-1} preceding and current row) as v1 from x'
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans, sql

question = "window big" # q3
sql = f'select avg(v1) over (order by id1 rows between {wbig-1} preceding and current row) as v1 from x'
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans, sql

question = "min" # q4
sql = f'select min(v1) over (order by id1 rows between {w-1} preceding and current row) as v1 from x'
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans, sql

#question = "median" # q5
#sql = f'select median(v1) over (order by id1 rows between {w-1} preceding and current row) as v1 from x'
#gc.collect()
#t_start = timeit.default_timer()
#ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
#print((ans.count(), len(ans.columns)), flush=True) # shape
#t = timeit.default_timer() - t_start
#m = memory_usage()
#ans.createOrReplaceTempView("ans")
#t_start = timeit.default_timer()
#chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#ans.unpersist()
#spark.catalog.uncacheTable("ans")
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
#print((ans.count(), len(ans.columns)), flush=True) # shape
#t = timeit.default_timer() - t_start
#m = memory_usage()
#ans.createOrReplaceTempView("ans")
#t_start = timeit.default_timer()
#chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(ans.head(3), flush=True)
#print(ans.tail(3), flush=True)
#ans.unpersist()
#spark.catalog.uncacheTable("ans")
#del ans, sql

question = "multiroll" # q6
sql = f'select avg(v1) over (order by id1 rows between {w-51} preceding and current row) as v1_small, avg(v1) over (order by id1 rows between {w+49} preceding and current row) as v1_big, avg(v2) over (order by id1 rows between {w-51} preceding and current row) as v2_small, avg(v2) over (order by id1 rows between {w+49} preceding and current row) as v2_big from x'
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1_small) as v1_small, sum(v1_big) as v1_big, sum(v2_small) as v2_small, sum(v2_big) as v2_big from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1_small) as v1_small, sum(v1_big) as v1_big, sum(v2_small) as v2_small, sum(v2_big) as v2_big from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans, sql

#question = "weighted" # q7 ## not yet implemented
#sql = f'select avg(v1) over (order by id1 rows between {w-1} preceding and current row) as v1 from x'
#gc.collect()
#t_start = timeit.default_timer()
#ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
#print((ans.count(), len(ans.columns)), flush=True) # shape
#t = timeit.default_timer() - t_start
#m = memory_usage()
#ans.createOrReplaceTempView("ans")
#t_start = timeit.default_timer()
#chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#ans.unpersist()
#spark.catalog.uncacheTable("ans")
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
#print((ans.count(), len(ans.columns)), flush=True) # shape
#t = timeit.default_timer() - t_start
#m = memory_usage()
#ans.createOrReplaceTempView("ans")
#t_start = timeit.default_timer()
#chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(ans.head(3), flush=True)
#print(ans.tail(3), flush=True)
#ans.unpersist()
#spark.catalog.uncacheTable("ans")
#del ans, sql

question = "uneven dense" # q8
sql = f'select avg(v1) over (order by id2 range between {w-1} preceding and current row) as v1 from x'
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans, sql

question = "uneven sparse" # q9
sql = f'select avg(v1) over (order by id3 range between {w-1} preceding and current row) as v1 from x'
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans
gc.collect()
t_start = timeit.default_timer()
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans.count(), len(ans.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans, sql

#question = "regression"
#sql = f'select avg(v1) over (order by id1 rows between {w-1} preceding and current row) as v1 from x'
#gc.collect()
#t_start = timeit.default_timer()
#ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
#print((ans.count(), len(ans.columns)), flush=True) # shape
#t = timeit.default_timer() - t_start
#m = memory_usage()
#ans.createOrReplaceTempView("ans")
#t_start = timeit.default_timer()
#chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#ans.unpersist()
#spark.catalog.uncacheTable("ans")
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
#print((ans.count(), len(ans.columns)), flush=True) # shape
#t = timeit.default_timer() - t_start
#m = memory_usage()
#ans.createOrReplaceTempView("ans")
#t_start = timeit.default_timer()
#chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(ans.head(3), flush=True)
#print(ans.tail(3), flush=True)
#ans.unpersist()
#spark.catalog.uncacheTable("ans")
#del ans, sql

spark.stop()

print("rolling finished, took %0.fs" % (timeit.default_timer()-task_init), flush=True)

exit(0)
