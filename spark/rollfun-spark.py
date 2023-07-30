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
task = "rollfun"
solution = "spark"
fun = "over"
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
     .appName("rollfun-spark") \
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
sql0 = f'select id1, avg(v1) over (order by id1 rows between {w-1} preceding and current row) as v1 from x'
sql = f'select case when id1<{w} then null else v1 end as v1 from ans' ## handling partial window? https://stackoverflow.com/q/76799677/2490497
gc.collect()
t_start = timeit.default_timer()
ans0 = spark.sql(sql0).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans0.count(), len(ans0.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans0.createOrReplaceTempView("ans")
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
ansdo = [ans.count(), len(ans.columns)]
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans0.unpersist()
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans0, ans, ansdo
gc.collect()
t_start = timeit.default_timer()
ans0 = spark.sql(sql0).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans0.count(), len(ans0.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans0.createOrReplaceTempView("ans")
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
ansdo = [ans.count(), len(ans.columns)]
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans0.unpersist()
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans0, ans, ansdo, sql0, sql

question = "window small" # q2
sql0 = f'select id1, avg(v1) over (order by id1 rows between {wsmall-1} preceding and current row) as v1 from x'
sql = f'select case when id1<{wsmall} then null else v1 end as v1 from ans'
gc.collect()
t_start = timeit.default_timer()
ans0 = spark.sql(sql0).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans0.count(), len(ans0.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans0.createOrReplaceTempView("ans")
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
ansdo = [ans.count(), len(ans.columns)]
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans0.unpersist()
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans0, ans, ansdo
gc.collect()
t_start = timeit.default_timer()
ans0 = spark.sql(sql0).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans0.count(), len(ans0.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans0.createOrReplaceTempView("ans")
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
ansdo = [ans.count(), len(ans.columns)]
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans0.unpersist()
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans0, ans, ansdo, sql0, sql

question = "window big" # q3
sql0 = f'select id1, avg(v1) over (order by id1 rows between {wbig-1} preceding and current row) as v1 from x'
sql = f'select case when id1<{wbig} then null else v1 end as v1 from ans'
gc.collect()
t_start = timeit.default_timer()
ans0 = spark.sql(sql0).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans0.count(), len(ans0.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans0.createOrReplaceTempView("ans")
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
ansdo = [ans.count(), len(ans.columns)]
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans0.unpersist()
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans0, ans, ansdo
gc.collect()
t_start = timeit.default_timer()
ans0 = spark.sql(sql0).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans0.count(), len(ans0.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans0.createOrReplaceTempView("ans")
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
ansdo = [ans.count(), len(ans.columns)]
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans0.unpersist()
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans0, ans, ansdo, sql0, sql

question = "min" # q4
sql0 = f'select id1, avg(v1) over (order by id1 rows between {w-1} preceding and current row) as v1 from x'
sql = f'select case when id1<{w} then null else v1 end as v1 from ans'
gc.collect()
t_start = timeit.default_timer()
ans0 = spark.sql(sql0).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans0.count(), len(ans0.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans0.createOrReplaceTempView("ans")
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
ansdo = [ans.count(), len(ans.columns)]
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans0.unpersist()
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans0, ans, ansdo
gc.collect()
t_start = timeit.default_timer()
ans0 = spark.sql(sql0).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans0.count(), len(ans0.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans0.createOrReplaceTempView("ans")
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
ansdo = [ans.count(), len(ans.columns)]
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans0.unpersist()
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans0, ans, ansdo, sql0, sql

#question = "median" # q5 ## https://stackoverflow.com/q/76760672/2490497

question = "multiroll" # q6
sql0 = f'select id1, avg(v1) over small as v1_small, avg(v1) over big as v1_big, avg(v2) over small as v2_small, avg(v2) over big as v2_big from x window small as (order by id1 rows between {w-51} preceding and current row), big as (order by id1 rows between {w+49} preceding and current row)'
sql = f'select case when id1<{w-50} then null else v1_small end as v1_small, case when id1<{w+50} then null else v1_big end as v1_big, case when id1<{w-50} then null else v2_small end as v2_small, case when id1<{w+50} then null else v2_big end as v2_big from ans'
gc.collect()
t_start = timeit.default_timer()
ans0 = spark.sql(sql0).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans0.count(), len(ans0.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans0.createOrReplaceTempView("ans")
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
ansdo = [ans.count(), len(ans.columns)]
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = spark.sql("select sum(v1_small) as v1_small, sum(v1_big) as v1_big, sum(v2_small) as v2_small, sum(v2_big) as v2_big from ans").collect()[0].asDict().values()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans0.unpersist()
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans0, ans, ansdo
gc.collect()
t_start = timeit.default_timer()
ans0 = spark.sql(sql0).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans0.count(), len(ans0.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans0.createOrReplaceTempView("ans")
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
ansdo = [ans.count(), len(ans.columns)]
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = spark.sql("select sum(v1_small) as v1_small, sum(v1_big) as v1_big, sum(v2_small) as v2_small, sum(v2_big) as v2_big from ans").collect()[0].asDict().values()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans0.unpersist()
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans0, ans, ansdo, sql0, sql

#question = "weighted" # q7 ## not yet implemented

question = "uneven dense" # q8
sql0 = f'select id2, avg(v1) over (order by id2 range between {w-1} preceding and current row) as v1 from x'
sql = f'select case when id2<{w} then null else v1 end as v1 from ans'
gc.collect()
t_start = timeit.default_timer()
ans0 = spark.sql(sql0).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans0.count(), len(ans0.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans0.createOrReplaceTempView("ans")
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
ansdo = [ans.count(), len(ans.columns)]
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans0.unpersist()
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans0, ans, ansdo
gc.collect()
t_start = timeit.default_timer()
ans0 = spark.sql(sql0).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans0.count(), len(ans0.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans0.createOrReplaceTempView("ans")
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
ansdo = [ans.count(), len(ans.columns)]
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans0.unpersist()
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans0, ans, ansdo, sql0, sql

question = "uneven sparse" # q9
sql0 = f'select id3, avg(v1) over (order by id3 range between {w-1} preceding and current row) as v1 from x'
sql = f'select case when id3<{w} then null else v1 end as v1 from ans'
gc.collect()
t_start = timeit.default_timer()
ans0 = spark.sql(sql0).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans0.count(), len(ans0.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans0.createOrReplaceTempView("ans")
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
ansdo = [ans.count(), len(ans.columns)]
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
ans0.unpersist()
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans0, ans, ansdo
gc.collect()
t_start = timeit.default_timer()
ans0 = spark.sql(sql0).persist(pyspark.StorageLevel.MEMORY_ONLY)
print((ans0.count(), len(ans0.columns)), flush=True) # shape
t = timeit.default_timer() - t_start
m = memory_usage()
ans0.createOrReplaceTempView("ans")
ans = spark.sql(sql).persist(pyspark.StorageLevel.MEMORY_ONLY)
ansdo = [ans.count(), len(ans.columns)]
ans.createOrReplaceTempView("ans")
t_start = timeit.default_timer()
chk = [spark.sql("select sum(v1) as v1 from ans").collect()[0].asDict()['v1']]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.count(), question=question, out_rows=ans.count(), out_cols=len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
ans0.unpersist()
ans.unpersist()
spark.catalog.uncacheTable("ans")
del ans0, ans, ansdo, sql0, sql

#question = "regression" # q10

spark.stop()

print("rolling finished, took %0.fs" % (timeit.default_timer()-task_init), flush=True)

exit(0)
