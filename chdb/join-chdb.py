#!/usr/bin/env python3

print("# join-chdb.py", flush=True)

import os
import gc
import timeit
import chdb
import shutil
import time

exec(open("./_helpers/helpers.py").read())

ver = '.'.join(chdb.chdb_version)
task = "join"
git = "NA"
solution = "chdb"
fun = ".join"
cache = "TRUE"

data_name = os.environ["SRC_DATANAME"]
machine_type = os.environ["MACHINE_TYPE"]
src_jn_x = os.path.join("data", data_name + ".csv")
y_data_name = join_to_tbls(data_name)
src_jn_y = [os.path.join("data", y_data_name[0] + ".csv"), os.path.join("data", y_data_name[1] + ".csv"), os.path.join("data", y_data_name[2] + ".csv")]
if len(src_jn_y) != 3:
  raise Exception("Something went wrong in preparing files used for join")


chdb_join_db = f'{solution}_{task}_{data_name}.chdb'
conn = chdb.session.Session(chdb_join_db)

# Parse data characteristics from data name
data_parts = data_name.split("_")
scale_factor = float(data_parts[1])  # Number of rows
has_null = int(data_parts[3]) > 0    # NULL values flag
is_sorted = int(data_parts[4]) == 1  # Sorted flag

compress = scale_factor >= 1e9
on_disk = (scale_factor >= 1e9) or (scale_factor >= 1e8 and machine_type == "c6id.4xlarge")

threads = os.cpu_count() // 2
settings = f"SETTINGS max_insert_threads={threads}, max_threads={threads}"

print("loading datasets " + data_name + ", " + y_data_name[0] + ", " + y_data_name[1] + ", " + y_data_name[2], flush=True)

query_engine = "Memory"
storage_engine = "MergeTree"

if compress:
    query_engine = "Memory SETTINGS compress=1"

if on_disk:
    query_engine = "MergeTree ORDER BY tuple()"

print(f"query_engine = '{query_engine}'")
print(f"storage_engine = '{storage_engine}'")
print(f"compress = {compress}, on_disk = {on_disk}")
conn.query("CREATE DATABASE IF NOT EXISTS db_benchmark ENGINE = Atomic")
conn.query("DROP TABLE IF EXISTS db_benchmark.x")
conn.query("DROP TABLE IF EXISTS db_benchmark.small")
conn.query("DROP TABLE IF EXISTS db_benchmark.medium")
conn.query("DROP TABLE IF EXISTS db_benchmark.big")

if has_null:
    if is_sorted:
        x_schema = "(id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), id6 Nullable(String), v1 Nullable(Float64))"
        small_schema = "(id1 Nullable(Int32), id4 Nullable(String), v2 Nullable(Float64))"
        medium_schema = "(id1 Nullable(Int32), id2 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), v2 Nullable(Float64))"
        big_schema = "(id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), id6 Nullable(String), v2 Nullable(Float64))"
        x_order = "ORDER BY (id1, id2, id3, id4, id5, id6)" if storage_engine == "MergeTree" else ""
        small_order = "ORDER BY (id1, id4)" if storage_engine == "MergeTree" else ""
        medium_order = "ORDER BY (id1, id2, id4, id5)" if storage_engine == "MergeTree" else ""
        big_order = "ORDER BY (id1, id2, id3, id4, id5, id6)" if storage_engine == "MergeTree" else ""
    else:
        x_schema = "(id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), id6 Nullable(String), v1 Nullable(Float64))"
        small_schema = "(id1 Nullable(Int32), id4 Nullable(String), v2 Nullable(Float64))"
        medium_schema = "(id1 Nullable(Int32), id2 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), v2 Nullable(Float64))"
        big_schema = "(id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), id6 Nullable(String), v2 Nullable(Float64))"
        x_order = "ORDER BY tuple()" if storage_engine == "MergeTree" else ""
        small_order = "ORDER BY tuple()" if storage_engine == "MergeTree" else ""
        medium_order = "ORDER BY tuple()" if storage_engine == "MergeTree" else ""
        big_order = "ORDER BY tuple()" if storage_engine == "MergeTree" else ""
else:
    if is_sorted:
        x_schema = "(id1 Int32, id2 Int32, id3 Int32, id4 String, id5 String, id6 String, v1 Float64)"
        small_schema = "(id1 Int32, id4 String, v2 Float64)"
        medium_schema = "(id1 Int32, id2 Int32, id4 String, id5 String, v2 Float64)"
        big_schema = "(id1 Int32, id2 Int32, id3 Int32, id4 String, id5 String, id6 String, v2 Float64)"
        x_order = "ORDER BY (id1, id2, id3, id4, id5, id6)" if storage_engine == "MergeTree" else ""
        small_order = "ORDER BY (id1, id4)" if storage_engine == "MergeTree" else ""
        medium_order = "ORDER BY (id1, id2, id4, id5)" if storage_engine == "MergeTree" else ""
        big_order = "ORDER BY (id1, id2, id3, id4, id5, id6)" if storage_engine == "MergeTree" else ""
    else:
        x_schema = "(id1 Int32, id2 Int32, id3 Int32, id4 String, id5 String, id6 String, v1 Float64)"
        small_schema = "(id1 Int32, id4 String, v2 Float64)"
        medium_schema = "(id1 Int32, id2 Int32, id4 String, id5 String, v2 Float64)"
        big_schema = "(id1 Int32, id2 Int32, id3 Int32, id4 String, id5 String, id6 String, v2 Float64)"
        x_order = "ORDER BY tuple()" if storage_engine == "MergeTree" else ""
        small_order = "ORDER BY tuple()" if storage_engine == "MergeTree" else ""
        medium_order = "ORDER BY tuple()" if storage_engine == "MergeTree" else ""
        big_order = "ORDER BY tuple()" if storage_engine == "MergeTree" else ""

# Construct and execute CREATE TABLE statements
x_engine = f"ENGINE = {storage_engine}() {x_order}"
small_engine = f"ENGINE = {storage_engine}() {small_order}"
medium_engine = f"ENGINE = {storage_engine}() {medium_order}"
big_engine = f"ENGINE = {storage_engine}() {big_order}"

conn.query(f"CREATE TABLE db_benchmark.x {x_schema} {x_engine};")
conn.query(f"CREATE TABLE db_benchmark.small {small_schema} {small_engine};")
conn.query(f"CREATE TABLE db_benchmark.medium {medium_schema} {medium_engine};")
conn.query(f"CREATE TABLE db_benchmark.big {big_schema} {big_engine};");

conn.query(f"INSERT INTO db_benchmark.x FROM INFILE '{src_jn_x}'")
conn.query(f"INSERT INTO db_benchmark.small FROM INFILE '{src_jn_y[0]}'")
conn.query(f"INSERT INTO db_benchmark.medium FROM INFILE '{src_jn_y[1]}'")
conn.query(f"INSERT INTO db_benchmark.big FROM INFILE '{src_jn_y[2]}'")

print(conn.query("SELECT count(*) from db_benchmark.x"))
print(conn.query("SELECT count(*) from db_benchmark.small"))
print(conn.query("SELECT count(*) from db_benchmark.medium"))
print(conn.query("SELECT count(*) from db_benchmark.big"))

in_rows = int(str(conn.query("SELECT count(*) from db_benchmark.x")).strip())

task_init = timeit.default_timer()
print("joining...", flush=True)

question = "small inner on int" # q1
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT x.*, small.id4 AS small_id4, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.small AS small USING (id1) {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk='NA', chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    # It will take some time for memory freed by Memory engine to be returned back to the system.
    # Without a sleep we might get a MEMORY_LIMIT exception during the second run of the query.
    # It is done only when compress is true because this variable is set to true only for the largest dataset.
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT x.*, small.id4 AS small_id4, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.small AS small USING (id1) {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk='NA', chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "medium inner on int" # q2
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 as medium_id5, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.medium AS medium USING (id2) {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk='NA', chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 as medium_id5, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.medium AS medium USING (id2) {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk='NA', chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "medium outer on int" # q3
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 as medium_id5, v2 FROM db_benchmark.x AS x LEFT JOIN db_benchmark.medium AS medium USING (id2) {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk='NA', chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 as medium_id5, v2 FROM db_benchmark.x AS x LEFT JOIN db_benchmark.medium AS medium USING (id2) {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk='NA', chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "medium inner on factor" # q4
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 as medium_id4, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.medium AS medium USING (id5) {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk='NA', chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 as medium_id4, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.medium AS medium USING (id5) {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk='NA', chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "big inner on int" # q5
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 as big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.big AS big USING (id3) {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk='NA', chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 as big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.big AS big USING (id3) {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk='NA', chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

print("joining finished, took %0.fs" % (timeit.default_timer() - task_init), flush=True)

conn.close()
shutil.rmtree(chdb_join_db, ignore_errors=True)
exit(0)
