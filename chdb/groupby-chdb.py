#!/usr/bin/env python3

print("# groupby-chdb.py", flush=True)

import os
import gc
import timeit
import chdb
import shutil
import time

exec(open("./_helpers/helpers.py").read())

def parse_and_format_chk(csv_result):
    try:
        values = csv_result.strip().split(',')
        result = []
        for x in values:
            try:
                result.append(int(x))
            except ValueError:
                result.append(float(x))
        return make_chk(result)
    except (ValueError, TypeError, AttributeError):
        return 'NA'


ver = '.'.join(chdb.chdb_version)
task = "groupby"
git = "NA"
solution = "chdb"
fun = ".groupby"
cache = "TRUE"


data_name = os.environ["SRC_DATANAME"]
machine_type = os.environ["MACHINE_TYPE"]
src_grp = os.path.join("data", data_name + ".csv")
print("loading dataset %s" % data_name, flush=True)

chdb_join_db = f'{solution}_{task}_{data_name}.chdb'
conn = chdb.session.Session(chdb_join_db)

# Parse data characteristics from data name
data_parts = data_name.split("_")
scale_factor = float(data_parts[1])  # Number of rows
has_null = int(data_parts[3]) > 0    # NULL values flag
is_sorted = int(data_parts[4]) == 1  # Sorted flag

compress = scale_factor >= 1e9
on_disk = (scale_factor >= 1e9 and machine_type == "c6id.4xlarge")

threads = os.cpu_count() // 2
settings = f"SETTINGS max_insert_threads={threads}, max_threads={threads}"

# Start with Memory engine (matching ClickHouse ch_query logic)
query_engine = "Memory"
storage_engine = "MergeTree"

if compress:
    query_engine = "Memory SETTINGS compress=1"

if on_disk:
    query_engine = "MergeTree ORDER BY tuple()"

print(f"query_engine = '{query_engine}'")
print(f"storage_engine = '{storage_engine}'")
print(f"compress = {compress}, on_disk = {on_disk}")

if has_null:
    if is_sorted:
        table_schema = "(id1 LowCardinality(Nullable(String)), id2 LowCardinality(Nullable(String)), id3 Nullable(String), id4 Nullable(Int32), id5 Nullable(Int32), id6 Nullable(Int32), v1 Nullable(Int32), v2 Nullable(Int32), v3 Nullable(Float64))"
        order_by = "ORDER BY (id1,id2,id3,id4,id5,id6)" if storage_engine == "MergeTree" else ""
    else:
        table_schema = "(id1 LowCardinality(Nullable(String)), id2 LowCardinality(Nullable(String)), id3 Nullable(String), id4 Nullable(Int32), id5 Nullable(Int32), id6 Nullable(Int32), v1 Nullable(Int32), v2 Nullable(Int32), v3 Nullable(Float64))"
        order_by = "ORDER BY tuple()" if storage_engine == "MergeTree" else ""
else:
    if is_sorted:
        table_schema = "(id1 LowCardinality(String), id2 LowCardinality(String), id3 String, id4 Int32, id5 Int32, id6 Int32, v1 Int32, v2 Int32, v3 Float64)"
        order_by = "ORDER BY (id1,id2,id3,id4,id5,id6)" if storage_engine == "MergeTree" else ""
    else:
        table_schema = "(id1 LowCardinality(String), id2 LowCardinality(String), id3 String, id4 Int32, id5 Int32, id6 Int32, v1 Int32, v2 Int32, v3 Float64)"
        order_by = "ORDER BY tuple()" if storage_engine == "MergeTree" else ""

engine_clause = f"ENGINE = {storage_engine}() {order_by}"

conn.query("CREATE DATABASE IF NOT EXISTS db_benchmark ENGINE = Atomic")
conn.query("DROP TABLE IF EXISTS db_benchmark.x")

create_table_sql = f"CREATE TABLE db_benchmark.x {table_schema} {engine_clause};"
conn.query(create_table_sql)

conn.query(f"INSERT INTO db_benchmark.x FROM INFILE '{src_grp}'")
in_rows = int(str(conn.query("SELECT count(*) from db_benchmark.x")).strip())

task_init = timeit.default_timer()
print("grouping...", flush=True)

question = "sum v1 by id1" # q1
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id1, sum(v1) AS v1 FROM db_benchmark.x GROUP BY id1 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(v1) AS v1_sum FROM ans")))
chkt = timeit.default_timer() - t_start
# task, data, in_rows, question, out_rows, out_cols, solution, version, git, fun, run, time_sec, mem_gb, cache, chk, chk_time_sec, on_disk, machine_type
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    # It will take some time for memory freed by Memory engine to be returned back to the system.
    # Without a sleep we might get a MEMORY_LIMIT exception during the second run of the query.
    # It is done only when compress is true because this variable is set to true only for the largest dataset.
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id1, sum(v1) AS v1 FROM db_benchmark.x GROUP BY id1 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(v1) AS v1_sum FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "sum v1 by id1:id2" # q2
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id1, id2, sum(v1) AS v1 FROM db_benchmark.x GROUP BY id1, id2 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(v1) AS v1_sum FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id1, id2, sum(v1) AS v1 FROM db_benchmark.x GROUP BY id1, id2 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(v1) AS v1_sum FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "sum v1 mean v3 by id3" # q3
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM db_benchmark.x GROUP BY id3 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(v1) AS v1_sum, SUM(v3) AS v3_sum FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM db_benchmark.x GROUP BY id3 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(v1) AS v1_sum, SUM(v3) AS v3_sum FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "mean v1:v3 by id4" # q4
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM db_benchmark.x GROUP BY id4 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(v1) AS v1_sum, SUM(v2) AS v2_sum, SUM(v3) AS v3_sum FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM db_benchmark.x GROUP BY id4 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(v1) AS v1_sum, SUM(v2) AS v2_sum, SUM(v3) AS v3_sum FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "sum v1:v3 by id6" # q5
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM db_benchmark.x GROUP BY id6 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(v1) AS v1_sum, SUM(v2) AS v2_sum, SUM(v3) AS v3_sum FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM db_benchmark.x GROUP BY id6 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(v1) AS v1_sum, SUM(v2) AS v2_sum, SUM(v3) AS v3_sum FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "median v3 sd v3 by id4 id5" # q6
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id4, id5, medianExact(v3) AS median_v3, stddevPop(v3) AS sd_v3 FROM db_benchmark.x GROUP BY id4, id5 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(median_v3) AS median_v3, SUM(sd_v3) AS sd_v3 FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id4, id5, medianExact(v3) AS median_v3, stddevPop(v3) AS sd_v3 FROM db_benchmark.x GROUP BY id4, id5 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(median_v3) AS median_v3, SUM(sd_v3) AS sd_v3 FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "max v1 - min v2 by id3" # q7
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id3, max(v1) - min(v2) AS range_v1_v2 FROM db_benchmark.x GROUP BY id3 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(range_v1_v2) AS range_v1_v2 FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id3, max(v1) - min(v2) AS range_v1_v2 FROM db_benchmark.x GROUP BY id3 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(range_v1_v2) AS range_v1_v2 FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "largest two v3 by id6" # q8
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id6, arrayJoin(arraySlice(arrayReverseSort(groupArray(v3)), 1, 2)) AS largest2_v3 FROM (SELECT id6, v3 FROM db_benchmark.x WHERE v3 IS NOT NULL) AS subq GROUP BY id6 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(largest2_v3) AS largest2_v3 FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id6, arrayJoin(arraySlice(arrayReverseSort(groupArray(v3)), 1, 2)) AS largest2_v3 FROM (SELECT id6, v3 FROM db_benchmark.x WHERE v3 IS NOT NULL) AS subq GROUP BY id6 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(largest2_v3) AS largest2_v3 FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "regression v1 v2 by id2 id4" # q9
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM db_benchmark.x GROUP BY id2, id4 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(r2) AS r2 FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM db_benchmark.x GROUP BY id2, id4 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(r2) AS r2 FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "sum v3 count by id1:id6" # q10
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count() AS cnt FROM db_benchmark.x GROUP BY id1, id2, id3, id4, id5, id6 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(v3) AS v3, SUM(cnt) as cnt FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
if compress:
    time.sleep(60)
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans ENGINE = {query_engine} AS SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count() AS cnt FROM db_benchmark.x GROUP BY id1, id2, id3, id4, id5, id6 {settings}"
conn.query(QUERY)
nr = int(str(conn.query("SELECT count(*) AS cnt FROM ans")).strip())
nc = len(str(conn.query("SELECT * FROM ans LIMIT 0", "CSVWITHNAMES")).split(','))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = parse_and_format_chk(str(conn.query("SELECT SUM(v3) AS v3, SUM(cnt) as cnt FROM ans")))
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=chk, chk_time_sec=chkt, on_disk='TRUE', machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
if int(nr) > 3:
    print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

print("grouping finished, took %0.3fs" % (timeit.default_timer() - task_init), flush=True)

conn.close()
shutil.rmtree(chdb_join_db, ignore_errors=True)
exit(0)
