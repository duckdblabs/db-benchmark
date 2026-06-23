#!/usr/bin/env python3

print("# join-duckdb.py", flush=True)

import os
import sys
import timeit
import duckdb

exec(open("./_helpers/helpers.py").read())

ver = duckdb.__version__
# git is set later on, after connecting to the db
task = "join"
solution = "duckdb"
cache = "TRUE"

data_name = os.environ['SRC_DATANAME']
machine_type = os.environ['MACHINE_TYPE']
src_jn_x = os.path.join("data", data_name+".csv")
y_data_name = join_to_tbls(data_name)
src_jn_y = [os.path.join("data", y+".csv") for y in y_data_name]
assert len(src_jn_y) == 3, "join_to_tbls returned the wrong number of tables"
print("loading datasets %s" % ", ".join([data_name] + y_data_name), flush=True)

duckdb_join_db = "%s_%s_%s.db" % (solution, task, data_name)
on_disk = float(data_name.split("_")[1]) >= 1e9

less_cores = float("J1_1e7_NA_0_0".split("_")[1]) <= 1e7

uses_NAs = int(data_name.split("_")[3]) > 0

if on_disk:
  print("using disk memory-mapped data storage", flush=True)
  con = duckdb.connect(database=duckdb_join_db)
else:
  print("using in-memory data storage", flush=True)
  con = duckdb.connect(database=":memory:")

# set to latest storage compatibility version
con.execute("set storage_compatibility_version='latest'")
if machine_type == 'c6id.4xlarge':
  con.execute("pragma memory_limit='25G'")
  table_type = ""

ncores = os.cpu_count()
if less_cores:
  ncores = min(ncores, 40)

con.execute("PRAGMA THREADS=%d" % ncores)
git = con.execute("SELECT source_id FROM pragma_version()").fetchone()[0]

con.execute("CREATE TABLE x_csv AS SELECT * FROM read_csv_auto('%s')" % src_jn_x)
con.execute("CREATE TABLE small_csv AS SELECT * FROM read_csv_auto('%s')" % src_jn_y[0])
con.execute("CREATE TABLE medium_csv AS SELECT * FROM read_csv_auto('%s')" % src_jn_y[1])
con.execute("CREATE TABLE big_csv AS SELECT * FROM read_csv_auto('%s')" % src_jn_y[2])

id4_enum_statement = "SELECT id4 from (SELECT id4 FROM x_csv UNION ALL SELECT id4 FROM small_csv UNION ALL SELECT id4 from medium_csv UNION ALL SELECT id4 from big_csv) where id4 IS NOT NULL"
id5_enum_statement = "SELECT id5 from (SELECT id5 FROM x_csv UNION ALL SELECT id5 from medium_csv UNION ALL SELECT id5 from big_csv) where id5 IS NOT NULL"
con.execute("CREATE TYPE id4ENUM AS ENUM (%s)" % id4_enum_statement)
con.execute("CREATE TYPE id5ENUM AS ENUM (%s)" % id5_enum_statement)

con.execute("CREATE TABLE small(id1 INT64, id4 id4ENUM, v2 DOUBLE)")
con.execute("INSERT INTO small (SELECT * from small_csv)")

con.execute("CREATE TABLE medium(id1 INT64, id2 INT64, id4 id4ENUM, id5 id5ENUM, v2 DOUBLE)")
con.execute("INSERT INTO medium (SELECT * FROM medium_csv)")

con.execute("CREATE TABLE big(id1 INT64, id2 INT64, id3 INT64, id4 id4ENUM, id5 id5ENUM, id6 VARCHAR, v2 DOUBLE)")
con.execute("INSERT INTO big (Select * from big_csv)")

con.execute("CREATE TABLE x(id1 INT64, id2 INT64, id3 INT64, id4 id4ENUM, id5 id5ENUM, id6 VARCHAR, v1 DOUBLE)")
con.execute("INSERT INTO x (SELECT * FROM x_csv);")

# drop all the csv ingested tables
con.execute("DROP TABLE x_csv")
con.execute("DROP TABLE small_csv")
con.execute("DROP TABLE medium_csv")
con.execute("DROP TABLE big_csv")

in_nr = con.execute("SELECT count(*) AS cnt FROM x").fetchone()[0]
print(in_nr, flush=True)
print(con.execute("SELECT count(*) AS cnt FROM small").fetchone()[0], flush=True)
print(con.execute("SELECT count(*) AS cnt FROM medium").fetchone()[0], flush=True)
print(con.execute("SELECT count(*) AS cnt FROM big").fetchone()[0], flush=True)


def count_cols():
  nr = con.execute("SELECT count(*) AS cnt FROM ans").fetchone()[0]
  nc = len(con.execute("SELECT * FROM ans LIMIT 0").description)
  print([nr, nc], flush=True)
  return nr, nc

def print_head_tail():
  print(con.execute("SELECT * FROM ans LIMIT 3").fetchall(), flush=True)                                      ## head
  print(con.execute("SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4").fetchall(), flush=True)  ## tail


task_init = timeit.default_timer()
print("joining...", flush=True)

question = "small inner on int" # q1
fun = "inner_join"

table_type = "TEMP"
if on_disk and machine_type == "c6id.4xlarge":
  con.execute("pragma memory_limit='20G'")
  table_type = ""

t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT x.*, small.id4 AS small_id4, v2 FROM x JOIN small USING (id1)" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT x.*, small.id4 AS small_id4, v2 FROM x JOIN small USING (id1)" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

question = "medium inner on int" # q2
fun = "inner_join"

t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x JOIN medium USING (id2)" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x JOIN medium USING (id2)" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

question = "medium outer on int" # q3
fun = "left_join"

t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x LEFT JOIN medium USING (id2)" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x LEFT JOIN medium USING (id2)" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

question = "medium inner on factor" # q4
fun = "inner_join"

t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 AS medium_id4, v2 FROM x JOIN medium USING (id5)" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 AS medium_id4, v2 FROM x JOIN medium USING (id5)" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

question = "big inner on int" # q5
fun = "inner_join"

t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 AS big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM x JOIN big USING (id3)" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 AS big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM x JOIN big USING (id3)" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT SUM(v1) AS v1, SUM(v2) AS v2 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

con.close()

if on_disk and os.path.isfile(duckdb_join_db):
  os.remove(duckdb_join_db)

print("joining finished, took %.0fs" % (timeit.default_timer() - task_init), flush=True)

exit(0)
