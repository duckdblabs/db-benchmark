#!/usr/bin/env python3

print("# groupby-duckdb.py", flush=True)

import os
import sys
import time
import timeit
import duckdb

exec(open("./_helpers/helpers.py").read())

ver = duckdb.__version__
# git is set later on, after connecting to the db
task = "groupby"
solution = "duckdb"
fun = "group_by"
cache = "TRUE"

data_name = os.environ['SRC_DATANAME']
machine_type = os.environ['MACHINE_TYPE']
src_grp = os.path.join("data", data_name+".csv")
print("loading dataset %s" % data_name, flush=True)

db_file = "%s-%s-%s.db" % (solution, task, data_name)

on_disk = float(data_name.split("_")[1]) >= 1e10
on_disk = on_disk or (float(data_name.split("_")[1]) >= 1e9 and machine_type == "c6id.4xlarge")
uses_NAs = int(data_name.split("_")[3]) > 0

if on_disk:
  print("using disk memory-mapped data storage", flush=True)
  con = duckdb.connect(database=db_file)
else:
  print("using in-memory data storage", flush=True)
  con = duckdb.connect(database=":memory:")

con.execute("set storage_compatibility_version='latest'")

table_type = "TEMP"
if machine_type == 'c6id.4xlarge' and on_disk:
  con.execute("pragma memory_limit='25G'")
  table_type = ""

con.execute("SET enable_progress_bar = false;")

ncores = os.cpu_count()
con.execute("PRAGMA THREADS=%d" % ncores)
git = con.execute("SELECT source_id FROM pragma_version()").fetchone()[0]

# first create and ingest the table.
con.execute("CREATE TABLE y(id1 VARCHAR, id2 VARCHAR, id3 VARCHAR, id4 INT, id5 INT, id6 INT, v1 INT, v2 INT, v3 FLOAT)")
con.execute("COPY y FROM '%s' (AUTO_DETECT TRUE)" % src_grp)

con.execute("CREATE TYPE id1ENUM AS ENUM (SELECT id1 FROM y where id1 IS NOT NULL)")
con.execute("CREATE TYPE id2ENUM AS ENUM (SELECT id2 FROM y where id2 NOT NULL)")

con.execute("CREATE TABLE x(id1 id1ENUM, id2 id2ENUM, id3 VARCHAR, id4 INT, id5 INT, id6 INT, v1 INT, v2 INT, v3 FLOAT)")
con.execute("INSERT INTO x (SELECT * FROM y)")
con.execute("DROP TABLE IF EXISTS y")

in_nr = con.execute("SELECT count(*) AS cnt FROM x").fetchone()[0]
print(in_nr, flush=True)
con.execute("DROP TABLE IF EXISTS ans")


def count_cols():
  nr = con.execute("SELECT count(*) AS cnt FROM ans").fetchone()[0]
  nc = len(con.execute("SELECT * FROM ans LIMIT 0").description)
  print([nr, nc], flush=True)
  return nr, nc

def print_head_tail():
  print(con.execute("SELECT * FROM ans LIMIT 3").fetchall(), flush=True)                                      ## head
  print(con.execute("SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4").fetchall(), flush=True)  ## tail


task_init = timeit.default_timer()
print("grouping...", flush=True)

question = "sum v1 by id1" # q1
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id1, sum(v1) AS v1 FROM x GROUP BY id1" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(v1) AS v1 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id1, sum(v1) AS v1 FROM x GROUP BY id1" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(v1) AS v1 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

question = "sum v1 by id1:id2" # q2
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id1, id2, sum(v1) AS v1 FROM x GROUP BY id1, id2" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(v1) AS v1 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id1, id2, sum(v1) AS v1 FROM x GROUP BY id1, id2" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(v1) AS v1 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

question = "sum v1 mean v3 by id3" # q3
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM x GROUP BY id3" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(v1) AS v1, sum(v3) AS v3 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM x GROUP BY id3" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(v1) AS v1, sum(v3) AS v3 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

question = "mean v1:v3 by id4" # q4
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM x GROUP BY id4" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM x GROUP BY id4" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

question = "sum v1:v3 by id6" # q5
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM x GROUP BY id6" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM x GROUP BY id6" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

time.sleep(60)

question = "median v3 sd v3 by id4 id5" # q6
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id4, id5, quantile_cont(v3, 0.5) AS median_v3, stddev(v3) AS sd_v3 FROM x GROUP BY id4, id5" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(median_v3) AS median_v3, sum(sd_v3) AS sd_v3 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id4, id5, quantile_cont(v3, 0.5) AS median_v3, stddev(v3) AS sd_v3 FROM x GROUP BY id4, id5" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(median_v3) AS median_v3, sum(sd_v3) AS sd_v3 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

time.sleep(60)

question = "max v1 - min v2 by id3" # q7
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id3, max(v1)-min(v2) AS range_v1_v2 FROM x GROUP BY id3" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(range_v1_v2) AS range_v1_v2 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id3, max(v1)-min(v2) AS range_v1_v2 FROM x GROUP BY id3" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(range_v1_v2) AS range_v1_v2 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

time.sleep(60)

question = "largest two v3 by id6" # q8
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id6, unnest(max(v3, 2)) largest2_v3 FROM x WHERE v3 IS NOT NULL GROUP BY id6" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(largest2_v3) AS largest2_v3 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id6, unnest(max(v3, 2)) largest2_v3 FROM x WHERE v3 IS NOT NULL GROUP BY id6" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(largest2_v3) AS largest2_v3 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

time.sleep(60)

question = "regression v1 v2 by id2 id4" # q9
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM x GROUP BY id2, id4" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(r2) AS r2 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM x GROUP BY id2, id4" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(r2) AS r2 FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

time.sleep(60)

question = "sum v3 count by id1:id6" # q10
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count(*) AS count FROM x GROUP BY id1, id2, id3, id4, id5, id6" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(v3) AS v3, sum(count) AS count FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=1, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
con.execute("DROP TABLE IF EXISTS ans")
t_start = timeit.default_timer()
con.execute("CREATE %s TABLE ans AS SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count(*) AS count FROM x GROUP BY id1, id2, id3, id4, id5, id6" % table_type)
nr, nc = count_cols()
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = con.execute("SELECT sum(v3) AS v3, sum(count) AS count FROM ans").fetchone()
chkt = timeit.default_timer() - t_start
write_log(run=2, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print_head_tail()
con.execute("DROP TABLE IF EXISTS ans")

print("grouping finished, took %.0fs" % (timeit.default_timer() - task_init), flush=True)

con.close()

if on_disk and os.path.isfile(db_file):
  os.remove(db_file)

exit(0)
