#!/usr/bin/env Rscript

cat("# rollfun-duckdb-latest.R\n")

source("./_helpers/helpers.R")

suppressPackageStartupMessages({
  library("DBI", lib.loc="./duckdb-latest/r-duckdb-latest", warn.conflicts=FALSE)
  library("duckdb", lib.loc="./duckdb-latest/r-duckdb-latest", warn.conflicts=FALSE)
})
ver = packageVersion("duckdb")
#git = "" # set up later on after connecting to db
task = "rollfun"
solution = "duckdb-latest"
fun = "over"
cache = TRUE

data_name = Sys.getenv("SRC_DATANAME")
src_grp = file.path("data", paste(data_name, "csv", sep="."))
cat(sprintf("loading dataset %s\n", data_name))

on_disk = as.numeric(strsplit(data_name, "_", fixed=TRUE)[[1L]][2L])>=1e9
uses_NAs = as.numeric(strsplit(data_name, "_", fixed=TRUE)[[1L]][4L])>0
if (on_disk) {
  print("using disk memory-mapped data storage")
  con = dbConnect(duckdb::duckdb(), dbdir=tempfile())
} else {
  print("using in-memory data storage")
  con = dbConnect(duckdb::duckdb())
}

ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, "SET experimental_parallel_csv=true;"))
git = dbGetQuery(con, "SELECT source_id FROM pragma_version()")[[1L]]

# first create and ingest the table.
invisible(dbExecute(con, "CREATE TABLE y(id1 INT, id2 INT, id3 INT, v1 FLOAT, v2 FLOAT, weights FLOAT)"))
invisible(dbExecute(con, sprintf("COPY y FROM '%s' (AUTO_DETECT TRUE)", src_grp)))

# no enums in table, also as of now no NULLs in data, so uses_NAs handling not needed
invisible(dbExecute(con, "ALTER TABLE y RENAME TO x"))

print(in_nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM x")$cnt)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))

# window size
w = in_nr/1e3
wsmall = in_nr/1e4
wbig = in_nr/1e2

task_init = proc.time()[["elapsed"]]
cat("rolling...\n")

question = "mean" # q1
sql = sprintf("CREATE TABLE ans AS SELECT avg(v1) OVER (ORDER BY id1 ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS v1 FROM x", w-1L)
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", w-1))) ## due to https://github.com/duckdb/duckdb/discussions/8340 note that rowid is 0-based thus w-1
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", w-1)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
rm(sql)

question = "window small" # q2
sql = sprintf("CREATE TABLE ans AS SELECT avg(v1) OVER (ORDER BY id1 ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS v1 FROM x", wsmall-1L)
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", wsmall-1)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", wsmall-1)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
rm(sql)

question = "window big" # q3
sql = sprintf("CREATE TABLE ans AS SELECT avg(v1) OVER (ORDER BY id1 ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS v1 FROM x", wbig-1L)
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", wbig-1)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", wbig-1)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
rm(sql)

question = "min" # q4
sql = sprintf("CREATE TABLE ans AS SELECT MIN(v1) OVER (ORDER BY id1 ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS v1 FROM x", w-1L)
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", w-1)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", w-1)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
rm(sql)

question = "median" # q5
sql = sprintf("CREATE TABLE ans AS SELECT MEDIAN(v1) OVER (ORDER BY id1 ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS v1 FROM x", w-1L)
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", w-1)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", w-1)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
rm(sql)

question = "multiroll" # q6
sql = sprintf("CREATE TABLE ans AS SELECT avg(v1) OVER small AS v1_small, avg(v1) OVER big AS v1_big, avg(v2) OVER small AS v2_small, avg(v2) OVER small AS v2_big FROM x WINDOW small AS (ORDER BY id1 ROWS BETWEEN %d PRECEDING AND CURRENT ROW), big AS (ORDER BY id1 ROWS BETWEEN %d PRECEDING AND CURRENT ROW)", w-51L, w+49L)
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", w-1)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1_small) AS v1_small, sum(v1_big) AS v1_big, sum(v2_small) AS v2_small, sum(v2_big) AS v2_big FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", w-1)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1_small) AS v1_small, sum(v1_big) AS v1_big, sum(v2_small) AS v2_small, sum(v2_big) AS v2_big FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
rm(sql)

#question = "weighted" # q7 ## not yet implemented
#sql = sprintf("CREATE TABLE ans AS SELECT avg(v1) OVER (ORDER BY id1 ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS v1 FROM x", w-1L)
#t = system.time({
#  dbExecute(con, sql)
#  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
#})[["elapsed"]]
#m = memory_usage()
#invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", w-1)))
#chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
#write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
#t = system.time({
#  dbExecute(con, sql)
#  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
#})[["elapsed"]]
#m = memory_usage()
#invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", w-1)))
#chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
#write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
#print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
#invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
#rm(sql)

question = "uneven dense" # q8
sql = sprintf("CREATE TABLE ans AS SELECT avg(v1) OVER (ORDER BY id2 RANGE BETWEEN %d PRECEDING AND CURRENT ROW) AS v1 FROM x", w-1L)
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID <= (SELECT max(ROWID) FROM x WHERE id2 < %d)", w)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID <= (SELECT max(ROWID) FROM x WHERE id2 < %d)", w)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
rm(sql)

question = "uneven sparse" # q9
sql = sprintf("CREATE TABLE ans AS SELECT avg(v1) OVER (ORDER BY id3 RANGE BETWEEN %d PRECEDING AND CURRENT ROW) AS v1 FROM x", w-1L)
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID <= (SELECT max(ROWID) FROM x WHERE id3 < %d)", w)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID <= (SELECT max(ROWID) FROM x WHERE id3 < %d)", w)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(v1) AS v1 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
rm(sql)

question = "regression" # q10
sql = sprintf("CREATE TABLE ans AS SELECT regr_r2(v2, v1) OVER (ORDER BY id1 ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS r2 FROM x", w-1L)
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", w-1)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(r2) AS r2 FROM ans"))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
t = system.time({
  dbExecute(con, sql)
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
})[["elapsed"]]
m = memory_usage()
invisible(dbSendQuery(con, sprintf("UPDATE ans SET v1 = NULL WHERE ROWID < %d", w-1)))
chkt = system.time(chk<-dbGetQuery(con, "SELECT sum(r2) AS r2 FROM ans"))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=in_nr, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(dbGetQuery(con, "SELECT * FROM ans LIMIT 3"))                                      ## head
print(dbGetQuery(con, "SELECT * FROM ans WHERE ROWID > (SELECT count(*) FROM ans) - 4")) ## tail
invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
rm(sql)

invisible(dbDisconnect(con, shutdown=TRUE))

cat(sprintf("rolling finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)
