#!/usr/bin/env Rscript

cat("# groupby-duckplyr.R\n")

source("./_helpers/helpers.R")

stopifnot(requireNamespace(c("bit64","data.table"), quietly=TRUE)) # used in chk to sum numeric columns and data loading
.libPaths("./duckplyr/r-duckplyr") # tidyverse/duckplyr#4641
suppressPackageStartupMessages(library("duckplyr", lib.loc="./duckplyr/r-duckplyr", warn.conflicts=FALSE))
ver = packageVersion("duckplyr")
git = "" # uses stable version now #124
task = "groupby"
solution = "duckplyr"
fun = "group_by"
cache = TRUE
on_disk = FALSE

data_name = Sys.getenv("SRC_DATANAME")
src_grp = file.path("data", paste(data_name, "csv", sep="."))
cat(sprintf("loading dataset %s\n", data_name))

x = as_duckplyr_tibble(data.table::fread(src_grp, showProgress=FALSE, stringsAsFactors=TRUE, na.strings="", data.table=FALSE))
print(nrow(x))

task_init = proc.time()[["elapsed"]]
cat("grouping...\n")

question = "sum v1 by id1" # q1
t = system.time(print(dim(ans<-x %>% summarise(.by = id1, v1=sum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, v1=sum(v1)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x %>% summarise(.by = id1, v1=sum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, v1=sum(v1)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1 by id1:id2" # q2
t = system.time(print(dim(ans<-x %>% summarise(.by = c(id1, id2), v1=sum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, v1=sum(v1)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x %>% summarise(.by = c(id1, id2), v1=sum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, v1=sum(v1)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1 mean v3 by id3" # q3
t = system.time(print(dim(ans<-x %>% summarise(.by = id3, v1=sum(v1), v3=mean(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, v1=sum(v1), v3=sum(v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x %>% summarise(.by = id3, v1=sum(v1), v3=mean(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, v1=sum(v1), v3=sum(v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "mean v1:v3 by id4" # q4
t = system.time(print(dim(ans<-x %>% summarise(.by = id4, v1 = mean(v1), v2 = mean(v2), v3 = mean(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, v1=sum(v1), v2=sum(v2), v3=sum(v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x %>% summarise(.by = id4, v1 = mean(v1), v2 = mean(v2), v3 = mean(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, v1=sum(v1), v2=sum(v2), v3=sum(v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1:v3 by id6" # q5
t = system.time(print(dim(ans<-x %>% summarise(.by = id6, v1 = sum(v1), v2 = sum(v2), v3 = sum(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans), v1=sum(v1), v2=sum(v2), v3=sum(v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x %>% summarise(.by = id6, v1 = sum(v1), v2 = sum(v2), v3 = sum(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans), v1=sum(v1), v2=sum(v2), v3=sum(v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "median v3 sd v3 by id4 id5" # q6
t = system.time(print(dim(ans<-x %>% summarise(.by = c(id4, id5), median_v3=median(v3), sd_v3=sd(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, median_v3=sum(median_v3), sd_v3=sum(sd_v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x %>% summarise(.by = c(id4, id5), median_v3=median(v3), sd_v3=sd(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, median_v3=sum(median_v3), sd_v3=sum(sd_v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "max v1 - min v2 by id3" # q7
t = system.time(print(dim(ans<-x %>% summarise(.by = id3, range_v1_v2=max(v1)-min(v2)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, range_v1_v2=sum(range_v1_v2)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x %>% summarise(.by = id3, range_v1_v2=max(v1)-min(v2)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, range_v1_v2=sum(range_v1_v2)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "largest two v3 by id6" # q8
t = system.time(print(dim(ans<-x %>% select(id6, largest2_v3=v3) %>% filter(!is.na(largest2_v3)) %>% arrange(desc(largest2_v3)) %>% filter(.by = id6, row_number() <= 2L))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, largest2_v3=sum(largest2_v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x %>% select(id6, largest2_v3=v3) %>% filter(!is.na(largest2_v3)) %>% arrange(desc(largest2_v3)) %>% filter(.by = id6, row_number() <= 2L))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, largest2_v3=sum(largest2_v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "regression v1 v2 by id2 id4" # q9
t = system.time(print(dim(ans<-x %>% summarise(.by = c(id2, id4), r2=cor(v1, v2, use="na.or.complete")^2))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, r2=sum(r2)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x %>% summarise(.by = c(id2, id4), r2=cor(v1, v2, use="na.or.complete")^2))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, r2=sum(r2)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v3 count by id1:id6" # q10
t = system.time(print(dim(ans<-x %>% summarise(.by = c(id1, id2, id3, id4, id5, id6), v3=sum(v3), count=n()))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, v3=sum(v3), count=sum(count)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x %>% summarise(.by = c(id1, id2, id3, id4, id5, id6), v3=sum(v3), count=n()))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, v3=sum(v3), count=sum(count)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

cat(sprintf("grouping finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)
