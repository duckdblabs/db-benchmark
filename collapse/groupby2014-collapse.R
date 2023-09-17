#!/usr/bin/env Rscript

cat("# groupby2014-collapse.R\n")

source("./_helpers/helpers.R")

stopifnot(requireNamespace(c("bit64","data.table"), quietly=TRUE)) # used in chk to sum numeric columns and data loading
.libPaths("./collapse/r-collapse") # tidyverse/collapse#4641
suppressPackageStartupMessages(library("collapse", lib.loc="./collapse/r-collapse", warn.conflicts=FALSE))
ver = packageVersion("collapse")
git = "" # uses stable version now #124
task = "groupby2014"
solution = "collapse"
fun = "group_by"
cache = TRUE
on_disk = FALSE

data_name = Sys.getenv("SRC_DATANAME")
src_grp = file.path("data", paste(data_name, "csv", sep="."))
cat(sprintf("loading dataset %s\n", data_name))

x = data.table::fread(src_grp, showProgress=FALSE, data.table=FALSE)
print(nrow(x))

task_init = proc.time()[["elapsed"]]
cat("grouping...\n")

question = "sum v1 by id1" # q1
t = system.time(print(dim(ans<-x |> fgroup_by(id1) %>% fsummarise(v1 = fsum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, v1=fsum(bit64::as.integer64(v1))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> fgroup_by(id1) %>% fsummarise(v1 = fsum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, v1=fsum(bit64::as.integer64(v1))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1 by id1:id2" # q2
t = system.time(print(dim(ans<-x %>% fgroup_by(id1,id2) %>% fsummarise(v1 = fsum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, v1=fsum(bit64::as.integer64(v1))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x %>% fgroup_by(id1,id2) %>% fsummarise(v1 = fsum(v1)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, v1=fsum(bit64::as.integer64(v1))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1 mean v3 by id3" # q3
t = system.time(print(dim(ans<-x %>% fgroup_by(id3) %>% fsummarise(v1 = sum(v1), v3 = mean(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, v1=fsum(bit64::as.integer64(v1)), v3=fsum(v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x %>% fgroup_by(id3) %>% fsummarise(v1 = sum(v1), v3 = mean(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, v1=fsum(bit64::as.integer64(v1)), v3=fsum(v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "mean v1:v3 by id4" # q4
t = system.time(print(dim(ans<-x |> fselect(id4, v1:v3) |> fgroup_by(id4) |> fmean)))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, v1=fsum(v1), v2=fsum(v2), v3=fsum(v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> fselect(id4, v1:v3) |> fgroup_by(id4) |> fmean)))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, v1=fsum(v1), v2=fsum(v2), v3=fsum(v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1:v3 by id6" # q5
t = system.time(print(dim(ans<-x %>% fselect(id6, v1:v3) |> fgroup_by(id6) |> fsum())))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, v1=fsum(bit64::as.integer64(v1)), v2=fsum(bit64::as.integer64(v2)), v3=fsum(v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x %>% fselect(id6, v1:v3) |> fgroup_by(id6) |> fsum())))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, v1=fsum(bit64::as.integer64(v1)), v2=fsum(bit64::as.integer64(v2)), v3=fsum(v3)))[["elapsed"]]
chkt = system.time(chk<-summarise(ans, v1=sum(bit64::as.integer64(v1)), v2=sum(bit64::as.integer64(v2)), v3=sum(v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

cat(sprintf("grouping finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)
