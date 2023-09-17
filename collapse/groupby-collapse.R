#!/usr/bin/env Rscript

cat("# groupby-collapse.R\n")

source("./_helpers/helpers.R")

stopifnot(requireNamespace(c("bit64","data.table"), quietly=TRUE)) # used in chk to sum numeric columns and data loading
.libPaths("./collapse/r-collapse") # tidyverse/collapse#4641
suppressPackageStartupMessages(library("collapse", lib.loc="./collapse/r-collapse", warn.conflicts=FALSE))
ver = packageVersion("collapse")
git = "" # uses stable version now #124
task = "groupby"
solution = "collapse"
fun = "group_by"
cache = TRUE
on_disk = FALSE

data_name = Sys.getenv("SRC_DATANAME")
src_grp = file.path("data", paste(data_name, "csv", sep="."))
cat(sprintf("loading dataset %s\n", data_name))

x = data.table::fread(src_grp, showProgress=FALSE, stringsAsFactors=TRUE, na.strings="", data.table=FALSE)
print(nrow(x))

task_init = proc.time()[["elapsed"]]
cat("grouping...\n")

question = "sum v1 by id1" # q1
t = system.time(print(dim(ans<-x |> fgroup_by("id1") |> fsummarize(v1 = fsum(na.rm = TRUE)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsum(bit64::as.integer64(ans$v1)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> fgroup_by("id1") |> fsummarize(v1 = fsum(na.rm = TRUE)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsum(bit64::as.integer64(ans$v1)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)



question = "sum v1 by id1:id2" # q2
t = system.time(print(dim(ans<-x |> fgroup_by(c("id1", "id2")) |> fsummarize(v1 = fsum(v1, na.rm = TRUE)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsum(bit64::as.integer64(x$v1)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> fgroup_by(c("id1", "id2")) |> fsummarize(v1 = fsum(v1, na.rm = TRUE)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsum(bit64::as.integer64(x$v1)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1 mean v3 by id3" # q3
t = system.time(print(dim(ans<-x |> fgroup_by("id3") |> fsummarize(v1 = fsum(v1, na.rm = TRUE), v3 = fmean(v3, na.rm = TRUE)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-qDF(list(v1=fsum(bit64::as.integer64(ans$v1)), v3=fsum(ans$v3))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> fgroup_by("id3") |> fsummarize(v1 = fsum(v1, na.rm = TRUE), v3 = fmean(v3, na.rm = TRUE)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-qDF(list(v1=fsum(bit64::as.integer64(ans$v1)), v3=fsum(ans$v3))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "mean v1:v3 by id4" # q4
t = system.time(print(dim(ans<-x |> fselect(id4, v1:v3) |> fgroup_by(id4) |> fmean(na.rm = TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans |> fselect(v1, v2, v3) |> fsum(na.rm = TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> fselect(id4, v1:v3) |> fgroup_by(id4) |> fmean(na.rm = TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans |> fselect(v1, v2, v3) |> fsum(na.rm = TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1:v3 by id6" # q5
t = system.time(print(dim(ans<-x |> fgroup_by(id6) |> fselect(v1:v3) |> fsum(na.rm = TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans |> fselect(v1, v2, v3) |> fsum(na.rm = TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> fgroup_by(id6) |> fselect(v1:v3) |> fsum(na.rm = TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans |> fselect(v1, v2, v3) |> fsum(na.rm = TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "median v3 sd v3 by id4 id5" # q6
t = system.time(print(dim(ans<-x |> fgroup_by(id4, id5) |> fsummarize(v3_median = fmedian(v3, na.rm = TRUE), v3_sd = fsd(v3, na.rm = TRUE)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans |> fselect(v3_median, v3_sd) |> fsum())[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> fgroup_by(id4, id5) |> fsummarize(v3_median = fmedian(v3, na.rm = TRUE), v3_sd = fsd(v3, na.rm = TRUE)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans |> fselect(v3_median, v3_sd) |> fsum())[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "max v1 - min v2 by id3" # q7
t = system.time(print(dim(ans<-x |> fgroup_by(id3) |> fsummarise(range_v1_v2=fmax(v1, na.rm=TRUE)-fmin(v2, na.rm=TRUE)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, range_v1_v2=sum(bit64::as.integer64(range_v1_v2))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> fgroup_by(id3) |> fsummarise(range_v1_v2=fmax(v1, na.rm=TRUE)-fmin(v2, na.rm=TRUE)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, range_v1_v2=sum(bit64::as.integer64(range_v1_v2))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "largest two v3 by id6" # q8
t = system.time(print(dim(ans<-x |> fselect(id6, v3) |> na_omit(cols = "v3") |> roworderv("v3", decreasing = TRUE) |> fgroup_by(id6) |> fsummarize(largest2_v3 = v3[1:2]))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, largest2_v3=sum(largest2_v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> fselect(id6, v3) |> na_omit(cols = "v3") |> roworderv("v3", decreasing = TRUE) |> fgroup_by(id6) |> fsummarize(largest2_v3 = v3[1:2]))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, largest2_v3=sum(largest2_v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

# TODO: there is probably a faster way to do this using psacf() et al.
question = "regression v1 v2 by id2 id4" # q9
t = system.time(print(dim(ans<-x |> fgroup_by(id2, id4) |> fsummarise(r2=cor(v1, v2, use="na.or.complete")^2))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, r2=sum(r2)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> fgroup_by(id2, id4) |> fsummarise(r2=cor(v1, v2, use="na.or.complete")^2))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, r2=sum(r2)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v3 count by id1:id6" # q10
t = system.time(print(dim(ans<-x |> fgroup_by(id1, id2, id3, id4, id5, id6) |> fsummarise(v3=fsum(v3, na.rm=TRUE), count=fnobs(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-fsummarise(ans, v3=fsum(v3), count=fsum(bit64::as.integer64(count))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> fgroup_by(id1, id2, id3, id4, id5, id6) |> fsummarise(v3=fsum(v3, na.rm=TRUE), count=fnobs(v3)))))[["elapsed"]]
mn = memory_usage()
chkt = system.time(chk<-fsummarise(ans, v3=fsum(v3), count=fsum(bit64::as.integer64(count))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

cat(sprintf("grouping finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)
