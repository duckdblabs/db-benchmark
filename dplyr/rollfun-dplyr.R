#!/usr/bin/env Rscript

cat("# rollfun-dplyr (slider).R\n")

source("./_helpers/helpers.R")

stopifnot(requireNamespace(c("bit64","data.table"), quietly=TRUE)) # used in chk to sum numeric columns and data loading
.libPaths("./dplyr/r-dplyr") # tidyverse/dplyr#4641
suppressPackageStartupMessages(library("dplyr", lib.loc="./dplyr/r-dplyr", warn.conflicts=FALSE))
suppressPackageStartupMessages(library("slider", lib.loc="./dplyr/r-dplyr", warn.conflicts=FALSE))
ver = packageVersion("dplyr")
git = "" # uses stable version now #124
task = "rollfun"
solution = "dplyr"
cache = TRUE
on_disk = FALSE

data_name = Sys.getenv("SRC_DATANAME")
src_grp = file.path("data", paste(data_name, "csv", sep="."))
cat(sprintf("loading dataset %s\n", data_name))

x = as_tibble(data.table::fread(src_grp, showProgress=FALSE, stringsAsFactors=TRUE, na.strings="", data.table=FALSE))
print(nrow(x))

# window size
w = nrow(x)/1e3L
wsmall = nrow(x)/1e4L
wbig = nrow(x)/1e2L

task_init = proc.time()[["elapsed"]]
cat("rolling...\n")

question = "rolling mean" # q1
fun = "slide_mean"
t = system.time(print(length(ans<-slide_mean(x$v1, before=w-1L, complete=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(length(ans<-slide_mean(x$v1, before=w-1L, complete=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "window small" # q2
fun = "slide_mean"
t = system.time(print(length(ans<-slide_mean(x$v1, before=wsmall-1L, complete=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(length(ans<-slide_mean(x$v1, before=wsmall-1L, complete=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "window big" # q3
fun = "slide_mean"
t = system.time(print(length(ans<-slide_mean(x$v1, before=wbig-1L, complete=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(length(ans<-slide_mean(x$v1, before=wbig-1L, complete=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "multi vars cols" # q4
fun = "slide_mean"
t = system.time(print(length(ans<-list(
  slide_mean(x$v1, before=w-51L, complete=TRUE), slide_mean(x$v1, before=w+49L, complete=TRUE),
  slide_mean(x$v2, before=w-51L, complete=TRUE), slide_mean(x$v2, before=w+49L, complete=TRUE)
))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-lapply(ans, sum, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans[[1L]]), out_cols=length(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(length(ans<-list(
  slide_mean(x$v1, before=w-51L, complete=TRUE), slide_mean(x$v1, before=w+49L, complete=TRUE),
  slide_mean(x$v2, before=w-51L, complete=TRUE), slide_mean(x$v2, before=w+49L, complete=TRUE)
))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-lapply(ans, sum, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans[[1L]]), out_cols=length(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(lapply(ans, head, 3))
print(lapply(ans, tail, 3))
rm(ans)

#question = "median" # q5 ## not yet implemeneted
#fun = "slide_median"
#t = system.time(print(length(ans<-slide_median(x$v1, before=w-1L, complete=TRUE))))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
#write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun="frollmedian", time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#rm(ans)
#t = system.time(print(length(ans<-slide_median(x$v1, before=w-1L, complete=TRUE))))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
#write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun="frollmedian", time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(head(ans, 3))
#print(tail(ans, 3))
#rm(ans)

#question = "weighted" # q6 ## not yet implemeneted
#fun = "slide_mean"
#t = system.time(print(length(ans<-slide_mean(x$v1, before=w-1L, complete=TRUE, w=x$weights))))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
#write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#rm(ans)
#t = system.time(print(length(ans<-slide_mean(x$v1, before=w-1L, complete=TRUE, w=x$weights))))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
#write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(head(ans, 3))
#print(tail(ans, 3))
#rm(ans)

question = "uneven dense" # q7
fun = "slide_index_mean"
t = system.time(print(length(ans<-slide_index_mean(x$v1, i=x$id2, before=w-1L, complete=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(length(ans<-slide_index_mean(x$v1, i=x$id2, before=w-1L, complete=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "uneven sparse" # q8
fun = "slide_index_mean"
t = system.time(print(length(ans<-slide_index_mean(x$v1, i=x$id3, before=w-1L, complete=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(length(ans<-slide_index_mean(x$v1, i=x$id3, before=w-1L, complete=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

#question = "regression" # q9 ## Killed, UDF simply does not scale
#fun = "slide"
#t = system.time(print(length(ans<-slide(select(x, v1, v2), ~lm(v2 ~ v1, data=.x), .before=w-1L, .complete=TRUE))))[["elapsed"]]
#m = memory_usage()
#TODO
#chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
#write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#rm(ans)
#t = system.time(print(length(ans<-slide(select(x, v1, v2), ~lm(v2 ~ v1, data=.x), .before=w-1L, .complete=TRUE))))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
#write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(head(ans, 3))
#print(tail(ans, 3))
#rm(ans)

#question = "udf" # q10 ## UDF simply does not scale
## compound distance
#udf = function(x) {
#  tmp <- range(x)
#  tmp[2L]/tmp[1L]
#}
#fun = "slide"
#t = system.time(print(length(ans<-slide_dbl(x$v1, udf, .before=w-1L, .complete=TRUE))))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
#write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#rm(ans)
#t = system.time(print(length(ans<-slide_dbl(x$v1, udf, .before=w-1L, .complete=TRUE))))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
#write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(head(ans, 3))
#print(tail(ans, 3))
#rm(ans)

cat(sprintf("rolling finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)
