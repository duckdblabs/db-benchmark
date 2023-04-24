#!/usr/bin/env Rscript

if (!dir.exists("./datatable/r-datatable-adapt/data.table")) {
  cat("# data.table adapt branch library does not exist, installing\n")
  stopifnot(requireNamespace("remotes", quietly=TRUE))
  remotes::install_github("Rdatatable/data.table@adapt", subdir="./datatable/r-datatable-adapt")
}

cat("# rollfun-datatable.R\n")

source("./_helpers/helpers.R")

suppressPackageStartupMessages(library("data.table", lib.loc="./datatable/r-datatable-adapt"))
setDTthreads(0L)
ver = packageVersion("data.table")
git = data.table:::.git(quiet=TRUE)
task = "rollfun"
solution = "data.table"
cache = TRUE
on_disk = FALSE

data_name = Sys.getenv("SRC_DATANAME")
n = strsplit(data_name, "_", fixed=TRUE)[[1L]][2L]
data_name = "random"

set.seed(108)
DT = data.table(
  i1 = sort(sample(1.1*n, n)),
  i2 = sort(sample(10*n, n)),
  x1 = rnorm(n),
  x2 = rnorm(n, sd=n/100)
)
print(nrow(x))
w1 = n/1e6
w2 = n/1e3

task_init = proc.time()[["elapsed"]]
cat("rolling...\n")

fun = "frollmean"

question = "rolling mean" # q1
t = system.time(print(dim(ans<-frollmean(DT$x2, w2))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-frollmean(DT$x2, w2))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "rolling mean window small sd small" # q2
t = system.time(print(dim(ans<-frollmean(DT$x1, w1))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-frollmean(DT$x1, w1))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "rolling mean sd small" # q3
t = system.time(print(dim(ans<-frollmean(DT$x1, w2))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-frollmean(DT$x1, w2))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "rolling mean window small" # q4
t = system.time(print(dim(ans<-frollmean(DT$x2, w1))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-frollmean(DT$x2, w1))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

#### uneven

question = "rolling mean uneven dense" # q5: rnorm sd=n/100, window=large, id=1.1
t = system.time(print(dim(ans<-frollmean(DT$x2, frolladapt(DT$i1, w2), adaptive=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-frollmean(DT$x2, frolladapt(DT$i1, w2), adaptive=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "rolling mean uneven sparse" # q6 rnorm sd=n/100, window=large, id=10
t = system.time(print(dim(ans<-frollmean(DT$x2, frolladapt(DT$i2, w2), adaptive=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-frollmean(DT$x2, frolladapt(DT$i2, w2), adaptive=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

#### multiple columns and windows

#### double precision

#### user defined function

cat(sprintf("rolling finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)
