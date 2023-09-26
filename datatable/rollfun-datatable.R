#!/usr/bin/env Rscript

if (!dir.exists("./datatable/r-datatable-rollmedian/data.table")) {
  cat("# data.table adapt branch library does not exist, installing\n")
  #stopifnot(requireNamespace("remotes", quietly=TRUE))
  dir.create("./datatable/r-datatable-rollmedian", showWarnings=FALSE)
  #remotes::install_github("Rdatatable/data.table@rollmedian", force=TRUE, lib="./datatable/r-datatable-rollmedian")
  ## https://github.com/duckdblabs/db-benchmark/actions/runs/5585092483/job/15159770868
  ## install_github fails from GH Actions with HTTP error 401, therefore use standard R repo instead
  install.packages("data.table", repos="https://jangorecki.github.io/data.table-rollmedian", lib="./datatable/r-datatable-rollmedian")
}

cat("# rollfun-datatable.R\n")

source("./_helpers/helpers.R")

suppressPackageStartupMessages(library("data.table", lib.loc="./datatable/r-datatable-rollmedian"))
setDTthreads(0L)
## till rollmedian branch is not yet merged to master we need extra trickery so DT version/git between logs.csv and time.csv matches
if (FALSE) {
  # use this when rollmedian branch will be merged to master
  ver = packageVersion("data.table")
  git = data.table:::.git(quiet=TRUE)
} else {
  f = Sys.getenv("CSV_LOGS_FILE", "logs.csv")
  if (file.exists(f)) {
    l = fread(f)[.N]
    if (nrow(l)==1L && identical(l$solution, "data.table") && identical(l$action, "start")) {
      ver = l$version
      git = l$git
    } else {
      # possibly run interactively
      ver = NA_character_
      git = ""
    }
    rm(f, l)
  } else {
    ver = NA_character_
    git = ""
  }
}
task = "rollfun"
solution = "data.table"
cache = TRUE
on_disk = FALSE

data_name = Sys.getenv("SRC_DATANAME")
src_grp = file.path("data", paste(data_name, "csv", sep="."))
cat(sprintf("loading dataset %s\n", data_name))

x = fread(src_grp, showProgress=FALSE, stringsAsFactors=TRUE, na.strings="")
print(nrow(x))

# window size
w = nrow(x)/1e3
wsmall = nrow(x)/1e4
wbig = nrow(x)/1e2

task_init = proc.time()[["elapsed"]]
cat("rolling...\n")

fun = "frollmean"

question = "mean" # q1
t = system.time(print(length(ans<-frollmean(x$v1, w))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(length(ans<-frollmean(x$v1, w))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "window small" # q2
t = system.time(print(length(ans<-frollmean(x$v1, wsmall))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(length(ans<-frollmean(x$v1, wsmall))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "window big" # q3
t = system.time(print(length(ans<-frollmean(x$v1, wbig))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(length(ans<-frollmean(x$v1, wbig))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

fun = "frollmin"

question = "min" # q4
t = system.time(print(length(ans<-frollmin(x$v1, w))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(length(ans<-frollmin(x$v1, w))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

fun = "frollmedian"

question = "median" # q5
t = system.time(print(length(ans<-frollmedian(x$v1, w))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun="frollmedian", time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(length(ans<-frollmedian(x$v1, w))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun="frollmedian", time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

fun = "frollmean"

question = "multiroll" # q6
t = system.time(print(length(ans<-frollmean(list(x$v1, x$v2), c(w-50L, w+50L)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-lapply(ans, sum, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans[[1L]]), out_cols=length(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(length(ans<-frollmean(list(x$v1, x$v2), c(w-50L, w+50L)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-lapply(ans, sum, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans[[1L]]), out_cols=length(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(lapply(ans, head, 3))
print(lapply(ans, tail, 3))
rm(ans)

#question = "weighted" # q7 ## not yet implemeneted
#t = system.time(print(length(ans<-frollmean(x$v1, w, w=x$weights))))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
#write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#rm(ans)
##t = system.time(print(length(ans<-frollmean(x$v1, w, w=x$weights))))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
#write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(head(ans, 3))
#print(tail(ans, 3))
#rm(ans)

fun = "frollmean"

question = "uneven dense" # q8
t = system.time(print(length(ans<-frollmean(x$v1, frolladapt(x$id2, w), adaptive=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(length(ans<-frollmean(x$v1, frolladapt(x$id2, w), adaptive=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "uneven sparse" # q9
t = system.time(print(length(ans<-frollmean(x$v1, frolladapt(x$id3, w), adaptive=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(length(ans<-frollmean(x$v1, frolladapt(x$id3, w), adaptive=TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

#fun = "frollreg"

#question = "regression" # q10 ## not yet implemeneted
#t = system.time(print(length(ans<-frollreg(list(x$v1, x$v2), w))))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
#write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#rm(ans)
#t = system.time(print(length(ans<-frollreg(list(x$v1, x$v2), w))))[["elapsed"]]
#m = memory_usage()
#chkt = system.time(chk<-sum(ans, na.rm=TRUE))[["elapsed"]]
#write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=length(ans), out_cols=1L, solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(head(ans, 3))
#print(tail(ans, 3))
#rm(ans)

cat(sprintf("rolling finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

if( !interactive() ) q("no", status=0)
