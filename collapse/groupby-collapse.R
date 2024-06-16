#!/usr/bin/env Rscript

cat("# groupby-collapse.R\n")

source("./_helpers/helpers.R")

stopifnot(requireNamespace("data.table", quietly=TRUE)) # collapse does not support integer64. Oversized ints will be summed to double. 
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
gc()

# Setting collapse options: namespace masking and performance
oldopts <- set_collapse(nthreads = max(data.table::getDTthreads(), 4), 
                        mask = "all",
                        sort = endsWith(data_name, "_1"), # || nrow(x) > 2e8, 
                        na.rm = anyNA(num_vars(x)), 
                        stable.algo = FALSE)

task_init = proc.time()[["elapsed"]]
cat("grouping...\n")

question = "sum v1 by id1" # q1
t = system.time(print(dim(ans<-collap(x, v1 ~ id1, sum, fill = TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans$v1))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-collap(x, v1 ~ id1, sum, fill = TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(ans$v1))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1 by id1:id2" # q2
t = system.time(print(dim(ans<-collap(x, v1 ~ id1 + id2, sum, fill = TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(x$v1))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-collap(x, v1 ~ id1 + id2, sum, fill = TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-sum(x$v1))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1 mean v3 by id3" # q3
options(collapse_unused_arg_action = "none")
t = system.time(print(dim(ans<-collap(x, ~ id3, custom = list(sum = "v1", mean = "v3"), fill = TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-qDF(list(v1=sum(ans$v1), v3=sum(ans$v3))))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-collap(x, ~ id3, custom = list(sum = "v1", mean = "v3"), fill = TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-qDF(list(v1=sum(ans$v1), v3=sum(ans$v3))))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "mean v1:v3 by id4" # q4
t = system.time(print(dim(ans<-x |> group_by(id4) |> select(v1:v3) |> mean())))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans |> select(v1, v2, v3) |> sum())[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> group_by(id4) |> select(v1:v3) |> mean())))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans |> select(v1, v2, v3) |> sum())[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v1:v3 by id6" # q5
t = system.time(print(dim(ans<-x |> group_by(id6) |> select(v1:v3) |> sum(fill = TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans |> select(v1, v2, v3) |> sum())[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> group_by(id6) |> select(v1:v3) |> sum(fill = TRUE))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans |> select(v1, v2, v3) |> sum())[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "median v3 sd v3 by id4 id5" # q6
t = system.time(print(dim(ans<-x |> group_by(id4, id5) |> summarize(v3_median = median(v3), v3_sd = sd(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans |> select(v3_median, v3_sd) |> sum())[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> group_by(id4, id5) |> summarize(v3_median = median(v3), v3_sd = sd(v3)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-ans |> select(v3_median, v3_sd) |> sum())[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "max v1 - min v2 by id3" # q7
t = system.time(print(dim(ans<-x |> group_by(id3) |> summarise(range_v1_v2=max(v1)%-=%min(v2)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, range_v1_v2=sum(range_v1_v2)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> group_by(id3) |> summarise(range_v1_v2=max(v1)%-=%min(v2)))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, range_v1_v2=sum(range_v1_v2)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

# Note: this is a native collapse solution to this problem: utilizing the fast fnth() function and quickselect to get the second largest element

question = "largest two v3 by id6" # q8
t = system.time(print(dim(ans<-x |> group_by(id6) |> summarize(max_v3 = max(v3), second_v3 = nth(v3, .99999, ties = "min")) |> pivot("id6") |> compute(largest2_v3 = value, keep = "id6"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, largest2_v3=sum(largest2_v3)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> group_by(id6) |> summarize(max_v3 = max(v3), second_v3 = nth(v3, .99999, ties = "min")) |> pivot("id6") |> compute(largest2_v3 = value, keep = "id6"))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, largest2_v3=sum(largest2_v3)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)


# Previous: x |> group_by(id2, id4) |> mutate(tmp = scale(v1)%*=%scale(v2)) |> summarise(r2 = (sum(tmp)%/=%(nobs(tmp)%-=%1))^2)
question = "regression v1 v2 by id2 id4" # q9
t = system.time(print(dim(ans<-x |> group_by(id2, id4) |> summarise(r2 = cor(v1, v2, use = "na.or.complete")^2))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, r2=sum(r2)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> group_by(id2, id4) |> summarise(r2 = cor(v1, v2, use = "na.or.complete")^2))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, r2=sum(r2)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

question = "sum v3 count by id1:id6" # q10
t = system.time(print(dim(ans<-x |> group_by(id1:id6) |> summarise(v3=sum(v3, fill = TRUE), count=n()))))[["elapsed"]]
m = memory_usage()
chkt = system.time(chk<-summarise(ans, v3=sum(v3), count=sum(count)))[["elapsed"]]
write.log(run=1L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
rm(ans)
t = system.time(print(dim(ans<-x |> group_by(id1:id6) |> summarise(v3=sum(v3, fill = TRUE), count=n()))))[["elapsed"]]
mn = memory_usage()
chkt = system.time(chk<-summarise(ans, v3=sum(v3), count=sum(count)))[["elapsed"]]
write.log(run=2L, task=task, data=data_name, in_rows=nrow(x), question=question, out_rows=nrow(ans), out_cols=ncol(ans), solution=solution, version=ver, git=git, fun=fun, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(head(ans, 3))
print(tail(ans, 3))
rm(ans)

cat(sprintf("grouping finished, took %.0fs\n", proc.time()[["elapsed"]]-task_init))

set_collapse(oldopts)

if( !interactive() ) q("no", status=0)
