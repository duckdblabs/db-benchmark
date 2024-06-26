#!/usr/bin/env Rscript

# ./_launcher/solution.R data.table
# ./_launcher/solution.R --solution=data.table --print=*
# ./_launcher/solution.R --solution=data.table --quiet=true
# ./_launcher/solution.R --solution=data.table --task=groupby --nrow=1e7 --quiet=true
# ./_launcher/solution.R --solution=data.table --task=groupby --nrow=1e7 --k=1e2 --na=0 --sort=0 --quiet=true
# ./_launcher/solution.R --solution=data.table --task=groupby --nrow=1e7 --k=1e2 --na=0 --sort=0 --quiet=true --print=question,run,time_sec
# ./_launcher/solution.R --solution=data.table --quiet=true --out=dt-grp.csv

# input ----

args = commandArgs(TRUE)
if (length(args)<1L)
  stop("example usage of ./_launcher/solution.R scripts launcher\n./_launcher/solution.R --solution=data.table --task=groupby --nrow=1e7 --k=1e2 --na=0 --sort=0 --quiet=true --print=question,run,time_sec")

args = strsplit(args, "=", fixed=TRUE)
if (length(unique(lengths(args)))!=1L)
  stop("all arguments must be named or unnamed, not mixed")

named = length(args[[1L]])==2L
if (named && any(sapply(args, function(x) substr(x[1L], 1, 2))!="--"))
  stop("named arguments must be prefixed with '--', in a form of --solution=x")

argsl = c("solution","task","nrow","k","na","sort","out","print","quiet")
if (named) {
  args = setNames(sapply(args, `[`, 2L), sapply(args, function(x) substr(x[1L], 3, nchar(x[1L]))))
  if (anyDuplicated(names(args)))
    stop("arguments names must be unique, use comma separated text for multiple values")
} else {
  args = setNames(unlist(args), argsl[seq_along(args)])
}
if (!all(names(args) %in% argsl))
  stop("allowed arguments are: ", paste(argsl, collapse=", "))

dict = read.csv("_control/solutions.csv", colClasses=c("character","character"))
dict = dict[dict$solution==args[["solution"]],, drop=FALSE] # some solutions might not have all tasks defined
dict[,1:2] = lapply(dict[,1:2], function(x) factor(x, levels=unique(x))) # retain order of levels

solutions = levels(dict$solution)
if (!args[["solution"]] %in% solutions)
  stop("unsupported solution in control/solutions.csv: ", args[["solution"]])

tasks = levels(dict$task)
if (!"task" %in% names(args)) {
  args[["task"]] = tasks[1L]
} else if (!args[["task"]] %in% tasks) {
  stop("unsupported task for solution in control/solutions.csv: ", args[["task"]])
}

datadict = read.csv("_control/data.csv", colClasses=c("character","character","character","character","character","character","integer"))
datadict = datadict[datadict$task==args[["task"]],, drop=FALSE]
datadict[,1:6] = lapply(datadict[,1:6], function(x) factor(x, levels=unique(x)))

nrows = levels(datadict$nrow)
if (!"nrow" %in% names(args)) {
  args[["nrow"]] = nrows[1L]
} else if (!args[["nrow"]] %in% nrows) {
  stop("unsupported nrow: ", args[["nrow"]])
}
if (args[["task"]]%in%c("groupby","groupby2014")) {
  ks = levels(datadict$k)
  if (!"k" %in% names(args)) {
    args[["k"]] = ks[1L]
  } else if (!args[["k"]] %in% ks) {
    stop("unsupported k: ", args[["k"]])
  }
} else {
  args[["k"]] = NA_character_
}
nas = levels(datadict$na)
if (!"na" %in% names(args)) {
  args[["na"]] = nas[1L]
} else if (!args[["na"]] %in% nas) {
  stop("unsupported na: ", args[["na"]])
}
sorts = levels(datadict$sort)
if (!"sort" %in% names(args)) {
  args[["sort"]] = sorts[1L]
} else if (!args[["sort"]] %in% sorts) {
  stop("unsupported sort: ", args[["sort"]])
}

stdout = !"out" %in% names(args)
if (stdout) {
  args[["out"]] = tempfile("dbb-time-", fileext=".csv")
  invisible(file.create(args[["out"]]))
} else {
  if (!dir.exists(dirname(args[["out"]]))) dir.create(dirname(args[["out"]]), recursive=TRUE)
  if (!file.exists(args[["out"]])) invisible(file.create(args[["out"]]))
}

if ("print" %in% names(args)) {
  if (!stdout)
    stop("'print' argument can only be used when printing to console, not specifying 'out' argument")
} else {
  args[["print"]] = "on_disk,question,run,time_sec"
}

if ("quiet" %in% names(args)) {
  if (!args[["quiet"]] %in% c("true","false","TRUE","FALSE"))
    stop("'quiet' argument must be logical")
} else {
  args[["quiet"]] = "FALSE"
}

# run env helpers ----

# solution to file ext mapping
file.ext = function(x) {
  ans = switch(
    x,
    "collapse"=, "data.table"=, "dplyr"=, "h2o"=, "R-arrow"=, "duckdb"="R", "duckdb-latest"="R",
    "pandas"="py", "spark"=, "pydatatable"=, "modin"=, "dask"=, "datafusion"=, "polars"="py",
    "clickhouse"="sh", "juliadf"="jl", "juliads"="jl"
  )
  if (is.null(ans)) stop(sprintf("solution %s does not have file extension defined in file.ext helper function", x))
  ans
}
# dynamic LHS in: Sys.setenv(var = value)
setenv = function(var, value, quiet=TRUE) {
  stopifnot(is.character(var), !is.na(var), length(value)==1L, is.atomic(value))
  qc = as.call(c(list(quote(Sys.setenv)), setNames(list(value), var)))
  if (!quiet) print(qc)
  eval(qc)
}
# encode data name
data.desc = function(task, nrow, k, na, sort) {
  if (task=="groupby") {
    prefix = "G1"
  } else if (task=="join") {
    prefix = "J1"
  } else if (task=="groupby2014") {
    prefix = "G0"
  } else {
    stop("undefined task in solution.R data.desc function")
  }
  sprintf("%s_%s_%s_%s_%s", prefix, nrow, k, na, sort)
}
# no dots solution name used in paths
solution.path = function(x) {
  gsub(".", "", x, fixed=TRUE)
}

# run ----

s = args[["solution"]]
t = args[["task"]]
d = data.desc(t, args[["nrow"]], args[["k"]], args[["na"]], args[["sort"]])

Sys.setenv("CSV_TIME_FILE"=args[["out"]])
setenv("SRC_DATANAME", d)

ns = solution.path(s)
ext = file.ext(s)
localcmd = if (s %in% c("clickhouse","h2o","juliadf", "juliads")) { # custom launcher bash script, for clickhouse h2o juliadf
  sprintf("exec.sh %s", t)
} else sprintf("%s-%s.%s", t, ns, ext)
cmd = sprintf("./%s/%s", ns, localcmd)
cmd
ret = system(cmd, ignore.stdout=as.logical(args[["quiet"]]))

Sys.unsetenv("SRC_DATANAME")
Sys.unsetenv("CSV_TIME_FILE")

if (stdout && file.size(args[["out"]])) {
  time = read.csv(args[["out"]], sep=",", header=TRUE, stringsAsFactors=FALSE)
  if (length(args[["print"]]) && args[["print"]]!="*") {
    cols = strsplit(args[["print"]], ",", fixed=TRUE)[[1L]]
    badcols = setdiff(cols, names(time))
    if (length(badcols)) {
      warning("'print' argument specifies not existing columns: ", paste(badcols, collapse=", "))
      cols = intersect(cols, names(time))
    }
    time = time[, cols, drop=FALSE]
  }
  print(time)
}

# close ----

q("no", status=ret)
