
source("./_report/report.R", chdir=TRUE)
source("./_helpers/helpers.R", chdir=TRUE)
source("./_benchplot/benchplot.R", chdir=TRUE)
source("./_benchplot/benchplot-dict.R", chdir=TRUE)
ld = time_logs()
lld = ld[script_recent==TRUE]
# lld_nodename = as.character(unique(lld$nodename))
lld_nodename = "c6id.metal"
if (length(lld_nodename)>1L)
  stop(sprintf("There are multiple different 'nodename' to be presented on single report '%s'", report_name))
lld_unfinished = lld[is.na(script_time_sec)]
if (nrow(lld_unfinished)) {
  warning(sprintf("Missing solution finish timestamp in logs.csv for '%s' (still running or launcher script killed): %s", paste(unique(lld_unfinished$task), collapse=","), paste(unique(lld_unfinished$solution), collapse=", ")))
}

dt_groupby = lld[task=="groupby"][substr(data,1,2)=="G1"]
dt_join = lld[task=="join"]


loop_benchplot = function(dt_task, report_name, syntax.dict, exceptions, solution.dict, question.txt.fun = NULL, title.txt.fun = NULL, data_namev, q_groupv, cutoff=NULL, pending=NULL) {
  for (data_name in data_namev) {
  for (q_group in q_groupv) {
    message(sprintf("benchplot %s %s %s", report_name, data_name, q_group))
    message(sprintf("machine type = %s", m_type))
    y = dt_task[data==data_name & question_group==q_group & machine_type==m_type][,machine_type := NULL]
    benchplot(
      y,
      filename = file.path("public", report_name, sprintf("%s_%s_%s.png", data_name, q_group, m_type)),
      solution.dict = solution.dict,
      syntax.dict = syntax.dict,
      exceptions = exceptions,
      question.txt.fun = question.txt.fun,
      title.txt.fun = title.txt.fun,
      cutoff = cutoff,
      pending = pending,
      url.footer = "https://duckdblabs.github.io/db-benchmark",
      interactive = FALSE
    )
    }
  }
}
link = function(data_name, q_group, report_name) {
  fnam = sprintf("%s_%s.png", data_name, q_group)
  paste(sprintf("[%s](%s)", q_group, file.path(report_name, fnam)), collapse=", ")
}
hours_took = function(lld) {
  lld_script_time = lld[, .(n_script_time_sec=uniqueN(script_time_sec), script_time_sec=unique(script_time_sec)), .(solution, task, data)]
  if (nrow(lld_script_time[n_script_time_sec>1L]))
    stop("There are multiple different 'script_time_sec' for single solution+task+data on report 'index'")
  lld_script_time[, round(sum(script_time_sec, na.rm=TRUE)/60/60, 1)]
}

data_name = get_data_levels()[["groupby"]]
loop_benchplot(dt_groupby, report_name="groupby", syntax.dict=groupby.syntax.dict, exceptions=groupby.exceptions, solution.dict=solution.dict, data_namev=data_name, q_groupv=c("basic","advanced"), title.txt.fun = header_title_fun, question.txt.fun = groupby_q_title_fun, cutoff = "spark", pending = "Modin", machine_types)