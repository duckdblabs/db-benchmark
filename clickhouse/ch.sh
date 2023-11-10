ch_installed() {
  dpkg-query -Wf'${db:Status-abbrev}' clickhouse-server 2>/dev/null | grep -q '^i'
}
ch_start() {
  echo '# ch_start: starting clickhouse-server'
  sudo service clickhouse-server start && sleep 15
}
ch_stop() {
  echo '# ch_stop: stopping clickhouse-server'
  sudo service clickhouse-server stop && sleep 15
}
ch_active() {
  clickhouse-client --query="SELECT 0;" > /dev/null 2>&1
  local ret=$?;
  if [[ $ret -eq 0 ]]; then return 0; elif [[ $ret -eq 210 ]]; then return 1; else echo "Unexpected return code from clickhouse-client: $ret" >&2 && return 1; fi;
}

ch_query() {
  clickhouse-client --query "DROP TABLE IF EXISTS ans;"
  clickhouse-client --log_comment ${RUNNAME} --query "CREATE TABLE ans ENGINE = Memory AS ${QUERY};"
  local ret=$?;
  if [[ $ret -eq 0 ]]; then return 0; elif [[ $ret -eq 210 ]]; then return 1; else echo "Unexpected return code from clickhouse-client: $ret" >&2 && return 1; fi;
  clickhouse-client --query "SELECT * FROM ans LIMIT 3;"
  clickhouse-client --query "DROP TABLE ans;"
}

ch_logrun() {
  clickhouse-client --query "SYSTEM FLUSH LOGS;"
  clickhouse-client --query "SELECT ${RUN} AS run, toUnixTimestamp(now()) AS timestamp, '${TASK}' AS task, '${SRC_DATANAME}' AS data_name, NULL AS in_rows, '${QUESTION}' AS question, result_rows AS out_rows, NULL AS out_cols, 'clickhouse' AS solution, version() AS version, NULL AS git, 'select group by' AS fun, query_duration_ms/1000 AS time_sec, memory_usage/1073741824 AS mem_gb, 1 AS cache, NULL AS chk, NULL AS chk_time_sec, 1 AS on_disk FROM system.query_log WHERE type='QueryFinish' AND log_comment='${RUNNAME}' ORDER BY query_start_time DESC LIMIT 1 FORMAT CSVWithNames;" > clickhouse/log/${RUNNAME}.csv
  local ret=$?;
  if [[ $ret -eq 0 ]]; then return 0; elif [[ $ret -eq 210 ]]; then return 1; else echo "Unexpected return code from clickhouse-client: $ret" >&2 && return 1; fi;
}

ch_make_2_runs() {
  RUN=1
  RUNNAME="${TASK}_${SRC_DATANAME}_q${Q}_r${RUN}"
  ch_query
  ch_logrun

  RUN=2
  RUNNAME="${TASK}_${SRC_DATANAME}_q${Q}_r${RUN}"
  ch_query
  ch_logrun
}