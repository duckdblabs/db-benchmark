ch_installed() {
  dpkg-query -Wf'${db:Status-abbrev}' clickhouse-server 2>/dev/null | grep -q '^i'
}

ch_active() {
  clickhouse-client --query="SELECT 0;" > /dev/null 2>&1
  local ret=$?;
  if [[ $ret -eq 0 ]]; then return 0; elif [[ $ret -eq 210 ]]; then return 1; else echo "Unexpected return code from clickhouse-client: $ret" >&2 && return 1; fi;
}

ch_wait() {
  for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]; then break ; else sleep 1; fi ; done
  ch_active
}

ch_start() {
  echo '# ch_start: starting clickhouse-server'
  sudo service clickhouse-server start
  ch_wait
}

ch_stop() {
  echo '# ch_stop: stopping clickhouse-server'
  sudo service clickhouse-server stop && sleep 15
}

ch_query() {
  ENGINE=Memory
  if [ $ON_DISK -eq 1 ]; then
  ENGINE="MergeTree ORDER BY tuple()"
  fi
  sudo touch '/var/lib/clickhouse/flags/force_drop_table' && sudo chmod 666 '/var/lib/clickhouse/flags/force_drop_table'
  clickhouse-client --query "DROP TABLE IF EXISTS ans;"
  clickhouse-client --log_comment ${RUNNAME} --query "CREATE TABLE ans ENGINE = ${ENGINE} AS ${QUERY} SETTINGS max_insert_threads=${THREADS}, max_threads=${THREADS};"
  local ret=$?;
  if [[ $ret -eq 0 ]]; then return 0; elif [[ $ret -eq 210 ]]; then return 1; else echo "Unexpected return code from clickhouse-client: $ret" >&2 && return 1; fi;
  clickhouse-client --query "SELECT * FROM ans LIMIT 3;"
  sudo touch '/var/lib/clickhouse/flags/force_drop_table' && sudo chmod 666 '/var/lib/clickhouse/flags/force_drop_table'
  clickhouse-client --query "DROP TABLE ans;"
}

ch_logrun() {
  clickhouse-client --query "SYSTEM FLUSH LOGS;"
  clickhouse-client --query "SELECT ${RUN} AS run, toUnixTimestamp(now()) AS timestamp, '${TASK}' AS task, '${SRC_DATANAME}' AS data_name, NULL AS in_rows, '${QUESTION}' AS question, result_rows AS out_rows, NULL AS out_cols, 'clickhouse' AS solution, version() AS version, NULL AS git, '${FUNCTION}' AS fun, query_duration_ms/1000 AS time_sec, memory_usage/1073741824 AS mem_gb, 1 AS cache, NULL AS chk, NULL AS chk_time_sec, 1 AS on_disk FROM system.query_log WHERE type='QueryFinish' AND log_comment='${RUNNAME}' ORDER BY query_start_time DESC LIMIT 1 FORMAT CSVWithNames;" > clickhouse/log/${RUNNAME}.csv
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