#!/bin/bash
set -e

# use this function to check error logs
# sudo -u clickhouse clickhouse-server --config=/etc/clickhouse-server/config.xml

if [ "$#" -ne 1 ]; then
  echo 'usage: ./clickhouse/exec.sh groupby';
  exit 1
fi;

source ./clickhouse/ch.sh
source ./_helpers/helpers.sh

# start server
ch_start

# confirm server working, wait if it crashed in last run
ch_active || sleep 20
ch_active || echo 'clickhouse-server should be already running, investigate' >&2
ch_active || exit 1


# tail -n+2 data/G1_1e7_1e2_0_0.csv | clickhouse-client --query="INSERT INTO G1_1e7_1e2_0_0 SELECT * FROM input('id1 Nullable(String), id2 Nullable(String), id3 Nullable(String), id4 Nullable(Int32), id5 Nullable(Int32), id6 Nullable(Int32), v1 Nullable(Int32), v2 Nullable(Int32), v3 Nullable(Float64)') FORMAT CSV"

# tune CH settings and load data
sudo touch '/var/lib/clickhouse/flags/force_drop_table' && sudo chmod 666 '/var/lib/clickhouse/flags/force_drop_table'
clickhouse-client --query 'DROP TABLE IF EXISTS ans'
echo '# clickhouse/exec.sh: creating tables and loading data'
# set ClickHouse parallelism at half of virtual cores
THREADS=$(($(nproc --all) /2))
HAS_NULL=$(clickhouse-client --query "SELECT splitByChar('_','$SRC_DATANAME')[4]>0 FORMAT TSV")
IS_SORTED=$(clickhouse-client --query "SELECT splitByChar('_','$SRC_DATANAME')[5]=1 FORMAT TSV")
ON_DISK=0

if [ $1 == 'groupby' ]; then
  clickhouse-client --query "DROP TABLE IF EXISTS $SRC_DATANAME"
  if [ $HAS_NULL -eq 1 ]; then
    if [ $IS_SORTED -eq 1 ]; then
      clickhouse-client --query "CREATE TABLE $SRC_DATANAME (id1 LowCardinality(Nullable(String)), id2 LowCardinality(Nullable(String)), id3 Nullable(String), id4 Nullable(Int32), id5 Nullable(Int32), id6 Nullable(Int32), v1 Nullable(Int32), v2 Nullable(Int32), v3 Nullable(Float64)) ENGINE = MergeTree() ORDER BY (id1,id2,id3,id4,id5,id6);"
    else
      clickhouse-client --query "CREATE TABLE $SRC_DATANAME (id1 LowCardinality(Nullable(String)), id2 LowCardinality(Nullable(String)), id3 Nullable(String), id4 Nullable(Int32), id5 Nullable(Int32), id6 Nullable(Int32), v1 Nullable(Int32), v2 Nullable(Int32), v3 Nullable(Float64)) ENGINE = MergeTree() ORDER BY tuple();"
    fi
  else
    if [ $IS_SORTED -eq 1 ]; then
      clickhouse-client --query "CREATE TABLE $SRC_DATANAME (id1 LowCardinality(String), id2 LowCardinality(String), id3 String, id4 Int32, id5 Int32, id6 Int32, v1 Int32, v2 Int32, v3 Float64) ENGINE = MergeTree() ORDER BY (id1,id2,id3,id4,id5,id6);"
    else
      clickhouse-client --query "CREATE TABLE $SRC_DATANAME (id1 LowCardinality(String), id2 LowCardinality(String), id3 String, id4 Int32, id5 Int32, id6 Int32, v1 Int32, v2 Int32, v3 Float64) ENGINE = MergeTree() ORDER BY tuple();"
    fi
  fi
  clickhouse-client --query "INSERT INTO $SRC_DATANAME FROM INFILE 'data/${SRC_DATANAME}.csv'"
  # confirm all data loaded
  echo -e "clickhouse-client --query 'SELECT count(*) FROM $SRC_DATANAME'\n$(echo $SRC_DATANAME | cut -d'_' -f2)" | Rscript -e 'stdin=readLines(file("stdin")); if ((loaded<-as.numeric(system(stdin[1L], intern=TRUE)))!=as.numeric(stdin[2L])) stop("incomplete data load, expected: ", stdin[2L],", loaded: ", loaded)'
  export THREADS
elif [ $1 == 'join' ]; then
  RHS=$(join_to_tbls $SRC_DATANAME)
  RHS1=$(echo $RHS | cut -d' ' -f1)
  RHS2=$(echo $RHS | cut -d' ' -f2)
  RHS3=$(echo $RHS | cut -d' ' -f3)
  ON_DISK=$(clickhouse-client --query "SELECT (splitByChar('_','$SRC_DATANAME')[2])::Float32 >= 1e9::Float32 FORMAT TSV")

  # cleanup
  clickhouse-client --query "DROP TABLE IF EXISTS $SRC_DATANAME"
  clickhouse-client --query "DROP TABLE IF EXISTS $RHS1"
  clickhouse-client --query "DROP TABLE IF EXISTS $RHS2"
  clickhouse-client --query "DROP TABLE IF EXISTS $RHS3"

  echo IS_SORTED ${IS_SORTED} HAS_NULL ${HAS_NULL} ON_DISK ${ON_DISK}
  # schemas
  if [ $HAS_NULL -eq 1 ]; then
    if [ $IS_SORTED -eq 1 ]; then
      clickhouse-client --query "CREATE TABLE $SRC_DATANAME (id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 LowCardinality(Nullable(String)), id5 LowCardinality(Nullable(String)), id6 Nullable(String), v1 Nullable(Float64)) ENGINE = MergeTree() ORDER BY (id1, id2, id3, id4, id5, id6);"
      clickhouse-client --query "CREATE TABLE $RHS1 (id1 Nullable(Int32), id4 LowCardinality(Nullable(String)), v2 Nullable(Float64)) ENGINE = MergeTree() ORDER BY  (id1, id4);"
      clickhouse-client --query "CREATE TABLE $RHS2 (id1 Nullable(Int32), id2 Nullable(Int32), id4 LowCardinality(Nullable(String)), id5 LowCardinality(Nullable(String)), v2 Nullable(Float64)) ENGINE = MergeTree() ORDER BY (id1, id2, id4, id5);"
      clickhouse-client --query "CREATE TABLE $RHS3 (id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 LowCardinality(Nullable(String)), id5 LowCardinality(Nullable(String)), id6 Nullable(String), v2 Nullable(Float64)) ENGINE = MergeTree() ORDER BY (id1, id2, id3, id4, id5, id6);"
    else
      clickhouse-client --query "CREATE TABLE $SRC_DATANAME (id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 LowCardinality(Nullable(String)), id5 LowCardinality(Nullable(String)), id6 Nullable(String), v1 Nullable(Float64)) ENGINE = MergeTree() ORDER BY tuple();"
      clickhouse-client --query "CREATE TABLE $RHS1 (id1 Nullable(Int32), id4 LowCardinality(Nullable(String)), v2 Nullable(Float64)) ENGINE = MergeTree() ORDER BY tuple();"
      clickhouse-client --query "CREATE TABLE $RHS2 (id1 Nullable(Int32), id2 Nullable(Int32), id4 LowCardinality(Nullable(String)), id5 LowCardinality(Nullable(String)), v2 Nullable(Float64)) ENGINE = MergeTree() ORDER BY tuple();"
      clickhouse-client --query "CREATE TABLE $RHS3 (id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 LowCardinality(Nullable(String)), id5 LowCardinality(Nullable(String)), id6 Nullable(String), v2 Nullable(Float64)) ENGINE = MergeTree() ORDER BY tuple();"
    fi
  else
    if [ $IS_SORTED -eq 1 ]; then
      clickhouse-client --query "CREATE TABLE $SRC_DATANAME (id1 Int32, id2 Int32, id3 Int32, id4 LowCardinality(String), id5 LowCardinality(String), id6 String, v1 Float64) ENGINE = MergeTree() ORDER BY (id1, id2, id3, id4, id5, id6);"
      clickhouse-client --query "CREATE TABLE $RHS1 (id1 Int32, id4 LowCardinality(String), v2 Float64) ENGINE = MergeTree() ORDER BY  (id1, id4);"
      clickhouse-client --query "CREATE TABLE $RHS2 (id1 Int32, id2 Int32, id4 LowCardinality(String), id5 LowCardinality(String), v2 Float64) ENGINE = MergeTree() ORDER BY (id1, id2, id4, id5);"
      clickhouse-client --query "CREATE TABLE $RHS3 (id1 Int32, id2 Int32, id3 Int32, id4 LowCardinality(String), id5 LowCardinality(String), id6 String, v2 Float64) ENGINE = MergeTree() ORDER BY (id1, id2, id3, id4, id5, id6);"
    else
      clickhouse-client --query "CREATE TABLE $SRC_DATANAME (id1 Int32, id2 Int32, id3 Int32, id4 LowCardinality(String), id5 LowCardinality(String), id6 String, v1 Float64) ENGINE = MergeTree() ORDER BY tuple();"
      clickhouse-client --query "CREATE TABLE $RHS1 (id1 Int32, id4 LowCardinality(String), v2 Float64) ENGINE = MergeTree() ORDER BY tuple();"
      clickhouse-client --query "CREATE TABLE $RHS2 (id1 Int32, id2 Int32, id4 LowCardinality(String), id5 LowCardinality(String), v2 Float64) ENGINE = MergeTree() ORDER BY tuple();"
      clickhouse-client --query "CREATE TABLE $RHS3 (id1 Int32, id2 Int32, id3 Int32, id4 LowCardinality(String), id5 LowCardinality(String), id6 String, v2 Float64) ENGINE = MergeTree() ORDER BY tuple();"
    fi
  fi

  # insert
  tail -n+2 data/$SRC_DATANAME.csv | clickhouse-client --query "INSERT INTO $SRC_DATANAME SELECT * FROM input('id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), id6 Nullable(String), v1 Nullable(Float64)') FORMAT CSV"
  tail -n+2 data/$RHS1.csv | clickhouse-client --query "INSERT INTO $RHS1 SELECT * FROM input('id1 Nullable(Int32), id4 Nullable(String), v2 Nullable(Float64)') FORMAT CSV"
  tail -n+2 data/$RHS2.csv | clickhouse-client --query "INSERT INTO $RHS2 SELECT * FROM input('id1 Nullable(Int32), id2 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), v2 Nullable(Float64)') FORMAT CSV"
  tail -n+2 data/$RHS3.csv | clickhouse-client --query "INSERT INTO $RHS3 SELECT * FROM input('id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), id6 Nullable(String), v2 Nullable(Float64)') FORMAT CSV"

  # validate
  echo -e "clickhouse-client --query 'SELECT count(*) FROM $SRC_DATANAME'\n$(echo $SRC_DATANAME | cut -d'_' -f2)" | Rscript -e 'stdin=readLines(file("stdin")); if ((loaded<-as.numeric(system(stdin[1L], intern=TRUE)))!=as.numeric(stdin[2L])) stop("incomplete data load, expected: ", stdin[2L],", loaded: ", loaded)'
  echo -e "clickhouse-client --query 'SELECT count(*) FROM $RHS1'\n$(echo $RHS1 | cut -d'_' -f3)" | Rscript -e 'stdin=readLines(file("stdin")); if ((loaded<-as.numeric(system(stdin[1L], intern=TRUE)))!=as.numeric(stdin[2L])) stop("incomplete data load, expected: ", stdin[2L],", loaded: ", loaded)'
  echo -e "clickhouse-client --query 'SELECT count(*) FROM $RHS2'\n$(echo $RHS2 | cut -d'_' -f3)" | Rscript -e 'stdin=readLines(file("stdin")); if ((loaded<-as.numeric(system(stdin[1L], intern=TRUE)))!=as.numeric(stdin[2L])) stop("incomplete data load, expected: ", stdin[2L],", loaded: ", loaded)'
  echo -e "clickhouse-client --query 'SELECT count(*) FROM $RHS3'\n$(echo $RHS3 | cut -d'_' -f3)" | Rscript -e 'stdin=readLines(file("stdin")); if ((loaded<-as.numeric(system(stdin[1L], intern=TRUE)))!=as.numeric(stdin[2L])) stop("incomplete data load, expected: ", stdin[2L],", loaded: ", loaded)'

  export RHS1
  export RHS2
  export RHS3
else
  echo "clickhouse task $1 not implemented" >&2 && exit 1
fi
export ON_DISK
export THREADS

# cleanup timings from last run if they have not been cleaned up after parsing
mkdir -p clickhouse/log
rm -f clickhouse/log/$1_${SRC_DATANAME}_q*.csv

# execute sql script on clickhouse
clickhouse-client --query 'TRUNCATE TABLE system.query_log'
echo "# clickhouse/exec.sh: data loaded, logs truncated, runnning $1-$SRC_DATANAME benchmark sh script"
if [ $1 == 'groupby' ] || [ $1 == 'join' ]; then
  "./clickhouse/$1-clickhouse.sh" && echo '# clickhouse/exec.sh: benchmark sh script finished' || echo "# clickhouse/exec.sh: benchmark sh script for $SRC_DATANAME terminated with error"
else
  echo "clickhouse task $1 benchmark script launching not defined" >&2 && exit 1
fi

# need to wait in case if server crashed to release memory
sleep 90

# cleanup data
sudo touch '/var/lib/clickhouse/flags/force_drop_table' && sudo chmod 666 '/var/lib/clickhouse/flags/force_drop_table'
ch_active && echo '# clickhouse/exec.sh: finishing, cleaning up' && clickhouse-client --query "DROP TABLE IF EXISTS ans" || echo '# clickhouse/exec.sh: finishing, clickhouse server down, could not clean up'
ch_active && clickhouse-client --query "DROP TABLE IF EXISTS $SRC_DATANAME" || echo '# clickhouse/exec.sh: finishing, clickhouse server down, could not clean up'
if [ $1 == 'join' ]; then
  ch_active && clickhouse-client --query "DROP TABLE IF EXISTS $RHS1" || echo '# clickhouse/exec.sh: finishing, clickhouse server down, could not clean up'
  ch_active && clickhouse-client --query "DROP TABLE IF EXISTS $RHS2" || echo '# clickhouse/exec.sh: finishing, clickhouse server down, could not clean up'
  ch_active && clickhouse-client --query "DROP TABLE IF EXISTS $RHS3" || echo '# clickhouse/exec.sh: finishing, clickhouse server down, could not clean up'
fi;

# stop server
ch_stop && echo '# clickhouse/exec.sh: stopping server finished' || echo '# clickhouse/exec.sh: stopping server failed'

# wait for memory
sleep 30

# parse timings from clickhouse/log/[task]_[data_name]_q[i]_r[j].csv
Rscript clickhouse/clickhouse-parse-log.R $1 $SRC_DATANAME
