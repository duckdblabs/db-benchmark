#!/bin/bash
set -e

# install stable duckdb
mkdir -p ./duckdb/r-duckdb
# instal duckdb 0.8.1-3
# TODO: update to 0.9.0
Rscript -e 'install.packages("https://cran.r-project.org/src/contrib/duckdb_0.8.1-3.tar.gz", repos="https://cloud.r-project.org/", lib="./duckdb/r-duckdb")'
# prevent errors when running 'ver-duckdb.sh'
Rscript -e 'install.packages("DBI", lib="./duckdb/r-duckdb")'
