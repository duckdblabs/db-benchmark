#!/bin/bash
set -e

# install stable duckdb
rm -rf ./duckdb/r-duckdb
mkdir -p ./duckdb/r-duckdb
# Rscript -e  'withr::with_libpaths(new = "./duckdb/r-duckdb", devtools::install_github("duckdb/duckdb/tools/rpkg"))'
# prevent errors when running 'ver-duckdb.sh'
Rscript -e 'install.packages("DBI", lib="./duckdb/r-duckdb", repos = "http://cloud.r-project.org")'
ncores=`python3 -c 'import multiprocessing as mp; print(mp.cpu_count())'`
MAKE="make -j$ncores" Rscript -e 'install.packages("duckdb", lib="./duckdb/r-duckdb", repos = "http://cloud.r-project.org")'

./duckdb/ver-duckdb.sh
