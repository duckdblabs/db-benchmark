#!/bin/bash
set -e

# install stable duckdb
mkdir -p ./duckdb/r-duckdb
# Rscript -e  'withr::with_libpaths(new = "./duckdb/r-duckdb", devtools::install_github("duckdb/duckdb/tools/rpkg"))'
# prevent errors when running 'ver-duckdb.sh'
Rscript -e 'install.packages("DBI", lib="./duckdb/r-duckdb", repos = "http://cloud.r-project.org")'


cd duckdb
git clone https://github.com/duckdb/duckdb-r.git
cd duckdb-r
git checkout v1.1.0
cd ..
ncores=`python3 -c 'import multiprocessing as mp; print(mp.cpu_count())'`
MAKE="make -j$ncores" R CMD INSTALL -l "./r-duckdb" duckdb-r
rm -rf duckdb-r
cd ..

./duckdb/ver-duckdb.sh
