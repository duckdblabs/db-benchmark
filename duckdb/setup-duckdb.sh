#!/bin/bash
set -e

# install stable duckdb
mkdir -p ./duckdb-latest/r-duckdb-latest
# Rscript -e  'withr::with_libpaths(new = "./duckdb-latest/r-duckdb-latest", devtools::install_github("duckdb/duckdb/tools/rpkg"))'
# prevent errors when running 'ver-duckdb.sh'
Rscript -e 'install.packages("DBI", lib="./duckdb-latest/r-duckdb-latest", repos = "http://cran.us.r-project.org")'


cd duckdb-latest
git clone https://github.com/duckdb/duckdb-r.git
cd duckdb-r
git checkout v0.8.1-3
cd ..
ncores=`python3 -c 'import multiprocessing as mp; print(mp.cpu_count())'`
MAKE="make -j$ncores" R CMD INSTALL -l "./r-duckdb-latest" duckdb-r
rm -rf duckdb-r
cd ..
