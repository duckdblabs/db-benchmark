#!/bin/bash
set -e

# install stable duckdb-latest
rm -rf ./duckdb-latest/r-duckdb-latest
mkdir -p ./duckdb-latest/r-duckdb-latest
# Rscript -e  'withr::with_libpaths(new = "./duckdb-latest/r-duckdb-latest", devtools::install_github("duckdb-latest/duckdb-latest/tools/rpkg"))'
# prevent errors when running 'ver-duckdb-latest.sh'
Rscript -e 'install.packages("DBI", lib="./duckdb-latest/r-duckdb-latest", repos = "http://cloud.r-project.org")'


cd duckdb-latest
rm -rf duckdb-r
git clone https://github.com/duckdb/duckdb-r.git
cd duckdb-r
git checkout next
cd ..
ncores=`python3 -c 'import multiprocessing as mp; print(mp.cpu_count())'`
MAKEFLAGS="-j$ncores" R CMD INSTALL -l "./r-duckdb-latest" duckdb-r
rm -rf duckdb-r
cd ..

./duckdb-latest/ver-duckdb-latest.sh
