#!/bin/bash
set -e

# upgrade all packages in duckdb library only if new arrow is out
echo 'upgrading duckdb-latest, installing 0.9.1'

rm -rf ./duckdb-latest/r-duckdb-latest
mkdir -p ./duckdb-latest/r-duckdb-latest
Rscript -e 'install.packages("DBI", lib="./duckdb-latest/r-duckdb-latest", repos = "http://cloud.r-project.org")'


cd duckdb-latest
rm -rf duckdb-r
git clone https://github.com/duckdb/duckdb-r
ncores=`python3 -c 'import multiprocessing as mp; print(mp.cpu_count())'`
MAKE="make -j$ncores" R CMD INSTALL -l "./r-duckdb-latest" duckdb-r
rm -rf duckdb-r
cd ..

./duckdb-latest/ver-duckdb-latest.sh