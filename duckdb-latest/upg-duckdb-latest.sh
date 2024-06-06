#!/bin/bash
set -e

# upgrade all packages in duckdb library only if new arrow is out
echo 'upgrading duckdb-latest, installing 0.9.1'

rm -rf ./duckdb-latest/r-duckdb-latest
mkdir -p ./duckdb-latest/r-duckdb-latest


cd duckdb-latest
git clone https://github.com/duckdb/duckdb-r
cd duckdb-r 
git checkout v1.0.0
cd ..
ncores=$(nproc --all)
MAKE="make -j$ncores" R CMD INSTALL -l "./r-duckdb-latest" duckdb-r
rm -rf duckdb-r
cd ..
