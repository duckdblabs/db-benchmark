#!/bin/bash
set -e

# upgrade all packages in duckdb library only if new arrow is out
echo 'upgrading duckdb, installing 0.8.1'

rm -rf ./duckdb/r-duckdb
mkdir -p ./duckdb/r-duckdb


cd duckdb
git clone https://github.com/duckdb/duckdb-r
cd duckdb-r 
git checkout v1.0.0
cd ..
ncores=$(nproc --all)
MAKE="make -j$ncores" R CMD INSTALL -l "./r-duckdb" duckdb-r
rm -rf duckdb-r
cd ..


# Rscript -e 'ap=available.packages(repos="https://cloud.r-project.org/"); if (ap["duckdb","Version"]!=packageVersion("duckdb", lib.loc="./duckdb/r-duckdb")) update.packages(lib.loc="./duckdb/r-duckdb", ask=FALSE, checkBuilt=TRUE, quiet=TRUE, repos="https://cloud.r-project.org/")'
