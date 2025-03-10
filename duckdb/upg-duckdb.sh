#!/bin/bash
set -e

rm -rf ./duckdb/r-duckdb
mkdir -p ./duckdb/r-duckdb


cd duckdb
rm -rf duckdb-r
git clone https://github.com/duckdb/duckdb-r
cd duckdb-r 
git checkout v1.2.0
cd ..
ncores=$(nproc --all)
MAKE="make -j$ncores" R CMD INSTALL -l "./r-duckdb" duckdb-r
rm -rf duckdb-r
cd ..


# Rscript -e 'ap=available.packages(repos="https://cloud.r-project.org/"); if (ap["duckdb","Version"]!=packageVersion("duckdb", lib.loc="./duckdb/r-duckdb")) update.packages(lib.loc="./duckdb/r-duckdb", ask=FALSE, checkBuilt=TRUE, quiet=TRUE, repos="https://cloud.r-project.org/")'
