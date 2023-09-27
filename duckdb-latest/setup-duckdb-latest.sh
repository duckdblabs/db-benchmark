#!/bin/bash
set -e

# install stable duckdb
mkdir -p ./duckdb-latest/r-duckdb-latest
# Rscript -e  'withr::with_libpaths(new = "./duckdb-latest/r-duckdb-latest", devtools::install_github("duckdb/duckdb/tools/rpkg"))'
# prevent errors when running 'ver-duckdb.sh'
Rscript -e 'install.packages("DBI", lib="./duckdb-latest/r-duckdb-latest")'


cd duckdb-latest
git clone https://github.com/duckdb/duckdb-r.git
MAKE="make -j30" R CMD INSTALL -l "./r-duckdb-latest" .
rm -rf duckdb
cd ..
