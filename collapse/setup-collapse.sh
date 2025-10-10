#!/bin/bash
set -e

# install stable collapse
mkdir -p ./collapse/r-collapse
ncores=`python3 -c 'import multiprocessing as mp; print(mp.cpu_count())'`
MAKEFLAGS="-j$ncores" Rscript -e 'install.packages(c("Rcpp", "collapse"), lib="./collapse/r-collapse", repos = "http://cloud.r-project.org")'

./collapse/ver-collapse.sh