#!/bin/bash
set -e

# install stable arrow
mkdir -p ./R-arrow/r-arrow
ncores=`python3 -c 'import multiprocessing as mp; print(mp.cpu_count())'`
MAKE="make -j$ncores" Rscript -e 'install.packages(c("arrow","dplyr"), lib="./R-arrow/r-arrow")'

./R-arrow/ver-R-arrow.sh
