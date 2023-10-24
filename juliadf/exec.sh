#!/bin/bash
set -e

if [ "$#" -ne 1 ]; then
  echo 'usage: ./juliadf/exec.sh groupby';
  exit 1
fi;

source ./path.env

ncores=`python3 -c 'import multiprocessing as mp; print(mp.cpu_count())'`

# execute benchmark script
julia -t $ncores ./juliadf/$1-juliadf.jl
