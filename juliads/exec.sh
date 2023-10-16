#!/bin/bash
set -e

if [ "$#" -ne 1 ]; then
  echo 'usage: ./juliads/exec.sh groupby';
  exit 1
fi;

source ./path.env

ncores=`python3 -c 'import multiprocessing as mp; print(mp.cpu_count())'`

# execute benchmark script
julia -t $ncores ./juliads/$1-juliads.jl
