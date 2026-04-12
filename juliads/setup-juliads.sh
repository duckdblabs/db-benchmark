#!/bin/bash
# install julia

curl -fsSL https://install.julialang.org | sh -s -- -y
. /home/ubuntu/.bashrc

# install julia InMemoryDatasets and csv packages
julia -q -e 'using Pkg; Pkg.add(["InMemoryDatasets","DLMReader", "PooledArrays", "Arrow", "CSV"])'
julia -q -e 'include("$(pwd())/_helpers/helpersds.jl"); pkgmeta = getpkgmeta("InMemoryDatasets"); println(string(pkgmeta["version"])); pkgmeta = getpkgmeta("DLMReader"); println(string(pkgmeta["version"]))'

./juliadf/ver-juliadf.sh
