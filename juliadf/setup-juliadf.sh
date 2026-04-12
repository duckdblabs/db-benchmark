#!/bin/bash
# install julia

curl -fsSL https://install.julialang.org | sh -s -- -y
. /home/ubuntu/.bashrc


# install julia dataframes and csv packages
julia -q -e 'using Pkg; Pkg.add(["DataFrames","CSV"])'
julia -q -e 'include("$(pwd())/_helpers/helpers.jl"); pkgmeta = getpkgmeta("DataFrames"); println(string(pkgmeta["version"])); pkgmeta = getpkgmeta("CSV"); println(string(pkgmeta["version"]))'

./juliadf/ver-juliadf.sh