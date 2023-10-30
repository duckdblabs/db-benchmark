#!/bin/bash
set -e

# install stable collapse
mkdir -p ./collapse/r-collapse
Rscript -e 'install.packages(c("Rcpp", "collapse"), lib="./collapse/r-collapse", repos = "http://cloud.r-project.org")'
