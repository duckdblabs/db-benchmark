#!/bin/bash
set -e

# install stable collapse
mkdir -p ./collapse/r-collapse
Rscript -e 'install.packages("collapse", lib="./collapse/r-collapse")'
