#!/bin/bash
set -e

# install stable collapse
mkdir -p ./collapse/r-collapse
Rscript -e 'install.packages(c("collapse", "dplyr"), lib="./collapse/r-collapse")'
