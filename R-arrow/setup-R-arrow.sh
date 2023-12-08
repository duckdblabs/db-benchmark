#!/bin/bash
set -e

# install stable arrow
mkdir -p ./R-arrow/r-arrow
Rscript -e 'install.packages(c("arrow","dplyr"), lib="./R-arrow/r-arrow")'
