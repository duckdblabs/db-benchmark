#!/bin/bash
set -e

# install stable dplyr
mkdir -p ./dplyr/r-dplyr
Rscript -e 'install.packages("dplyr", lib="./dplyr/r-dplyr")'
Rscript -e 'install.packages(c("bit64"), dependecies=TRUE, repos="https://cloud.r-project.org")'
