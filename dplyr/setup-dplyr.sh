#!/bin/bash
set -e

# install stable dplyr
mkdir -p ./dplyr/r-dplyr
Rscript -e 'install.packages("dplyr", lib="./dplyr/r-dplyr", repos = "http://cloud.r-project.org")'
