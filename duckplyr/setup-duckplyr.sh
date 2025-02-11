#!/bin/bash
set -e

# install stable duckplyr
mkdir -p ./duckplyr/r-duckplyr
Rscript -e 'install.packages("duckplyr", lib="./duckplyr/r-duckplyr", repos = "http://cloud.r-project.org")'
