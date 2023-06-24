#!/bin/bash
set -e

# install stable dplyr
mkdir -p ./dplyr/r-dplyr
Rscript -e 'install.packages("dplyr", lib="./dplyr/r-dplyr")'
Rscript -e 'install.packages("slider", lib="./dplyr/r-dplyr")' ## rollfun
Rscript -e 'install.packages("data.table", lib="./dplyr/r-dplyr")' ## data load
