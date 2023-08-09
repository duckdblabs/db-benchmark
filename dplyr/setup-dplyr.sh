#!/bin/bash
set -e

# install stable dplyr
mkdir -p ./dplyr/r-dplyr
Rscript -e 'install.packages("dplyr", lib="./dplyr/r-dplyr")'
Rscript -e 'install.packages("slider", lib="./dplyr/r-dplyr")' ## rollfun
Rscript -e 'install.packages("data.table", lib="./dplyr/r-dplyr")' ## data load
Rscript -e 'install.packages("rlang", lib="./dplyr/r-dplyr")' ## seems also be needed
Rscript -e 'install.packages("R6", lib="./dplyr/r-dplyr")' ## seems also be needed
Rscript -e 'install.packages("glue", lib="./dplyr/r-dplyr")' ## seems also be needed
Rscript -e 'install.packages("lifecycle", lib="./dplyr/r-dplyr")' ## seems also be needed
Rscript -e 'install.packages("magrittr", lib="./dplyr/r-dplyr")' ## seems also be needed
Rscript -e 'install.packages("cli", lib="./dplyr/r-dplyr")' ## seems also be needed
Rscript -e 'install.packages("fansi", lib="./dplyr/r-dplyr")' ## seems also be needed
Rscript -e 'install.packages("utf8", lib="./dplyr/r-dplyr")' ## seems also be needed
Rscript -e 'install.packages("pkgconfig", lib="./dplyr/r-dplyr")' ## seems also be needed

