#!/bin/bash
set -e

# upgrade all packages in duckplyr library only if new duckplyr is out
echo 'upgrading duckplyr...'
Rscript -e 'ap=available.packages(); install.packages(c("duckplyr", "data.table"), lib="./duckplyr/r-duckplyr", ask=FALSE, checkBuilt=TRUE, quiet=TRUE)'
