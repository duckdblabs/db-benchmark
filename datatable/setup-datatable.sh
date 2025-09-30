#!/bin/bash
# install devel data.table
mkdir -p ./datatable/r-datatable
Rscript -e 'install.packages("data.table", repos="https://Rdatatable.gitlab.io/data.table", lib="./datatable/r-datatable")'

./datatable/ver-datatable.sh
	