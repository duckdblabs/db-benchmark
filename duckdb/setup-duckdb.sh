#!/bin/bash
set -e

# install dependencies
# sudo apt-get update -qq

mkdir -p duckdb/py-duckdb

virtualenv duckdb/py-duckdb --python=python3
source duckdb/py-duckdb/bin/activate

# install the latest released duckdb python package plus runtime deps
python3 -m pip install --upgrade psutil duckdb

deactivate

./duckdb/upg-duckdb.sh

./duckdb/ver-duckdb.sh

# check
# source duckdb/py-duckdb/bin/activate
# python3
# import duckdb
# duckdb.__version__
# quit()
# deactivate
