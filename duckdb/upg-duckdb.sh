#!/bin/bash
set -e

# upgrade the duckdb python package in the existing virtual environment to the
# latest released version. If the venv is missing, run ./duckdb/setup-duckdb.py first.
source ./duckdb/py-duckdb/bin/activate
python3 -m pip install --upgrade duckdb
deactivate

./duckdb/ver-duckdb.sh
