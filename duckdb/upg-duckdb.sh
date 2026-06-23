#!/bin/bash
set -e

echo 'upgrading duckdb...'

source ./duckdb/py-duckdb/bin/activate

python3 -m pip install --upgrade duckdb > /dev/null

deactivate
