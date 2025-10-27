#!/bin/bash
set -e

# install dependencies
# sudo apt-get update -qq

virtualenv chdb/py-chdb --python=python3
source chdb/py-chdb/bin/activate

python3 -m pip install --upgrade psutil chdb

# build
deactivate

./chdb/upg-chdb.sh

./chdb/ver-chdb.sh
