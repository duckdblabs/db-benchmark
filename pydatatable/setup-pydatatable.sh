#!/bin/bash
set -e

# install dependencies
virtualenv pydatatable/py-pydatatable --python=python3
source pydatatable/py-pydatatable/bin/activate

python -m pip install --upgrade psutil

# # build
deactivate
./pydatatable/upg-pydatatable.sh

# # check
# source pydatatable/py-pydatatable/bin/activate
# python
# import datatable as dt
# dt.__version__
# quit()
# deactivate

# resave 1e9 join data from csv to jay format so pydt can try out-of-memory processing
source pydatatable/py-pydatatable/bin/activate
python3 pydatatable/convert-pydatatable-data.py

./pydatatable/ver-pydatatable.sh