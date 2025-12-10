#!/bin/bash
set -e

source ./dask/py-dask/bin/activate
python3 -c 'import dask as dk; open("dask/VERSION","w").write(dk.__version__)' > /dev/null