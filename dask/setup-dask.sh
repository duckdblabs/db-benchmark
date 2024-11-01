#!/bin/bash
set -e

virtualenv dask/py-dask --python=python3
source dask/py-dask/bin/activate

# install binaries
python3 -m pip install "dask[complete]"

# check
# python3
# import dask as dk
# dk.__version__
# dk.__git_revision__
# quit()

deactivate
