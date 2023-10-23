#!/bin/bash
set -e

sudo apt-get update -qq
sudo apt-get install -y python3.6-dev virtualenv

virtualenv dask/py-dask --python=python3.6
source dask/py-dask/bin/activate

# install binaries
python3 -m pip install --upgrade dask[complete]
python3 -m pip install pandas logging

# check
# python3
# import dask as dk
# dk.__version__
# dk.__git_revision__
# quit()

deactivate
