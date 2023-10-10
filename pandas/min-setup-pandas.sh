#!/bin/bash
set -e

# remove apt install calls as they break github action scipt
virtualenv pandas/py-pandas --python=python3
source pandas/py-pandas/bin/activate

# install binaries
python3 -m pip install --upgrade psutil
python3 -m pip install --upgrade pandas
python3 -m pip install --upgrade pyarrow

deactivate

# # check
# source pandas/py-pandas/bin/activate
# python3
# import pandas as pd
# pd.__version__
# quit()
# deactivate
