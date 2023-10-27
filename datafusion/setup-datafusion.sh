#!/bin/bash
set -e

virtualenv datafusion/py-datafusion --python=python3
source datafusion/py-datafusion/bin/activate

python3 -m pip install --upgrade psutil datafusion pandas

# build
deactivate
./datafusion/upg-datafusion.sh

./datafusion/ver-datafusion.sh

# check
# source datafusion/py-datafusion/bin/activate
# python3
# import datafusion as df
# df.__version__
# quit()
# deactivate

# fix: print(ans.head(3), flush=True): UnicodeEncodeError: 'ascii' codec can't encode characters in position 14-31: ordinal not in range(128)
# vim datafusion/py-datafusion/bin/activate
#deactivate () {
#    unset PYTHONIOENCODING
#    ...
#}
#...
#PYTHONIOENCODING="utf-8"
#export PYTHONIOENCODING
#...
