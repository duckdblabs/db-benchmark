#!/bin/bash
set -e

source ./chdb/py-chdb/bin/activate
python3 -c 'import chdb; open("chdb/VERSION","w").write(".".join(chdb.chdb_version)); open("chdb/REVISION","w").write("");' > /dev/null
