#!/bin/bash
set -e

source ./duckdb/py-duckdb/bin/activate
python3 -c "import duckdb; open('duckdb/VERSION','w').write(duckdb.__version__); open('duckdb/REVISION','w').write(duckdb.connect().execute('SELECT source_id FROM pragma_version()').fetchone()[0]);" > /dev/null
deactivate
