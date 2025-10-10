# download and expand large data

rm data/*.csv
rm data/*.duckdb

./_run/download_large_data.sh

cp _control/data_large.csv _control/data.csv

echo "Running all solutions on large (50GB) datasets"
./run.sh


###
echo "done..."
echo "removing data files"
rm data/*.csv
rm data/*.duckdb
