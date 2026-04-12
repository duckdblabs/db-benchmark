./_run/download_large.sh

cp _control/data_large.csv _control/data.csv

echo "Running all solutions on large (50GB) datasets"
./run.sh


##
echo "done..."
echo "removing data files"
rm data/*.csv
rm data/*.duckdb
