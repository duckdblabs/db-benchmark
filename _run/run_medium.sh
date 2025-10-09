./_run/download_small_medium.sh

cp _control/data_medium.csv _control/data.csv


echo "Running all solutions on medium (5GB) datasets"
./run.sh


###
echo "done..."
echo "removing small data files"
rm data/*.duckdb

