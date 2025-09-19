./_run/download_small_medium.sh

cp _control/data_small.csv _control/data.csv


echo "Running all solutions on small (0.5GB) datasets"
./run.sh


###
echo "done..."
echo "removing small data files"
rm data/*.csv
rm data/*.duckdb

