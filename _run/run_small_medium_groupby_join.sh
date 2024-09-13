# first download and expand small data

# get groupby small (0.5GB and 5GB datasets)
aws s3 cp s3://duckdb-blobs/data/db-benchmark-data/groupby_small.duckdb data/groupby_small.duckdb
# get join small (0.5GB and 5GB datasets)
aws s3 cp s3://duckdb-blobs/data/db-benchmark-data/join_small.duckdb data/join_small.duckdb


# expand groupby-small datasets to csv
duckdb data/groupby_small.duckdb -c "copy G1_1e7_1e2_0_0 to 'data/G1_1e7_1e2_0_0.csv' (FORMAT CSV)"
duckdb data/groupby_small.duckdb -c "copy G1_1e7_1e1_0_0 to 'data/G1_1e7_1e1_0_0.csv' (FORMAT CSV)"
duckdb data/groupby_small.duckdb -c "copy G1_1e7_2e0_0_0 to 'data/G1_1e7_2e0_0_0.csv' (FORMAT CSV)"
duckdb data/groupby_small.duckdb -c "copy G1_1e7_1e2_0_1 to 'data/G1_1e7_1e2_0_1.csv' (FORMAT CSV)"
duckdb data/groupby_small.duckdb -c "copy G1_1e7_1e2_5_0 to 'data/G1_1e7_1e2_5_0.csv' (FORMAT CSV)"
duckdb data/groupby_small.duckdb -c "copy G1_1e8_1e2_0_0 to 'data/G1_1e8_1e2_0_0.csv' (FORMAT CSV)"
duckdb data/groupby_small.duckdb -c "copy G1_1e8_1e1_0_0 to 'data/G1_1e8_1e1_0_0.csv' (FORMAT CSV)"
duckdb data/groupby_small.duckdb -c "copy G1_1e8_2e0_0_0 to 'data/G1_1e8_2e0_0_0.csv' (FORMAT CSV)"
duckdb data/groupby_small.duckdb -c "copy G1_1e8_1e2_0_1 to 'data/G1_1e8_1e2_0_1.csv' (FORMAT CSV)"
duckdb data/groupby_small.duckdb -c "copy G1_1e8_1e2_5_0 to 'data/G1_1e8_1e2_5_0.csv' (FORMAT CSV)"

# expand join-small datasets to csv
duckdb data/join_small.duckdb -c "copy J1_1e7_1e1_0_0 to 'data/J1_1e7_1e1_0_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e7_1e4_5_0 to 'data/J1_1e7_1e4_5_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e7_NA_0_1 to 'data/J1_1e7_NA_0_1.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e8_1e5_0_0 to 'data/J1_1e8_1e5_0_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e8_1e8_5_0 to 'data/J1_1e8_1e8_5_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e7_1e1_0_1 to 'data/J1_1e7_1e1_0_1.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e7_1e7_0_0 to 'data/J1_1e7_1e7_0_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e7_NA_5_0 to 'data/J1_1e7_NA_5_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e8_1e5_0_1 to 'data/J1_1e8_1e5_0_1.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e8_NA_0_0 to 'data/J1_1e8_NA_0_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e7_1e1_5_0 to 'data/J1_1e7_1e1_5_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e7_1e7_0_1 to 'data/J1_1e7_1e7_0_1.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e8_1e2_0_0 to 'data/J1_1e8_1e2_0_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e8_1e5_5_0 to 'data/J1_1e8_1e5_5_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e8_NA_0_1 to 'data/J1_1e8_NA_0_1.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e7_1e4_0_0 to 'data/J1_1e7_1e4_0_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e7_1e7_5_0 to 'data/J1_1e7_1e7_5_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e8_1e2_0_1 to 'data/J1_1e8_1e2_0_1.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e8_1e8_0_0 to 'data/J1_1e8_1e8_0_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e8_NA_5_0 to 'data/J1_1e8_NA_5_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e7_1e4_0_1 to 'data/J1_1e7_1e4_0_1.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e7_NA_0_0 to 'data/J1_1e7_NA_0_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e8_1e2_5_0 to 'data/J1_1e8_1e2_5_0.csv' (FORMAT CSV)"
duckdb data/join_small.duckdb -c "copy J1_1e8_1e8_0_1 to 'data/J1_1e8_1e8_0_1.csv' (FORMAT CSV)"


cp _control/data_small.csv _control/data.csv


echo "Running all solutions on small (0.5GB and 5GB) datasets"
./run.sh


###
echo "done..."
echo "removing small data files"
rm data/*.csv
rm data/*.duckdb
