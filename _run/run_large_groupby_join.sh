# download and expand large data

# get groupby large (0.5GB and 5GB datasets)
aws s3 cp s3://duckdb-blobs/data/db-benchmark-data/groupby_large.duckdb data/groupby_large.duckdb
# get join small (0.5GB and 5GB datasets)
aws s3 cp s3://duckdb-blobs/data/db-benchmark-data/join_large.duckdb data/join_large.duckdb


# expand groupby-small datasets to csv
duckdb data/groupby_large.duckdb  -c "copy G1_1e9_1e2_0_0 to 'data/G1_1e9_1e2_0_0.csv' (FORMAT CSV)"
duckdb data/groupby_large.duckdb  -c "copy G1_1e9_1e1_0_0 to 'data/G1_1e9_1e1_0_0.csv' (FORMAT CSV)"
duckdb data/groupby_large.duckdb  -c "copy G1_1e9_2e0_0_0 to 'data/G1_1e9_2e0_0_0.csv' (FORMAT CSV)"
duckdb data/groupby_large.duckdb  -c "copy G1_1e9_1e2_0_1 to 'data/G1_1e9_1e2_0_1.csv' (FORMAT CSV)"
duckdb data/groupby_large.duckdb  -c "copy G1_1e9_1e2_5_0 to 'data/G1_1e9_1e2_5_0.csv' (FORMAT CSV)"

# expand join-small datasets to csv
duckdb data/join_large.duckdb  -c "copy J1_1e9_NA_0_0 to 'data/J1_1e9_NA_0_0.csv' (FORMAT CSV)"
duckdb data/join_large.duckdb  -c "copy J1_1e9_1e9_0_0 to 'data/J1_1e9_1e9_0_0.csv' (FORMAT CSV)"
duckdb data/join_large.duckdb  -c "copy J1_1e9_1e6_0_0 to 'data/J1_1e9_1e6_0_0.csv' (FORMAT CSV)"
duckdb data/join_large.duckdb  -c "copy J1_1e9_1e3_0_0 to 'data/J1_1e9_1e3_0_0.csv' (FORMAT CSV)"


echo "Running all solutions on large (50GB) datasets"
./run.sh


###
echo "done..."
echo "removing data files"
#rm data/*.csv
#rm data/*.duckdb
