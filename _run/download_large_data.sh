# download and expand large data

# get groupby large (50GB datasets)
if [ ! -f data/groupby_large.duckdb ]; then
	aws s3 cp s3://duckdb-data-for-ec2-regression-tests/db-benchmark-data/groupby_large.duckdb data/groupby_large.duckdb --quiet
fi

# get join large (50GB datasets)
if [ ! -f data/join_large.duckdb ]; then
	aws s3 cp s3://duckdb-data-for-ec2-regression-tests/db-benchmark-data/join_large.duckdb data/join_large.duckdb --quiet
fi


# expand groupby-large datasets to csv
duckdb data/groupby_large.duckdb  -c "copy G1_1e9_1e2_0_0 to 'data/G1_1e9_1e2_0_0.csv' (FORMAT CSV)"
duckdb data/groupby_large.duckdb  -c "copy G1_1e9_1e1_0_0 to 'data/G1_1e9_1e1_0_0.csv' (FORMAT CSV)"
duckdb data/groupby_large.duckdb  -c "copy G1_1e9_2e0_0_0 to 'data/G1_1e9_2e0_0_0.csv' (FORMAT CSV)"
duckdb data/groupby_large.duckdb  -c "copy G1_1e9_1e2_0_1 to 'data/G1_1e9_1e2_0_1.csv' (FORMAT CSV)"
duckdb data/groupby_large.duckdb  -c "copy G1_1e9_1e2_5_0 to 'data/G1_1e9_1e2_5_0.csv' (FORMAT CSV)"


# expand join-large datasets to csv
duckdb data/join_large.duckdb  -c "copy J1_1e9_NA_0_0 to 'data/J1_1e9_NA_0_0.csv' (FORMAT CSV)"
duckdb data/join_large.duckdb  -c "copy J1_1e9_1e9_0_0 to 'data/J1_1e9_1e9_0_0.csv' (FORMAT CSV)"
duckdb data/join_large.duckdb  -c "copy J1_1e9_1e6_0_0 to 'data/J1_1e9_1e6_0_0.csv' (FORMAT CSV)"
duckdb data/join_large.duckdb  -c "copy J1_1e9_1e3_0_0 to 'data/J1_1e9_1e3_0_0.csv' (FORMAT CSV)"

