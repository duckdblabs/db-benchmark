
# get small data
wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/groupby_small.duckdb
~/duckdb groupby_small.duckdb -c "copy G1_1e7_1e2_0_0 to 'G1_1e7_1e2_0_0.csv' (FORMAT CSV)"
~/duckdb groupby_small.duckdb -c "copy G1_1e7_1e1_0_0 to 'G1_1e7_1e1_0_0.csv' (FORMAT CSV)"
~/duckdb groupby_small.duckdb -c "copy G1_1e7_2e0_0_0 to 'G1_1e7_2e0_0_0.csv' (FORMAT CSV)"
~/duckdb groupby_small.duckdb -c "copy G1_1e7_1e2_0_1 to 'G1_1e7_1e2_0_1.csv' (FORMAT CSV)"
~/duckdb groupby_small.duckdb -c "copy G1_1e7_1e2_5_0 to 'G1_1e7_1e2_5_0.csv' (FORMAT CSV)"
~/duckdb groupby_small.duckdb -c "copy G1_1e8_1e2_0_0 to 'G1_1e8_1e2_0_0.csv' (FORMAT CSV)"
~/duckdb groupby_small.duckdb -c "copy G1_1e8_1e1_0_0 to 'G1_1e8_1e1_0_0.csv' (FORMAT CSV)"
~/duckdb groupby_small.duckdb -c "copy G1_1e8_2e0_0_0 to 'G1_1e8_2e0_0_0.csv' (FORMAT CSV)"
~/duckdb groupby_small.duckdb -c "copy G1_1e8_1e2_0_1 to 'G1_1e8_1e2_0_1.csv' (FORMAT CSV)"
~/duckdb groupby_small.duckdb -c "copy G1_1e8_1e2_5_0 to 'G1_1e8_1e2_5_0.csv' (FORMAT CSV)"

wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/join_small.duckdb

# get large data
wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/join_large.duckdb

wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/groupby_large.duckdb
~/duckdb -c "copy G1_1e9_1e2_0_0 to 'G1_1e9_1e2_0_0.csv' (FORMAT CSV)"
~/duckdb -c "copy G1_1e9_1e1_0_0 to 'G1_1e9_1e1_0_0.csv' (FORMAT CSV)"
~/duckdb -c "copy G1_1e9_2e0_0_0 to 'G1_1e9_2e0_0_0.csv' (FORMAT CSV)"
~/duckdb -c "copy G1_1e9_1e2_0_1 to 'G1_1e9_1e2_0_1.csv' (FORMAT CSV)"
~/duckdb -c "copy G1_1e9_1e2_5_0 to 'G1_1e9_1e2_5_0.csv' (FORMAT CSV)"

# get 500GB data
wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/join-500gb.duckdb

# ??? 
wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/groupby-500gb.duckdb