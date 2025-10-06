CREATE USER IF NOT EXISTS db_benchmark IDENTIFIED WITH no_password SETTINGS max_memory_usage = 28000000000 WRITABLE;
GRANT select, insert, create, alter, alter user, drop on *.* to db_benchmark;

ALTER USER db_benchmark SETTINGS max_memory_usage_for_user = 28000000000;


CREATE TABLE G1_1e9_1e1_0_0 (id1 LowCardinality(Nullable(String)), id2 LowCardinality(Nullable(String)), id3 Nullable(String), id4 Nullable(Int32), id5 Nullable(Int32), id6 Nullable(Int32), v1 Nullable(Int32), v2 Nullable(Int32), v3 Nullable(Float64)) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO G1_1e9_1e1_0_0 FROM INFILE 'data/G1_1e9_1e1_0_0.csv';

# q1
CREATE TABLE ans ENGINE = MergeTree ORDER BY tuple() AS SELECT id1, sum(v1) AS v1 FROM G1_1e9_1e1_0_0 GROUP BY id1 SETTINGS max_insert_threads=32, max_threads=32;

drop table if exists ans;
CREATE TABLE ans ENGINE = MergeTree ORDER BY tuple() AS SELECT id1, id2, sum(v1) AS v1 FROM G1_1e9_1e1_0_0 GROUP BY id1, id2  SETTINGS max_insert_threads=32, max_threads=32;

drop table if exists ans;
CREATE TABLE ans ENGINE = MergeTree ORDER BY tuple() AS SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM G1_1e9_1e1_0_0 GROUP BY id3 SETTINGS max_insert_threads=16, max_threads=16;

drop table if exists ans;
CREATE TABLE ans ENGINE = MergeTree ORDER BY tuple() AS SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM G1_1e9_1e1_0_0 GROUP BY id4 SETTINGS max_insert_threads=32, max_threads=32;

drop table if exists ans;
CREATE TABLE ans ENGINE = MergeTree ORDER BY tuple() AS SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM G1_1e9_1e1_0_0 GROUP BY id6 SETTINGS max_insert_threads=32, max_threads=32;

drop table if exists ans;
CREATE TABLE ans ENGINE = MergeTree ORDER BY tuple() AS SELECT id4, id5, medianExact(v3) AS median_v3, stddevPop(v3) AS sd_v3 FROM G1_1e9_1e1_0_0 GROUP BY id4, id5 SETTINGS max_insert_threads=32, max_threads=32;

drop table if exists ans;
CREATE TABLE ans ENGINE = MergeTree ORDER BY tuple() AS SELECT id3, max(v1) - min(v2) AS range_v1_v2 FROM G1_1e9_1e1_0_0 GROUP BY id3 SETTINGS max_insert_threads=32, max_threads=32;

drop table if exists ans;
CREATE TABLE ans ENGINE = MergeTree ORDER BY tuple() AS SELECT id6, arrayJoin(arraySlice(arrayReverseSort(groupArray(v3)), 1, 2)) AS v3 FROM (SELECT id6, v3 FROM G1_1e9_1e1_0_0 WHERE v3 IS NOT NULL) AS subq GROUP BY id6 SETTINGS max_insert_threads=32, max_threads=32;

drop table if exists ans;
CREATE TABLE ans ENGINE = MergeTree ORDER BY tuple() AS SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM G1_1e9_1e1_0_0 GROUP BY id2, id4 SETTINGS max_insert_threads=32, max_threads=32;

drop table if exists ans;

#q10 

CREATE TABLE ans ENGINE = MergeTree ORDER BY tuple() AS SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count() AS cnt FROM G1_1e9_1e1_0_0 GROUP BY id1, id2, id3, id4, id5, id6 SETTINGS max_insert_threads=32, max_threads=32;