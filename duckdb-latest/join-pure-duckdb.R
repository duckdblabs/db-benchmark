CREATE TABLE x_csv AS SELECT * FROM read_csv_auto('../data/J1_1e9_NA_0_0.csv');
CREATE TABLE small_csv AS SELECT * FROM read_csv_auto('../data/J1_1e9_1e3_0_0.csv');
CREATE TABLE medium_csv AS SELECT * FROM read_csv_auto('../data/J1_1e9_1e6_0_0.csv');
CREATE TABLE big_csv AS SELECT * FROM read_csv_auto('../data/J1_1e9_1e9_0_0.csv');

create table id_4_enum as SELECT id4 FROM x_csv UNION ALL SELECT id4 FROM small_csv UNION ALL SELECT id4 from medium_csv UNION ALL SELECT id4 from big_csv;
create table id_5_enum as SELECT id5 FROM x_csv UNION ALL SELECT id5 from medium_csv UNION ALL SELECT id5 from big_csv;

CREATE TYPE id4ENUM AS ENUM (select * from id_4_enum);
CREATE TYPE id5ENUM AS ENUM (select * from id_5_enum);

CREATE TABLE small(id1 INT64, id4 id4ENUM, v2 DOUBLE) as select * from small_csv;
CREATE TABLE medium(id1 INT64, id2 INT64, id4 id4ENUM, id5 id5ENUM, v2 DOUBLE);
insert into medium (select * from medium_csv);
CREATE TABLE big(id1 INT64, id2 INT64, id3 INT64, id4 id4ENUM, id5 id5ENUM, id6 VARCHAR, v2 DOUBLE);
insert into big (select * from big_csv);
CREATE TABLE x(id1 INT64, id2 INT64, id3 INT64, id4 id4ENUM, id5 id5ENUM, id6 VARCHAR, v1 DOUBLE);
insert into x (select * from x_csv);


DROP TABLE x_csv;
DROP TABLE small_csv;
DROP TABLE medium_csv;
DROP TABLE big_csv;

create table new_join_test.big as select * from big;
create table new_join_test.x as select * from x;
create table new_join_test.medium as select * from medium;
create table new_join_test.small as select * from small;