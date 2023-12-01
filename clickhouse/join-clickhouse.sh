#!/bin/bash
set -e
source ./clickhouse/ch.sh

SOLUTION=clickhouse
TASK=join

echo SRC ${SRC_DATANAME} RHS1 ${RHS1} RHS2 ${RHS2} RHS3 ${RHS3} ON_DISK ${ON_DISK} THREADS ${THREADS}

# /* q1: question='small inner on int' */
Q=1
QUESTION="small inner on int"
QUERY="SELECT id1, x.id2, x.id3, x.id4, y.id4, x.id5, x.id6, x.v1, y.v2 FROM ${SRC_DATANAME} AS x INNER JOIN ${RHS1} AS y USING (id1)"
ch_make_2_runs

# /* q2: question='medium inner on int' */
Q=2
QUESTION="medium inner on int"
QUERY="SELECT x.id1, y.id1, id2, x.id3, x.id4, y.id4, x.id5, y.id5, x.id6, x.v1, y.v2 FROM ${SRC_DATANAME} AS x INNER JOIN ${RHS2} AS y USING (id2)"
ch_make_2_runs

# /* q3: question='medium outer on int' */
Q=3
QUESTION="medium outer on int"
QUERY="SELECT x.id1, y.id1, id2, x.id3, x.id4, y.id4, x.id5, y.id5, x.id6, x.v1, y.v2 FROM ${SRC_DATANAME} AS x LEFT JOIN ${RHS2} AS y USING (id2)"
ch_make_2_runs

# /* q4: question='medium inner on factor' */
Q=4
QUESTION="medium inner on factor"
QUERY="SELECT x.id1, y.id1, x.id2, y.id2, x.id3, x.id4, y.id4, id5, x.id6, x.v1, y.v2 FROM ${SRC_DATANAME} AS x INNER JOIN ${RHS2} AS y USING (id5)"
ch_make_2_runs

# /* q5: question='big inner on int' */
Q=5
QUESTION="big inner on int"
QUERY="SELECT x.id1, y.id1, x.id2, y.id2, id3, x.id4, y.id4, x.id5, y.id5, x.id6, y.id6, x.v1, y.v2 FROM ${SRC_DATANAME} AS x INNER JOIN ${RHS3} AS y USING (id3)"
ch_make_2_runs
