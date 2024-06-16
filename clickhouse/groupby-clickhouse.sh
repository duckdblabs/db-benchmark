source ./clickhouse/ch.sh

SOLUTION=clickhouse
TASK=groupby

# /* q1: question='sum v1 by id1' */

Q=1
QUESTION="sum v1 by id1"
QUERY="SELECT id1, sum(v1) AS v1 FROM ${SRC_DATANAME} GROUP BY id1"

ch_make_2_runs

# /* q2: question='sum v1 by id1:id2' */
Q=2
QUESTION="sum v1 by id1:id2"
QUERY="SELECT id1, id2, sum(v1) AS v1 FROM ${SRC_DATANAME} GROUP BY id1, id2"

ch_make_2_runs

# /* q3: question='sum v1 mean v3 by id3' */
Q=3
QUESTION="sum v1 mean v3 by id3"
QUERY="SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM ${SRC_DATANAME} GROUP BY id3"

ch_make_2_runs

# /* q4: question='mean v1:v3 by id4' */
Q=4
QUESTION="mean v1:v3 by id4"
QUERY="SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM ${SRC_DATANAME} GROUP BY id4"

ch_make_2_runs

# /* q5: question='sum v1:v3 by id6' */
Q=5
QUESTION="sum v1:v3 by id6"
QUERY="SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM ${SRC_DATANAME} GROUP BY id6"

ch_make_2_runs

# /* q6: question='median v3 sd v3 by id4 id5' */
Q=6
QUESTION="median v3 sd v3 by id4 id5"
QUERY="SELECT id4, id5, medianExact(v3) AS median_v3, stddevPop(v3) AS sd_v3 FROM ${SRC_DATANAME} GROUP BY id4, id5"

ch_make_2_runs

# /* q7: question='max v1 - min v2 by id3' */
Q=7
QUESTION="max v1 - min v2 by id3"
QUERY="SELECT id3, max(v1) - min(v2) AS range_v1_v2 FROM ${SRC_DATANAME} GROUP BY id3"

ch_make_2_runs

# /* q8: question='largest two v3 by id6' */
Q=8
QUESTION="largest two v3 by id6"
QUERY="SELECT id6, arrayJoin(arraySlice(arrayReverseSort(groupArray(v3)), 1, 2)) AS v3 FROM (SELECT id6, v3 FROM ${SRC_DATANAME} WHERE v3 IS NOT NULL) AS subq GROUP BY id6"

ch_make_2_runs

# /* q9: question='regression v1 v2 by id2 id4' */
Q=9
QUESTION="regression v1 v2 by id2 id4"
QUERY="SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM ${SRC_DATANAME} GROUP BY id2, id4"

ch_make_2_runs

# /* q10: question='sum v3 count by id1:id6' */
Q=10
QUESTION="sum v3 count by id1:id6"
QUERY="SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count() AS cnt FROM ${SRC_DATANAME} GROUP BY id1, id2, id3, id4, id5, id6"

ch_make_2_runs
