# Data generation data for groupby 0.5GB

mkdir -p data
cd data/
Rscript ../_data/groupby-datagen.R 1e7 1e2 0 0
Rscript ../_data/groupby-datagen.R 1e7 1e2 15 0
Rscript ../_data/join-datagen.R 1e7 0 0 0

cp G1_1e7_1e2_0_0.csv G1_1e9_1e2_0_0.csv
cp J1_1e7_1e1_0_0.csv J1_1e9_1e3_0_0.csv
cp J1_1e7_1e4_0_0.csv J1_1e9_1e6_0_0.csv
cp J1_1e7_1e7_0_0.csv J1_1e9_1e9_0_0.csv
cp J1_1e7_NA_0_0.csv J1_1e9_NA_0_0.csv

cd ..

# don't publish, we dont even have the keys
sed -i 's/DO_PUBLISH=true/DO_PUBLISH=false/g' run.conf

# set sizes
mv _control/data.csv _control/data.csv.original

echo "task,data,nrow,k,na,sort,active" > _control/data.csv
echo "groupby,G1_1e7_1e2_0_0,1e7,1e2,0,0,1" >> _control/data.csv
echo "groupby,G1_1e7_1e2_15_0,1e7,1e2,15,0,1" >> _control/data.csv
echo "groupby,G1_1e9_1e2_0_0,1e9,1e2,0,0,1" >> _control/data.csv
echo "join,J1_1e7_NA_0_0,1e7,NA,0,0,1" >> _control/data.csv
echo "join,J1_1e9_NA_0_0,1e9,NA,0,0,1" >> _control/data.csv
