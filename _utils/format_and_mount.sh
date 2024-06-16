# script to format mount and copy data.

# remove a leftover instance mount
rm -rf ~/db-benchmark-metal

# format the mount
sudo mkfs -t xfs /dev/nvme0n1

mkdir ~/db-benchmark-metal
# mount the nvme volumn
sudo mount /dev/nvme0n1 ~/db-benchmark-metal
# change ownsership of the volume
sudo chown -R ubuntu ~/db-benchmark-metal/

git clone https://github.com/duckdblabs/db-benchmark.git ~/db-benchmark-metal

# if you have an EBS volume, you can generate the data once, save it on the ebs volume, and transfer it
# each time.

if [[ $# -gt 0 ]]
then
	echo "Creating data"
	mkdir -p ~/db-benchmark-metal/data/
	cd ~/db-benchmark-metal/data/
	echo "Creating 500mb group by datasets"
	Rscript ../_data/groupby-datagen.R 1e7 1e2 0 0
	Rscript ../_data/groupby-datagen.R 1e7 1e1 0 0
	Rscript ../_data/groupby-datagen.R 1e7 2e0 0 0
	Rscript ../_data/groupby-datagen.R 1e7 1e2 0 1
	Rscript ../_data/groupby-datagen.R 1e7 1e2 5 0
	echo "Creating 5gb group by datasets"
	Rscript ../_data/groupby-datagen.R 1e8 1e2 0 0
	Rscript ../_data/groupby-datagen.R 1e8 1e1 0 0
	Rscript ../_data/groupby-datagen.R 1e8 2e0 0 0
	Rscript ../_data/groupby-datagen.R 1e8 1e2 0 1
	Rscript ../_data/groupby-datagen.R 1e8 1e2 5 0
	echo "Creating 50gb group by datasets"
	Rscript ../_data/groupby-datagen.R 1e9 1e2 0 0
	Rscript ../_data/groupby-datagen.R 1e9 1e1 0 0
	Rscript ../_data/groupby-datagen.R 1e9 2e0 0 0
	Rscript ../_data/groupby-datagen.R 1e9 1e2 0 1
	Rscript ../_data/groupby-datagen.R 1e9 1e2 5 0
	echo "Creating 500mb join datasets"
	Rscript ../_data/join-datagen.R 1e7 0 0
	Rscript ../_data/join-datagen.R 1e7 5 0
	Rscript ../_data/join-datagen.R 1e7 0 1
	echo "Creating 5gb join datasets"
	Rscript ../_data/join-datagen.R 1e8 0 0
	Rscript ../_data/join-datagen.R 1e8 5 0
	Rscript ../_data/join-datagen.R 1e8 0 1
	echo "Creating 50gb join datasets"
	Rscript ../_data/join-datagen.R 1e9 0 0
	cd ..
elif [[ ! -d "~/db-benchark/data" ]]
then
	echo "no arguments passed. Copying data..."
	echo "ERROR: directory ~/db-benchmark/data does not exist"
else
	mkdir -p ~/db-benchmark-metal/data/
	cd ~/db-benchmark-metal/data/
	echo "Copying data from ~/db-benchark/data"
	cp ~/db-benchmark/data/*.csv
	cd ~/db-benchmark-metal
fi


./_launcher/setup.sh

# setup all the solutions on db-benchmark-metal.
# creates the necessary python virtual environments and creates the r-libraries
# needed
source path.env && python3 _utils/install_all_solutions.py all


# setup mount for clickhouse spill
sudo mkfs -t xfs /dev/nvme1n1
sudo mkdir /var/lib/clickhouse-nvme-mount/
sudo mount /dev/nvme1n1 /var/lib/clickhouse-nvme-mount/
# not sure if below is necessary.
sudo cp -a /var/lib/clickhouse/. /var/lib/clickhouse-nvme-mount/
# change ownership of new mount to clickhouse
sudo chown -R clickhouse:clickhouse /var/lib/clickhouse-nvme-mount/
sudo chown -R clickhouse:clickhouse /dev/nvme1n1

# add config so clickhouse knows to use the mount to spill data
sudo cp clickhouse/clickhouse-mount-config.xml /etc/clickhouse-server/config.d/data-paths.xml

echo "------------------------------------------"
echo "------------------------------------------"
echo "READY TO RUN BENCHMARK. ./run.sh"
