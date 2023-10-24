# script to format mount and copy data.

sudo mkfs -t xfs /dev/nvme0n1

mkdir db-benchmark-metal
# mount the nvme volumn
sudo mount /dev/nvme0n1 ~/db-benchmark-metal
# change ownsership of the volumn
sudo chown -R ubuntu db-benchmark-metal/

cd db-benchmark-metal
git clone https://github.com/duckdblabs/db-benchmark.git .

mkdir data
cd data
cp ~/db-benchmark/data/*.csv .


./_launcher/setup.sh

# setup all the solutions on db-benchmark-metal.
# creates the necessary python virtual environments and creates the r-libraries
# needed
source path.env && python3 _utils/install_all_solutions.py all


# setup mount for clickhouse extra data
sudo mkfs -t xfs /dev/nvme1n1
sudo mkdir /var/lib/clickhouse-nvme-mount/
sudo mount /dev/nvme1n1 /var/lib/clickhouse-nvme-mount/
sudo cp -a /var/lib/clickhouse/. /var/lib/clickhouse-nvme-mount/
# change ownership of new mount to clickhouse
sudo chown -R clickhouse:clickhouse /var/lib/clickhouse-nvme-mount/
sudo chown -R clickhouse:clickhouse /dev/nvme1n1
sudo chmod 777 /home/ubuntu/

sudo cp clickhouse/clickhouse-mount-config.xml /etc/clickhouse-server/config.d/data-paths.xml

echo "------------------------------------------"
echo "------------------------------------------"
echo "READY TO RUN BENCHMARK. ./run.sh"

