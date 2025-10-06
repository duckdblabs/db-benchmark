# script to format mount and copy data.
# mount the data
./_setup_utils/mount.sh

# setup all the solutions on db-benchmark-metal.
# creates the necessary python virtual environments and creates the r-libraries
# needed
cd ~/db-benchmark-metal && source path.env && python3 _setup_utils/install_all_solutions.py all



# setup mount for clickhouse spill
# sudo mkfs -t xfs /dev/nvme1n1
# sudo mkdir /var/lib/clickhouse-nvme-mount/
# sudo mount /dev/nvme1n1 /var/lib/clickhouse-nvme-mount/
# # not sure if below is necessary.
# sudo cp -a /var/lib/clickhouse/. /var/lib/clickhouse-nvme-mount/
# # change ownership of new mount to clickhouse
# sudo chown -R clickhouse:clickhouse /var/lib/clickhouse-nvme-mount/
# sudo chown -R clickhouse:clickhouse /dev/nvme1n1

# # add config so clickhouse knows to use the mount to spill data
# sudo cp clickhouse/clickhouse-mount-config.xml /etc/clickhouse-server/config.d/data-paths.xml

echo "------------------------------------------"
echo "------------------------------------------"
echo "READY TO RUN BENCHMARK. ./run.sh"
