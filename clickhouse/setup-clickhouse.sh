#!/bin/bash
# install
curl https://clickhouse.com/install.sh | sudo sh

# stop server if service was already running
sudo service clickhouse-server start ||:


# modify clickhouse settings so data is stored on the mount.
sudo mkdir -p /var/lib/mount/clickhouse-nvme-mount/
sudo chown -R clickhouse:clickhouse /var/lib/mount/clickhouse-nvme-mount
sudo chown -R clickhouse:clickhouse /var/lib/mount/clickhouse-nvme-mount/store

# set up clickhouse tmp space
sudo mkdir -p /var/lib/mount/clickhouse-tmp/
sudo chown clickhouse:clickhouse /var/lib/mount/clickhouse-tmp

# copy clickhouse config
sudo cp -a /var/lib/clickhouse/. /var/lib/mount/clickhouse-nvme-mount/
sudo cp clickhouse/clickhouse-mount-config.xml /etc/clickhouse-server/config.d/data-paths.xml


# start server
sudo rm -rf /var/log/clickhouse-server/clickhouse-server.err.log /var/log/clickhouse-server/clickhouse-server.log
sudo service clickhouse-server start


MEMORY_LIMIT=0
BYTES_BEFORE_EXTERNAL_GROUP_BY=0
if [[ $MACHINE_TYPE == "c6id.4xlarge" ]]; then
	MEMORY_LIMIT=28000000000
	BYTES_BEFORE_EXTERNAL_GROUP_BY=20000000000
fi

clickhouse-client --query "CREATE USER IF NOT EXISTS db_benchmark IDENTIFIED WITH no_password SETTINGS max_memory_usage = $MEMORY_LIMIT, max_bytes_before_external_group_by = $BYTES_BEFORE_EXTERNAL_GROUP_BY WRITABLE;"
clickhouse-client --query "GRANT select, insert, create, alter, alter user, create table, truncate, drop, system flush logs on *.* to db_benchmark;"

./clickhouse/ver-clickhouse.sh
