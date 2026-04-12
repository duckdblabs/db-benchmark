#!/bin/bash
# install
cd clickhouse

sudo apt-get install -y expect

sudo mkdir -p /var/lib/mount/clickhouse-nvme-mount/

rm -rf clickhouse
curl https://clickhouse.com/install.sh | sudo sh
expect -c '
  spawn sudo ./clickhouse install
  expect "password"
  send "\r"
  expect eof
'

# created by install script above
sudo chown -R clickhouse:clickhouse '/etc/clickhouse-server'

# copy clickhouse config so install will inherit the correct data path
# there are questions as to what happens on a fresh install.
sudo cp clickhouse-mount-config.xml /etc/clickhouse-server/config.d/data-paths.xml

# modify mount so clickhouse can read it
sudo chown -R clickhouse:clickhouse /var/lib/mount/clickhouse-nvme-mount

# set up clickhouse tmp space
sudo mkdir -p /var/lib/mount/clickhouse-tmp/
sudo chown clickhouse:clickhouse /var/lib/mount/clickhouse-tmp

# start server
echo "attempting to start server"
sudo clickhouse start
echo "finished starting clickhouse server"

MEMORY_LIMIT=0
BYTES_BEFORE_EXTERNAL_GROUP_BY=0
if [[ $MACHINE_TYPE == "c6id.4xlarge" ]]; then
	MEMORY_LIMIT=28000000000
	BYTES_BEFORE_EXTERNAL_GROUP_BY=20000000000
fi

cd ..

clickhouse-client --query "CREATE USER IF NOT EXISTS db_benchmark IDENTIFIED WITH no_password SETTINGS max_memory_usage = $MEMORY_LIMIT, max_bytes_before_external_group_by = $BYTES_BEFORE_EXTERNAL_GROUP_BY WRITABLE;"
clickhouse-client --query "GRANT select, insert, create, alter, alter user, create table, truncate, drop, system flush logs on *.* to db_benchmark;"

./clickhouse/ver-clickhouse.sh

# stop clickhouse server to start
sudo clickhouse stop
