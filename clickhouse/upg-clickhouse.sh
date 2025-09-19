#!/bin/bash
set -e

# upgrade to latest released
echo 'upgrading clickhouse-server clickhouse-client...'
sudo apt-get install --only-upgrade clickhouse-server clickhouse-client

if [[ $TEST_RUN != "true" ]]; then
	sudo chown ubuntu:ubuntu clickhouse/VERSION
	sudo chown ubuntu:ubuntu clickhouse/REVISION
fi


# modify clickhouse settings so data is stored on the mount.
# This is necessary for when clickhouse is installed on a machine but the mount looses all data
sudo mkdir -p /var/lib/mount/clickhouse-nvme-mount/
sudo chown clickhouse:clickhouse /var/lib/mount/clickhouse-nvme-mount

# copy clickhouse config
sudo cp -a /var/lib/clickhouse/. /var/lib/mount/clickhouse-nvme-mount/
sudo cp clickhouse/clickhouse-mount-config.xml /etc/clickhouse-server/config.d/data-paths.xml


# start server
sudo rm -rf /var/log/clickhouse-server/clickhouse-server.err.log /var/log/clickhouse-server/clickhouse-server.log
sudo service clickhouse-server start