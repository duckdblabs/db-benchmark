#!/bin/bash
set -e

source ./clickhouse/ch.sh # clickhouse helper scripts

ch_installed && clickhouse-client --version-clean > clickhouse/VERSION && echo "" > clickhouse/REVISION

if [[ $TEST_RUN != "true" ]]; then
	sudo chown ubuntu:ubuntu clickhouse/VERSION
	sudo chown ubuntu:ubuntu clickhouse/REVISION
fi
