#!/bin/bash
set -e

# dirs for datasets and output of benchmark
mkdir -p data
mkdir -p out

sudo apt-get update

# install R
sudo add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/"
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
sudo apt-get update -qq
sudo apt-get install -y r-base-dev
sudo apt-get install python3-dev virtualenv

sudo chmod a+w /usr/local/lib/R/site-library

# configure R
echo 'LC_ALL=C' >> ~/.Renviron
mkdir -p ~/.R
echo 'CFLAGS=-O3 -mtune=native' > ~/.R/Makevars
echo 'CXXFLAGS=-O3 -mtune=native' >> ~/.R/Makevars

# packages used in launcher and report
Rscript -e 'install.packages(c("bit64","rmarkdown","data.table","rpivotTable","formattable","lattice"))'
Rscript -e 'sapply(c("bit64","rmarkdown","data.table","rpivotTable","formattable","lattice"), requireNamespace)'

# install duckdb for unpacking data
curl --fail --location --progress-bar --output duckdb_cli-linux-amd64.zip https://github.com/duckdb/duckdb/releases/download/v1.2.0/duckdb_cli-linux-amd64.zip
sudo unzip duckdb_cli-linux-amd64.zip -d /usr/local/bin


# install aws client to download benchmark data
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# after each restart of server
source clickhouse/ch.sh && ch_stop
sudo service docker stop
sudo swapoff -a

# stop and disable
sudo systemctl disable docker
sudo systemctl stop docker
sudo systemctl disable clickhouse-server
sudo systemctl stop clickhouse-server
