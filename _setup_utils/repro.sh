# full repro on Ubuntu 22.04

# never prompt during apt installs/upgrades (including needrestart's "restart services?" dialog)
export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=a
APT_OPTS='-y -qq -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold'

cd ~/h2oai-db-benchmark

sudo -E apt-get -qq update
sudo -E apt-get $APT_OPTS upgrade

sudo -E apt-get $APT_OPTS install lsb-release software-properties-common wget curl vim htop git byobu libcurl4-openssl-dev libssl-dev
sudo -E apt-get $APT_OPTS install libfreetype6-dev
sudo -E apt-get $APT_OPTS install libfribidi-dev
sudo -E apt-get $APT_OPTS install libharfbuzz-dev
sudo -E apt-get $APT_OPTS install git
sudo -E apt-get $APT_OPTS install libxml2-dev
sudo -E apt-get $APT_OPTS install make
sudo -E apt-get $APT_OPTS install libfontconfig1-dev
sudo -E apt-get $APT_OPTS install libicu-dev pandoc zlib1g-dev libgit2-dev libcurl4-openssl-dev libssl-dev libjpeg-dev libpng-dev libtiff-dev
# CRAN signing key, without it apt-get update rejects the CRAN repository
wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | sudo tee /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc > /dev/null
sudo -E add-apt-repository -y "deb [arch=amd64,i386] https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/"
sudo -E apt-get -qq update
sudo -E apt-get $APT_OPTS install r-base-dev virtualenv

# java for spark (JAVA_HOME in path.env)
sudo -E apt-get $APT_OPTS install openjdk-17-jdk

# duckdb cli, used to export the downloaded datasets to csv
curl -fsSL https://install.duckdb.org | sh

# haskell: build dependencies for ghc, then ghcup, ghc 9.12.2 and cabal
sudo -E apt-get $APT_OPTS install build-essential libffi-dev libgmp-dev libgmp10 libncurses-dev pkg-config
sudo -E apt-get $APT_OPTS install libtinfo6 || sudo -E apt-get $APT_OPTS install libtinfo5
if ! command -v ghcup &> /dev/null && [ ! -x "$HOME/.ghcup/bin/ghcup" ]; then
  curl --proto '=https' --tlsv1.2 -sSf https://get-ghcup.haskell.org | BOOTSTRAP_HASKELL_NONINTERACTIVE=1 BOOTSTRAP_HASKELL_MINIMAL=1 sh
fi
source "$HOME/.ghcup/env"
ghcup install ghc 9.12.2
ghcup set ghc 9.12.2
ghcup install cabal
cabal update
# put ghc/cabal on PATH for future shells
grep -qxF 'source "$HOME/.ghcup/env"' ~/.bashrc || echo 'source "$HOME/.ghcup/env"' >> ~/.bashrc

cd /usr/local/lib/R
sudo chmod o+w site-library

cd ~
mkdir -p .R
echo 'CFLAGS=-O3 -mtune=native' >> ~/.R/Makevars
echo 'CXXFLAGS=-O3 -mtune=native' >> ~/.R/Makevars

Rscript -e 'install.packages(c("jsonlite","bit64","devtools","rmarkdown"), dependecies=TRUE, repos="https://cloud.r-project.org")'


# install dplyr
Rscript -e 'devtools::install_github(c("tidyverse/readr","tidyverse/dplyr"))'

# install data.table
Rscript -e 'install.packages("data.table", repos="https://rdatatable.gitlab.io/data.table/")'

