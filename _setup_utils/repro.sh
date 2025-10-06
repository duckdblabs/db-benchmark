# full repro on Ubuntu 22.04

cd ~/h2oai-db-benchmark

sudo apt-get -qq update
sudo apt upgrade

sudo apt-get -qq install -y lsb-release software-properties-common wget curl vim htop git byobu libcurl4-openssl-dev libssl-dev
sudo apt-get -qq install -y libfreetype6-dev
sudo apt-get -qq install -y libfribidi-dev
sudo apt-get -qq install -y libharfbuzz-dev
sudo apt-get -qq install -y git
sudo apt-get -qq install -y libxml2-dev
sudo apt-get -qq install -y make
sudo apt-get -qq install -y libfontconfig1-dev
sudo apt-get -qq install -y libicu-dev pandoc zlib1g-dev libgit2-dev libcurl4-openssl-dev libssl-dev libjpeg-dev libpng-dev libtiff-dev
# sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
sudo add-apt-repository "deb [arch=amd64,i386] https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/"
sudo apt-get -qq update
sudo apt-get -qq install -y r-base-dev virtualenv

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

