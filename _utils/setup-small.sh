# full repro on Ubuntu 22.04

# update the key
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 51716619E084DAB9
## Install libraries

sudo apt-get -qq update
sudo apt upgrade

sudo apt-get -qq install make

sudo apt-get -qq install wget curl openssl build-essential
sudo apt-get -qq install -y r-base-dev virtualenv
sudo apt-get -qq install openjdk-8-jdk

sudo apt-get install -y zlib1g-dev
sudo apt-get install -y pandoc

# update virtualenv
python3 -m pip install virtualenv

# sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
# sudo add-apt-repository "deb [arch=amd64,i386] https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/"

sudo chmod o+w /usr/local/lib/R/site-library

Rscript -e 'install.packages(c("data.table", "dplyr", "knitr", "bit64"), dependecies=TRUE, repos="https://cloud.r-project.org")'

mkdir -p ~/.R
echo 'CFLAGS=-O3 -mtune=native' >> ~/.R/Makevars
echo 'CXXFLAGS=-O3 -mtune=native' >> ~/.R/Makevars