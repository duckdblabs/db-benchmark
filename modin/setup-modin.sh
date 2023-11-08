#!/bin/bash
set -e

curl -o install_miniconda.sh -L https://repo.anaconda.com/miniconda/Miniconda3-py311_23.9.0-0-Linux-x86_64.sh && \
    sh install_miniconda.sh -u -b -p ./modin/py-modin && \
    rm -f install_miniconda.sh

source ./modin/py-modin/bin/activate
conda install -y conda-libmamba-solver

conda create --name modin -y
conda activate modin
echo "conda activate modin" >> ./modin/py-modin/bin/activate

# install binaries
conda install -y -c conda-forge modin-hdk --solver=libmamba

# check
python3 -c "import modin; print(modin.__version__)"

conda deactivate
conda deactivate
