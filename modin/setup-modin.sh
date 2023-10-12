#!/bin/bash
set -e

curl -o install_miniconda.sh -L https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    sh install_miniconda.sh -u -b -p ./modin/miniconda && \
    rm -f install_miniconda.sh

source ./modin/miniconda/bin/activate
conda install -y conda-libmamba-solver

# install binaries
conda create -y --prefix ./modin/py-modin -c conda-forge python=3.10 --experimental-solver=libmamba
conda install -y -p ./modin/py-modin -c conda-forge modin-hdk --experimental-solver=libmamba

conda activate modin/py-modin

# check
conda run -p modin/py-modin python3 -c "import modin; print(modin.__version__)"

deactivate
