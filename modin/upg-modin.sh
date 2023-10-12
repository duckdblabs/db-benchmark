#!/bin/bash
set -e

echo 'upgrading modin...'

source ./modin/py-modin/bin/activate
conda update modin-hdk -y -c conda-forge --solver=libmamba
