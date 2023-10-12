#!/bin/bash
set -e

echo 'upgrading modin...'

source ./modin/miniconda/bin/activate

conda update modin-hdk -p ./modin/py-modin -y
