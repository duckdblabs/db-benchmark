#!/bin/bash
set -e

cd haskell

DF_VERSION="0.4.1"

echo "${DF_VERSION}" > VERSION

echo "dataframe-${DF_VERSION}" > REVISION

cd ..
