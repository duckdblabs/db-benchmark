#!/bin/bash
set -e

cd haskell

export HASKELL_DF_VERSION="3.6.0.0"

echo "${HASKELL_DF_VERSION}" > VERSION

echo "dataframe-${HASKELL_DF_VERSION}" > REVISION

cd ..
