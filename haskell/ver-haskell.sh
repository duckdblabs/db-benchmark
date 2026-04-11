#!/bin/bash
set -e

cd haskell

export HASKELL_DF_VERSION="0.7.0.0"

echo "${HASKELL_DF_VERSION}" > VERSION

echo "dataframe-${HASKELL_DF_VERSION}" > REVISION

cd ..
