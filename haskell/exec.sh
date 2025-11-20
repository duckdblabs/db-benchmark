#!/bin/bash
set -e

cd ./haskell

cabal run -O2 "$1-haskell"
