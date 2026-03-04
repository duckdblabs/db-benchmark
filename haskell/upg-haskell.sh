#!/bin/bash
set -e

cd haskell

cabal update
cabal build -O2

cd ..

./haskell/ver-haskell.sh
