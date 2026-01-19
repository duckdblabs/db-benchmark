#!/bin/bash
set -e

cd haskell

cabal update
cabal build

cd ..

./haskell/ver-haskell.sh
