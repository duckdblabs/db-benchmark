#!/bin/bash
set -e

cd haskell

cabal update

cd ..

./haskell/ver-haskell.sh
