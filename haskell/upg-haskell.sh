#!/bin/bash
set -e

cd haskell

# Update stack resolver and dependencies
stack update
stack upgrade
stack build --only-dependencies

cd ..

./haskell/ver-haskell.sh
