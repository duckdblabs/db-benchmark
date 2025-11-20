#!/bin/bash
set -e

# Install Cabal (Haskell build tool) if not present
if ! command -v ghcup &> /dev/null; then
    echo "Installing Cabal..."
    curl --proto '=https' --tlsv1.2 -sSf https://get-ghcup.haskell.org | BOOTSTRAP_HASKELL_NONINTERACTIVE=1 BOOTSTRAP_HASKELL_MINIMAL=1 sh
    ghcup install cabal
fi

cd haskell

# Install dependencies and build
cabal update
cabal build -O2

cd ..

./haskell/ver-haskell.sh
