#!/bin/bash
set -e

sudo apt-get -qq install -y curl ca-certificates libtinfo5

# Install Cabal (Haskell build tool) if not present
if ! command -v ghcup &> /dev/null; then
    echo "Installing Cabal..."
    curl --proto '=https' --tlsv1.2 -sSf https://get-ghcup.haskell.org | BOOTSTRAP_HASKELL_NONINTERACTIVE=1 BOOTSTRAP_HASKELL_MINIMAL=1 sh
fi

[ -f "$HOME/.ghcup/env" ] && source "$HOME/.ghcup/env"

ghcup install ghc 9.6.7
ghcup install cabal

# Set 9.6.7 as the active 'ghc' version
ghcup set ghc 9.6.7

cd haskell

# Install dependencies and build
cabal update
cabal build -O2

cd ..

./haskell/ver-haskell.sh
