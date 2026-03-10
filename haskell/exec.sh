#!/bin/bash
set -e

unset CC
unset CXX
unset LD

# Install Cabal (Haskell build tool) if not present
if ! command -v ghcup &> /dev/null; then
    echo "Installing Cabal..."
    curl --proto '=https' --tlsv1.2 -sSf https://get-ghcup.haskell.org | BOOTSTRAP_HASKELL_NONINTERACTIVE=1 BOOTSTRAP_HASKELL_MINIMAL=1 sh
fi

[ -f "$HOME/.ghcup/env" ] && source "$HOME/.ghcup/env"

ghcup install ghc 9.6.7

# Set 9.6.7 as the active 'ghc' version
ghcup set ghc 9.6.7

source ./haskell/ver-haskell.sh

cabal --project-dir=./haskell run -O2 "$1-haskell"
