#!/bin/bash
set -e

# Install Stack (Haskell build tool) if not present
if ! command -v stack &> /dev/null; then
    echo "Installing Stack..."
    curl -sSL https://get.haskellstack.org/ | sh
fi

cd haskell

# Initialize stack project if not already done
if [ ! -f "stack.yaml" ]; then
    stack init --force
fi

# Install dependencies and build
stack setup
stack build --only-dependencies --ghc-options "-O2"
stack build --ghc-options "-O2"

cd ..

./haskell/ver-haskell.sh
