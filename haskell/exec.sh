#!/bin/bash
set -e

cabal --project-dir=./haskell run -O2 "$1-haskell"
