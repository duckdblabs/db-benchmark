#!/bin/bash
set -e

cd ./haskell

stack run "$1-haskell"
