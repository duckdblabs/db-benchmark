#!/bin/bash
set -e

cd haskell

DF_PKG_ID="$(
  cabal v2-exec -- ghc-pkg latest --simple-output dataframe 2>/dev/null || true
)"

DF_VERSION="0.4.0.8"
if [ -n "${DF_PKG_ID}" ]; then
  DF_VERSION="${DF_PKG_ID#dataframe-}"
fi

echo "${DF_VERSION}" > VERSION

DF_SRC_DIR="$(
  find dist-newstyle/src -maxdepth 1 -type f -name 'dataframe.cabal' -print 2>/dev/null \
    | head -n 1 \
    | xargs -r dirname \
    || true
)"

GIT_REV=""
if [ -n "${DF_SRC_DIR}" ] && [ -d "${DF_SRC_DIR}/.git" ]; then
  GIT_REV="$(git -C "${DF_SRC_DIR}" rev-parse --short HEAD 2>/dev/null || true)"
fi

if [ -n "${GIT_REV}" ]; then
  echo "${GIT_REV}" > REVISION
else
  echo "dataframe-${DF_VERSION}" > REVISION
fi

cd ..
