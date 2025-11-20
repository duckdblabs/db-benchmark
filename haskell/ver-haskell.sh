#!/bin/bash
set -e

cd haskell

# Get dataframe version from stack
DF_VERSION=$(stack exec -- ghc-pkg field dataframe version 2>/dev/null | awk '{print $2}' || echo "0.3.3")

# Write version to VERSION file
echo "${DF_VERSION}" > VERSION

# Get git revision of dataframe if available
GIT_REV=$(stack path --local-install-root 2>/dev/null && git -C $(stack path --local-install-root 2>/dev/null || echo ".") rev-parse --short HEAD 2>/dev/null || echo "")
if [ -n "$GIT_REV" ]; then
    echo "$GIT_REV" > REVISION
else
    echo "dataframe-${DF_VERSION}" > REVISION
fi

cd ..
