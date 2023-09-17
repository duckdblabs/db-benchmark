#!/bin/bash
set -e

# upgrade all packages in collapse library only if new collapse is out
echo 'upgrading collapse...'
Rscript -e 'ap=available.packages(); if (ap["collapse","Version"]!=packageVersion("collapse", lib.loc="./collapse/r-collapse")) update.packages(lib.loc="./collapse/r-collapse", ask=FALSE, checkBuilt=TRUE, quiet=TRUE)'
