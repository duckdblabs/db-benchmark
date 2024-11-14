#!/bin/bash
set -e

Rscript -e 'v=read.dcf(system.file(package="duckplyr", lib.loc="./duckplyr/r-duckplyr", "DESCRIPTION"), fields=c("Version","RemoteSha")); colnames(v)[colnames(v)=="RemoteSha"]="Revision"; cnafill=function(x) {x=c(x); x[is.na(x)]=""; x}; fw=function(f, v) writeLines(v, file.path("duckplyr", f)); invisible(mapply(fw, toupper(colnames(v)), cnafill(v)))'
