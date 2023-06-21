# Rscript _data/rollfun-datagen.R 1e6 0 0 1
# Rscript _data/rollfun-datagen.R 1e7 0 0 1
# Rscript _data/rollfun-datagen.R 1e8 0 0 1

args = commandArgs(TRUE)

pretty_sci = function(x) {
  tmp<-strsplit(as.character(x), "+", fixed=TRUE)[[1L]]
  if(length(tmp)==1L) {
    paste0(substr(tmp, 1L, 1L), "e", nchar(tmp)-1L)
  } else if(length(tmp)==2L){
    paste0(tmp[1L], as.character(as.integer(tmp[2L])))
  }
}

library(data.table)
N=as.integer(args[1L]); K=as.integer(args[2L]); nas=as.integer(args[3L]); sort=as.integer(args[4L])
stopifnot(nas==0L, sort==1L) ## timeseries data always sorted
set.seed(108)
cat(sprintf("Producing data of %s rows, %s NAs ratio, %s sort flag\n", pretty_sci(N), nas, sort))
DT = list()
DT[["id1"]] = seq.int(N)                     ## index, do we need it as POSIXct/IDate?
## uneven idx
DT[["id2"]] = sort(sample(N*1.1, N))         ## index dense
DT[["id3"]] = sort(sample(N*2, N))           ## index sparse
DT[["v1"]] =  cumprod(rnorm(N, 1, 0.005))    ## more risky asset
DT[["v2"]] =  cumprod(rnorm(N, 1, 0.001))    ## less risky asset
DT[["weights"]] = rnorm(n=N, m=1, sd=0.1)

setDT(DT)
file = sprintf("R1_%s_NA_%s_%s.csv", pretty_sci(N), nas, sort)
cat(sprintf("Writing data to %s\n", file))
fwrite(DT, file)
cat(sprintf("Data written to %s, quitting\n", file))
if (!interactive()) quit("no", status=0)
