#!/usr/bin/env Rscript
#
# Make a fixed-width summary plot of a set of quality scores.
#
# Usage: Rscript plot_qscore_summary.r options
# Options:
#   datafile="filename"      input data file (row=cycle, column=qvalue)
#   plotfile="filename"      output file
#   read.starts=c(n1,n2,...) first cycle of each read not including the first
# Note that filenames and other strings must be quoted!
#
## Updated 3/29/2016; pbilling. Added qvalues if/else for HiSeq 4000 data

datafile <- NULL
plotfile <- NULL
read.starts <- NULL
arg.list <- commandArgs(trailingOnly=TRUE)
if (length(arg.list) == 0) {
   print("error: no arguments supplied")
   quit(save="no", status=1)
}
for (i in 1:length(arg.list)) {
    eval(parse(text=arg.list[i]))
}
if (is.null(datafile)) {
   print("error: missing datafile")
   quit(save="no", status=1)
}
if (is.null(plotfile)) {
   print("error: missing plotfile")
   quit(save="no", status=1)
}

data <- read.table(datafile, header=TRUE)
#png(file=plotfile, width=800, height=500, res=72)
pdf(file=plotfile, width=8, height=5)
counts <- round(data * 1000 / (rowSums(data)+1))  # +1 is pseudocount to prevent denominator from being zero.
if (ncol(data)==7){
  # RTA 2.7.3 (HiSeq 4000) data
  # http://www.illumina.com/documents/products/whitepapers/whitepaper_datacompression.pdf
  # Warning: illumina may change these bin values
  qvalues <- c(6,14,22,27,32,37,40) # Values based on median of bin qualities 
} else {
  qvalues <- c(1:ncol(data))
}
find_quantiles <- function(x) {quantile(rep(qvalues, x), c(1:3)/4)}
quantiles <- apply(counts, 1, find_quantiles)
par(mai=c(1.0,1.0,0.25,0.25))
plot(quantiles[1,], type='l', lwd=8, col="gray",
     ylim=c(0,45), xlab="Cycle", ylab="Q Score",
     xaxs="i", yaxs="i", yaxt="n", cex.lab=1.5) 
points(quantiles[3,], type='l', lwd=8, col="gray")
points(quantiles[2,], type='o', pch=16, cex=1.5, col="black", xpd=TRUE)
axis(2, tck=1)
if (!is.null(read.starts)) {
    abline(v=read.starts)
}
