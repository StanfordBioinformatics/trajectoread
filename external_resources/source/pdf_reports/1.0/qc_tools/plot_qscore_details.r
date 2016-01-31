#!/usr/bin/env Rscript

#
# Make a detailed boxplot of a set of quality scores.
#
# Usage: Rscript plot_qscore.r options
# Options:
#   datafile=filename     input data file (row=cycle, column=qvalue)
#   plotfile=filename     output file
#   read.starts=c(n1,n2,...) first cycle of each read
# Note that filenames and other strings must be quoted!
#

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
plot.width <- 100 + 11*nrow(data)
#png(file=plotfile, width=plot.width, height=500, res=72)
pdf(file=plotfile, width=8, height=5)
counts <- round(data * 1000 / (rowSums(data)+1))  # +1 is pseudocount to prevent denominator from being zero.
qvalues <- c(1:ncol(data))
expand_counts <- function(x) {rep(qvalues, x)}
expcnts <- apply(counts, 1, expand_counts)
if (length(expcnts) == 0) expcnts <- t(c(1:nrow(counts)))
par(mai=c(1.0,1.0,0.25,0.25))
boxplot(expcnts, outline=FALSE, ylim=c(0,40), range=0,
        names=c(1:nrow(counts)), yaxt="n", xlab="Cycle", ylab="Q Score",
	cex.lab=1.5)
axis(2, tck=1)
if (!is.null(read.starts)) {
    abline(v=(read.starts - 0.5))
}
