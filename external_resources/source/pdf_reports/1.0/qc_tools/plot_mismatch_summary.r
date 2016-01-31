#!/usr/bin/env Rscript

#
# Plot mismatches versus cycle for a set of aligned reads.
#
# Usage: Rscript plot_mismatch_summary.r options
# Options:
#   datafile="filename"      input data file (row=cycle, column=mmfraction)
#   plotfile="filename"      output file
#   read.starts=c(n1,n2,...) first cycle of each read after the first one
#   big.plot=bool            if TRUE then make a big plot
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
#png(file=plotfile, width=800, height=500, res=72)
pdf(file=plotfile, width=8, height=5)
max.height = max(c(max(data$mmfraction), 0.05))
# Cover case where there are no reads.
if (nrow(data) == 0) {
   data <- data.frame(mmfraction=0, mmcount=0)
} else {
   par(mai=c(1.0,1.0,0.25,0.25))
}
plot(x=c(1:nrow(data)), y=data$mmfraction, type='p', pch=16, cex=1.5,
     xlab="Cycle", ylab="Mismatch Fraction", ylim=c(0, max.height),
     xaxs="i", yaxs="i", yaxt="n", xpd=TRUE, cex.lab=1.5)
axis(2, tck=1)
if (!is.null(read.starts)) {
   abline(v=read.starts)
}
