#!/usr/bin/env Rscript

#
# Make a fixed-width summary plot of base calls.
#
# Usage: Rscript plot_call_summary.r options
# Options:
#   datafile="filename"      input data file (column=channel, row=cycle)
#   plotfile="filename"      output file
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
#png(file=plotfile, width=800, height=500, res=72)
pdf(file=plotfile, width=8, height=5)
par(mai=c(1.0,1.0,0.25,0.25))
plot(x=c(1:nrow(data)), y=data$a, type='p', pch=16, cex=1.5, col="gray",
     xlab="Cycle", ylab="Percent Base Calls", ylim=c(0, 100), cex.lab=1.5)
points(x=c(1:nrow(data)), y=data$c, type='p', pch=16, cex=1.5, col="red")
points(x=c(1:nrow(data)), y=data$g, type='p', pch=16, cex=1.5, col="blue")
points(x=c(1:nrow(data)), y=data$t, type='p', pch=16, cex=1.5, col="green")
points(x=c(1:nrow(data)), y=data$n, type='p', pch=16, cex=1.5, col="yellow")
if (!is.null(read.starts)) {
   abline(v=read.starts)
}
legend("topright", c("A", "C", "G", "T", "N"), pch=16, pt.cex=2,
       col=c("gray", "red", "blue", "green", "yellow"))
