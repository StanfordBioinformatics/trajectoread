#!/usr/bin/env Rscript

#
# Make a detailed barplot showing mismatches versus cycle for a set
# of aligned reads.
#
# Usage: Rscript plot_mismatch_details.r options
# Options:
#   datafile="filename"   input data file (row=cycle, column=mmfraction)
#   plotfile="filename"   output file
#   read.starts=c(n1,n2,...) first cycle of each read after the first one
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
    #eval(parse(text=arg.list[i]))
    eval(parse(file=arg.list[i]))
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
plot.width <- 90 + 10*nrow(data)
#png(file=plotfile, width=plot.width, height=500, res=72)
pdf(file=plotfile, width=8, height=5)
max.height = max(c(max(data$mmfraction), 0.05))

# Cover case where there are no reads.
if (nrow(data) == 0) {
   data <- data.frame(mmfraction=0, mmcount=0)
} else {
   par(mai=c(1.0,1.0,0.25,0.25))
}
mp = barplot(data$mmfraction, names.arg=c(1:nrow(data)),
     xlab="Cycle", ylab="Mismatch Fraction", ylim=c(0, max.height),
     xaxs="i", axes=FALSE, cex.lab=1.5)
axis(2, tck=1)
box()
if (!is.null(read.starts)) {
   abline(v=((mp[read.starts-1] + mp[read.starts])/2))
}
