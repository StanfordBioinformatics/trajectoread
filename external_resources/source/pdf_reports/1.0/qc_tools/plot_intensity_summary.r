#!/usr/bin/env Rscript

#
# Make a fixed-width summary plot of intensity values.
#
# Usage: Rscript plot_intensity_summary.r options
# Options:
#   datafile="filename"      input data file (row=cycle, column=channel)
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
max.intensity = max(data)
par(mai=c(1.0,1.0,0.25,0.25))
plot(x=c(1:nrow(data)), y=data$a, type='p', pch=16, cex=1.5, col="gray",
     xlab="Cycle", ylab="Intensity", ylim=c(0, max.intensity), 
     xaxs='i', yaxs='i', yaxt="n", xpd=TRUE, cex.lab=1.5)
points(x=c(1:nrow(data)), y=data$c, type='p', pch=16, cex=1.5, col="red",
       xpd=TRUE)
points(x=c(1:nrow(data)), y=data$g, type='p', pch=16, cex=1.5, col="blue",
       xpd=TRUE)
points(x=c(1:nrow(data)), y=data$t, type='p', pch=16, cex=1.5, col="green",
       xpd=TRUE)
axis(2, tck=1)
if (!is.null(read.starts)) {
   abline(v=read.starts)
}
legend("topright", c("A", "C", "G", "T"), pch=16, pt.cex=2,
       col=c("gray", "red", "blue", "green"))
