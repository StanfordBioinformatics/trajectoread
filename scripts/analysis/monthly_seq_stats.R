library(ggplot2)
library(optparse)

parse_arguments <- function() {
  option_list = list(
    make_option(c("-f", "--file"), 
                type = "character", 
                default = NULL,
                metavar = "character",
                help = "Filename of sequencing stats file."),
    make_option(c("-o", "--out"),
                type = "character",
                default = NULL,
                metavar = "character",
                help = "Prefix for plot files."))
  
  opt_parser = OptionParser(option_list = option_list)
  opt = parse_args(opt_parser)
  
  if (is.null(opt$file)){
    print_help(opt_parser)
    stop("Must specify a stats file.n", call.=FALSE)
  }
  return(opt)
}

make_base_count_plots <- function(data, out) {
  ## Generate area plots describing terabases sequenced per year
  
  ggplot(data, aes(x=Month, y=Terabase_Count, fill=factor(Year), group=Year)) + 
    geom_area(alpha=0.6, position='dodge') +
    theme(axis.text=element_text(size=18), axis.title=element_text(size=24), title=element_text(size=24), 
          legend.text=element_text(size=18), legend.title=(element_text(size=24)),
          legend.margin=unit(0.5, "cm"), plot.margin=unit(c(1,1,1,1), "cm")) +
    labs(title = "Stanford Sequencing Center Production by Year", y = "Terabases Sequenced", x = "Month") +
    scale_x_continuous(breaks=seq(1, 12, by=1)) +
    scale_fill_discrete(name="Year")
  ggsave(file = paste(out, '_bases_overlap.png', sep=''), width=14.4, height=8.1)
  
  ggplot(data, aes(x=Month, y=Terabase_Count, fill=factor(Year), group=Year)) + 
    geom_area(alpha=0.6, position='stack') +
    theme(axis.text=element_text(size=18), axis.title=element_text(size=24), title=element_text(size=24), 
          legend.text=element_text(size=18), legend.title=(element_text(size=24)),
          legend.margin=unit(0.5, "cm"), plot.margin=unit(c(1,1,1,1), "cm")) +
    labs(title = "Stanford Sequencing Center Production by Year", y = "Terabases Sequenced", x = "Month") +
    scale_x_continuous(breaks=seq(1, 12, by=1)) +
    scale_fill_discrete(name="Year")
  ggsave(file = paste(out, '_bases_stack.png', sep=''), width=14.4, height=8.1)
}

make_lane_count_plots <- function(data, out){
  
  ggplot(data, aes(x=Month, y=Lane_Count, fill=factor(Year))) + 
    geom_bar(alpha=0.6, position='dodge', stat='identity') +
    theme(axis.text=element_text(size=18), axis.title=element_text(size=24), title=element_text(size=24), 
          legend.text=element_text(size=18), legend.title=(element_text(size=24)),
          legend.margin=unit(0.5, "cm"), plot.margin=unit(c(1,1,1,1), "cm")) +
    labs(title = "Stanford Sequencing Center Production by Year", y = "Lanes Sequenced", x = "Month") +
    scale_x_continuous(breaks=seq(1, 12, by=1)) +
    scale_fill_discrete(name="Year")
  ggsave(file = paste(out, '_lanes_dodge.png', sep=''), width=14.4, height=8.1)
  
  ggplot(data, aes(x=Month, y=Lane_Count, fill=factor(Year))) + 
    geom_bar(alpha=0.6, position='stack', stat='identity') +
    theme(axis.text=element_text(size=18), axis.title=element_text(size=24), title=element_text(size=24), 
          legend.text=element_text(size=18), legend.title=(element_text(size=24)),
          legend.margin=unit(0.5, "cm"), plot.margin=unit(c(1,1,1,1), "cm")) +
    labs(title = "Stanford Sequencing Center Production by Year", y = "Lanes Sequenced", x = "Month") +
    scale_x_continuous(breaks=seq(1, 12, by=1)) +
    scale_fill_discrete(name="Year")
  ggsave(file = paste(out, '_lanes_stack.png', sep=''), width=14.4, height=8.1)
}

make_read_count_plots <- function(data, out){
  ggplot(data, aes(x=Month, y=Gigaread_Count, fill=factor(Year), group=Year)) + 
    geom_area(alpha=0.6, position='dodge') +
    theme(axis.text=element_text(size=18), axis.title=element_text(size=24), title=element_text(size=24), 
          legend.text=element_text(size=18), legend.title=(element_text(size=24)),
          legend.margin=unit(0.5, "cm"), plot.margin=unit(c(1,1,1,1), "cm")) +
    labs(title = "Stanford Sequencing Center Production by Year", y = "Billion Reads Sequenced", x = "Month") +
    scale_x_continuous(breaks=seq(1, 12, by=1)) +
    scale_fill_discrete(name="Year")
  ggsave(file = paste(out, '_reads_overlap.png', sep=''), width=14.4, height=8.1)
  
  ggplot(data, aes(x=Month, y=Gigaread_Count, fill=factor(Year), group=Year)) + 
    geom_area(alpha=0.6, position='stack') +
    theme(axis.text=element_text(size=18), axis.title=element_text(size=24), title=element_text(size=24), 
          legend.text=element_text(size=18), legend.title=(element_text(size=24)),
          legend.margin=unit(0.5, "cm"), plot.margin=unit(c(1,1,1,1), "cm")) +
    labs(title = "Stanford Sequencing Center Production by Year", y = "Billion Reads Sequenced", x = "Month") +
    scale_x_continuous(breaks=seq(1, 12, by=1)) +
    scale_fill_discrete(name="Year")
  ggsave(file = paste(out, '_reads_stack.png', sep=''), width=14.4, height=8.1)
}

main <- function() {
  
  opt = parse_arguments()
  file = opt$file
  out = opt$out
  
  # Interactive debug line
  #file = 'ssc_seq_stats.txt'
  
  print(file)
  data = read.table(file, header=T)
  
  Terabase_Count = data$Base_Count / 1000000000000
  Gigaread_Count = data$Read_Count / 1000000000
  data = cbind(data, Terabase_Count, Gigaread_Count)
  
  # Order data
  ordered_data = data[order(data$Year, data$Month),]
  
  make_base_count_plots(ordered_data, out)
  make_lane_count_plots(ordered_data, out)
  make_read_count_plots(ordered_data, out)
  
}

if(!interactive()) {
  main()
}