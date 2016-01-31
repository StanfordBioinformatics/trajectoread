#!/usr/bin/env ruby

#
# illumina_mismatches.rb: produce per-cycle mismatch statistics
# from a BAM file containing Illumina reads mapped by BWA.
#
# Phil Lacroute
#

require 'optparse'
require 'tempfile'

class IlluminaMismatches

  def parse_args
    opts = OptionParser.new do |opts|
      opts.banner = "Usage: illumina_mismatches.rb options"
      opts.separator "Options:"

      opts.on("--bam FILE1,FILE2,...", Array,
              "BAM input filename(s) (required)") do |filelist|
        @bam_files = filelist
      end

      opts.on("--details_plot FILE", "detailed plot output file") do |file|
        @details_plot = file
      end

      opts.on("--summary_plot FILE", "summary plot output file") do |file|
        @summary_plot = file
      end

      opts.on("--read_lengths LENGTH1,LENGTH2,LENGTH3", Array,
              "read lengths (bases)") do |list|
        @read_lengths = list
      end

      opts.on("--verbose", "print verbose messages") do
        @verbose = true
      end

      opts.on_tail("-h", "--help", "print help message") do
        puts opts
        exit
      end
    end

    if ARGV.empty?
      puts opts
      exit
    end

    begin
      opts.parse!(ARGV)
    rescue OptionParser::ParseError => err
      STDERR.puts err.to_s
      exit 1
    end

    if ARGV.size > 0
      STDERR.puts "error: unexpected arguments"
      exit 1
    end

    if @bam_files.nil?
      STDERR.puts "error: --bam is required"
      exit 1
    end
  end

  def make_plots
    # collect per-cycle mismatch data and put it in a temporary file
    tmpio = Tempfile.new('interop')
    dirname = File.expand_path(File.dirname(__FILE__))
    datacmd = dirname+"/bwa_mismatches --out #{tmpio.path} #{@bam_files.join(' ')}"
    datacmd += " --verbose" if @verbose
    puts "Preparing data..." if @verbose
    puts datacmd if @verbose
    if !system(datacmd)
      STDERR.puts "error running bwa_mismatches"
      exit 1
    end

    # compute read start cycles
    if !@read_lengths.nil?
      @read_starts = []
      cycle = 1
      @read_lengths.each do |len|
        @read_starts.push(cycle) unless cycle == 1
        cycle += len.to_i
      end
    end

    # make plots
    dirname = File.expand_path(File.dirname(__FILE__))
    run_plotter(dirname+"/plot_mismatch_details.r", tmpio.path, @details_plot)
    run_plotter(dirname+"/plot_mismatch_summary.r", tmpio.path, @summary_plot)

    tmpio.unlink
  end

  def run_plotter(plotscript, datafile, plotfile)
    return if plotfile.nil?
    plotcmd = ("#{plotscript} datafile=\\\"#{datafile}\\\" " +
               "plotfile=\\\"#{plotfile}\\\"")
    if !@read_lengths.nil?
      plotcmd += " read.starts=c\\(#{@read_starts.join(',')}\\)"
    end
    puts "Running cmd: #{plotcmd}" if @verbose
    if !system(plotcmd)
      STDERR.puts "error running R to make line plot"
      exit 1
    end
  end

  def main
    parse_args
    make_plots
  end
end

IlluminaMismatches.new.main
