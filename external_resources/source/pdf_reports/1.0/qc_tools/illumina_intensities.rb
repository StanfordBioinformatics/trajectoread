#!/usr/bin/env ruby

#
# illumina_intensities.rb: produce statistics from Illumina extraction
# metrics and corrected intensity metrics interop files.
#
# Phil Lacroute
#

require 'optparse'
require 'tempfile'

class IlluminaIntensities

  INTENSITY_KEYS = [:a_int_avg, :c_int_avg, :g_int_avg, :t_int_avg]

  def initialize
    @max_cycle = 0
  end

  def parse_args
    opts = OptionParser.new do |opts|
      opts.banner = "Usage: illumina_intensities.rb options"
      opts.separator "Options:"

      opts.on("--extraction FILE",
              "extraction metrics input filename",
              "(e.g. ExtractionMetricsOut.bin)") do |file|
        @extraction_file = file
      end

      opts.on("--corrected_int FILE",
              "corrected intensity metrics input filename",
              "(e.g. CorrectedIntMetricsOut.bin)") do |file|
        @corrected_int_file = file
      end

      opts.on("--raw_summary_plot FILE",
              "output file for raw intensity summary plot") do |file|
        @raw_summary_plot = file
      end

      opts.on("--raw_details_plot FILE",
              "output file for raw intensity detailed plot") do |file|
        @raw_details_plot = file
      end

      opts.on("--corr_summary_plot FILE",
              "output file for corrected intensity summary plot") do |file|
        @corr_summary_plot = file
      end

      opts.on("--corr_details_plot FILE",
              "output file for corrected intensity detailed plot") do |file|
        @corr_details_plot = file
      end

      opts.on("--call_summary_plot FILE",
              "output file for base calls summary plot") do |file|
        @call_summary_plot = file
      end

      opts.on("--call_details_plot FILE",
              "output file for base calls detailed plot") do |file|
        @call_details_plot = file
      end

      opts.on("--focus_summary_plot FILE",
              "output file for focus (fwhm) summary plot") do |file|
        @focus_summary_plot = file
      end

      opts.on("--lane NUM", "lane number (required)") do |num|
        @lane = num.to_i
      end

      opts.on("--read_lengths LENGTH1,LENGTH2,LENGTH3", Array,
              "read lengths (bases)") do |list|
        @read_lengths = list
      end

      opts.on("--force", "Continue past some error conditions") do
        @force = true
      end

      opts.on("--verbose", "print verbose messages") do
        @verbose = true
      end

      opts.on("--debug", "print debug messages") do
        @debug = true
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

    if @lane.nil?
      STDERR.puts "error: --lane is required"
      exit 1
    end
  end

  def readb(ios, bytecnt, format, opts = {})
    rawdata = ios.read(bytecnt)
    if rawdata.nil?
      if opts[:required]
        STDERR.puts "error: input file is truncated"
        exit 1
      else
        return nil
      end
    end
    data = rawdata.unpack(format)
    if opts[:single]
      return data[0]
    else
      return data
    end
  end

  def read_metrics
    # Read extraction metrics file: ExtractionMetricsOut.bin
    if !@extraction_file.nil?
      puts "Reading #{@extraction_file}:" if @verbose
      @extraction = Hash.new
      (1..8).each {|lane| @extraction[lane] = Hash.new}
      ios = File.open(@extraction_file, "rb")
      version = readb(ios, 1, 'C', :required => true, :single => true)
      puts "Version: #{version}" if @verbose
      case version
      when 2
        read_extraction_metrics_v2(ios)
      else
        STDERR.puts "error: invalid file format version number #{version}"
        exit 1
      end
      ios.close
    end

    # Read corrected intensity metrics file: CorrectedIntMetricsOut.bin
    if !@corrected_int_file.nil?
      puts "Reading #{@corrected_int_file}:" if @verbose
      @corrected_int = Hash.new
      (1..8).each {|lane| @corrected_int[lane] = Hash.new}
      ios = File.open(@corrected_int_file, "rb")
      version = readb(ios, 1, 'C', :required => true, :single => true)
      puts "Version: #{version}" if @verbose
      case version
      when 1
        read_corrected_int_metrics_v1(ios)
      when 2
        read_corrected_int_metrics_v2(ios)
      when 3
        read_corrected_int_metrics_v3(ios)
      else
        STDERR.puts "error: invalid file format version number #{version}"
        exit 1
      end
      ios.close
    end

    if !@extraction_file.nil? && !@corrected_int_file.nil?
      if @extraction_max_cycle != @corrected_int_max_cycle
        STDERR.puts "error: input files have different number of cycles"
        exit 1 unless @force
      end
    end
    if !@extraction_max_cycle.nil?
      @max_cycle = @extraction_max_cycle
    elsif !@corrected_int_max_cycle.nil?
      @max_cycle = @corrected_int_max_cycle
    end
  end

  def read_extraction_metrics_v2(ios)
    record_len = readb(ios, 1, 'C', :required => true, :single => true)
    puts "Record Length: #{record_len}" if @verbose
    @extraction_max_cycle = 0
    eof = false
    while !eof do
      record = readb(ios, record_len, 'S3F4S4Q')
      break if record.nil?
      lane = record[0]
      tile = record[1]
      cycle = record[2]
      @extraction_max_cycle = cycle if cycle > @extraction_max_cycle
      if @extraction[lane].nil?
        STDERR.puts "read_extraction_metrics_v2(): error: invalid lane #{lane}"
        if @force
          next
        else
          exit 1
        end
      end
      if @extraction[lane][cycle].nil?
        @extraction[lane][cycle] = Hash.new
      end
      if @extraction[lane][cycle][tile].nil?
        @extraction[lane][cycle][tile] = {
          :tile_cnt => 0,
          :a_fwhm => 0.0,
          :c_fwhm => 0.0,
          :g_fwhm => 0.0,
          :t_fwhm => 0.0,
          :a_int => 0,
          :c_int => 0,
          :g_int => 0,
          :t_int => 0
        }
      end
      @extraction[lane][cycle][tile][:tile_cnt] += 1
      @extraction[lane][cycle][tile][:a_fwhm] = record[3]
      @extraction[lane][cycle][tile][:c_fwhm] = record[4]
      @extraction[lane][cycle][tile][:g_fwhm] = record[5]
      @extraction[lane][cycle][tile][:t_fwhm] = record[6]
      @extraction[lane][cycle][tile][:a_int] = record[7]
      @extraction[lane][cycle][tile][:c_int] = record[8]
      @extraction[lane][cycle][tile][:g_int] = record[9]
      @extraction[lane][cycle][tile][:t_int] = record[10]
    end
    puts "Found #{@extraction_max_cycle} cycles." if @verbose

    (1..8).each do |lane|
      (1..@extraction_max_cycle).each do |cycle|
        data = @extraction[lane][cycle]
        next if data.nil?
        tile_cnt = 0
        data.keys().each() do |tile|
          # Skip non_integer "tiles".
          next if not tile.is_a? Integer or tile.to_i == 0
          if data[:a_fwhm_sum].nil?
            data.merge!({
              :a_fwhm_sum => 0.0,
              :c_fwhm_sum => 0.0,
              :g_fwhm_sum => 0.0,
              :t_fwhm_sum => 0.0,
              :a_int_sum  => 0,
              :c_int_sum  => 0,
              :g_int_sum  => 0,
              :t_int_sum  => 0
            })
          end
          data[:a_fwhm_sum] += data[tile][:a_fwhm]
          data[:c_fwhm_sum] += data[tile][:c_fwhm]
          data[:g_fwhm_sum] += data[tile][:g_fwhm]
          data[:t_fwhm_sum] += data[tile][:t_fwhm]
          data[:a_int_sum] += data[tile][:a_int]
          data[:c_int_sum] += data[tile][:c_int]
          data[:g_int_sum] += data[tile][:g_int]
          data[:t_int_sum] += data[tile][:t_int]
          tile_cnt += 1
        end
        data[:a_fwhm] = data[:a_fwhm_sum] / tile_cnt
        data[:c_fwhm] = data[:c_fwhm_sum] / tile_cnt
        data[:g_fwhm] = data[:g_fwhm_sum] / tile_cnt
        data[:t_fwhm] = data[:t_fwhm_sum] / tile_cnt
        data[:a_int] = data[:a_int_sum] / tile_cnt
        data[:c_int] = data[:c_int_sum] / tile_cnt
        data[:g_int] = data[:g_int_sum] / tile_cnt
        data[:t_int] = data[:t_int_sum] / tile_cnt
      end
    end
  end

  def read_corrected_int_metrics_v1(ios)
    record_len = readb(ios, 1, 'C', :required => true, :single => true)
    puts "Record Length: #{record_len}" if @verbose
    @corrected_int_max_cycle = 0
    eof = false
    while !eof do
      record = readb(ios, record_len, 'S12L5')
      break if record.nil?
      lane = record[0]
      tile = record[1]
      cycle = record[2]
      @corrected_int_max_cycle = cycle if cycle > @corrected_int_max_cycle
      if @extraction[lane].nil?
        STDERR.puts "read_corrected_int_metrics_v1(): error: invalid lane #{lane}"
        if @force
          next
        else
          exit 1
        end
      end
      if @corrected_int[lane][cycle].nil?
        @corrected_int[lane][cycle] = Hash.new
      end
      if @corrected_int[lane][cycle][tile].nil?
        @corrected_int[lane][cycle][tile] = {
          :tile_cnt => 0,
          :a_int => 0,
          :c_int => 0,
          :g_int => 0,
          :t_int => 0,
          :a_called_int => 0,
          :c_called_int => 0,
          :g_called_int => 0,
          :t_called_int => 0,
          :n_calls => 0,
          :a_calls => 0,
          :c_calls => 0,
          :g_calls => 0,
          :t_calls => 0
        }
      end
      @corrected_int[lane][cycle][tile][:tile_cnt] += 1
      @corrected_int[lane][cycle][tile][:a_int] = record[4]
      @corrected_int[lane][cycle][tile][:c_int] = record[5]
      @corrected_int[lane][cycle][tile][:g_int] = record[6]
      @corrected_int[lane][cycle][tile][:t_int] = record[7]
      @corrected_int[lane][cycle][tile][:a_called_int] = record[8]
      @corrected_int[lane][cycle][tile][:c_called_int] = record[9]
      @corrected_int[lane][cycle][tile][:g_called_int] = record[10]
      @corrected_int[lane][cycle][tile][:t_called_int] = record[11]
      @corrected_int[lane][cycle][tile][:n_calls] = record[12]
      @corrected_int[lane][cycle][tile][:a_calls] = record[13]
      @corrected_int[lane][cycle][tile][:c_calls] = record[14]
      @corrected_int[lane][cycle][tile][:g_calls] = record[15]
      @corrected_int[lane][cycle][tile][:t_calls] = record[16]
    end
    puts "Found #{cycle} cycles." if @verbose

    (1..8).each do |lane|
      (1..@corrected_int_max_cycle).each do |cycle|
        data = @corrected_int[lane][cycle]
        next if data.nil?
        tile_cnt = 0
        data.keys().each do |tile|
          # Skip non_integer "tiles".
          next if not tile.is_a? Integer or tile.to_i == 0
          if data[:a_int_sum].nil?
            data.merge!({
              :a_int_sum => 0,
              :c_int_sum => 0,
              :g_int_sum => 0,
              :t_int_sum => 0,
              :a_called_int_sum => 0,
              :c_called_int_sum => 0,
              :g_called_int_sum => 0,
              :t_called_int_sum => 0,
              :n_calls => 0,
              :a_calls => 0,
              :c_calls => 0,
              :g_calls => 0,
              :t_calls => 0
            })
          end
          data[:a_int_sum] += data[tile][:a_int]
          data[:c_int_sum] += data[tile][:c_int]
          data[:g_int_sum] += data[tile][:g_int]
          data[:t_int_sum] += data[tile][:t_int]
          data[:a_called_int_sum] += data[tile][:a_called_int]
          data[:c_called_int_sum] += data[tile][:c_called_int]
          data[:g_called_int_sum] += data[tile][:g_called_int]
          data[:t_called_int_sum] += data[tile][:t_called_int]
          data[:n_calls] += data[tile][:n_calls]
          data[:a_calls] += data[tile][:a_calls]
          data[:c_calls] += data[tile][:c_calls]
          data[:g_calls] += data[tile][:g_calls]
          data[:t_calls] += data[tile][:t_calls]
          tile_cnt += 1
        end
        data[:a_int] = data[:a_int_sum] / tile_cnt
        data[:c_int] = data[:c_int_sum] / tile_cnt
        data[:g_int] = data[:g_int_sum] / tile_cnt
        data[:t_int] = data[:t_int_sum] / tile_cnt
        data[:a_called_int] = data[:a_called_int_sum] / tile_cnt
        data[:c_called_int] = data[:c_called_int_sum] / tile_cnt
        data[:g_called_int] = data[:g_called_int_sum] / tile_cnt
        data[:t_called_int] = data[:t_called_int_sum] / tile_cnt
      end
    end
  end

  def read_corrected_int_metrics_v2(ios)
    record_len = readb(ios, 1, 'C', :required => true, :single => true)
    puts "Record Length: #{record_len}" if @verbose
    @corrected_int_max_cycle = 0
    eof = false
    while !eof do
      record = readb(ios, record_len, 'S12F6')
      break if record.nil?
      lane = record[0]
      tile = record[1]
      cycle = record[2]
      @corrected_int_max_cycle = cycle if cycle > @corrected_int_max_cycle
      if @extraction[lane].nil?
        STDERR.puts "read_corrected_int_metrics_v2(): error: invalid lane #{lane}"
        if @force
          next
        else
          exit 1
        end
      end
      if @corrected_int[lane][cycle].nil?
        @corrected_int[lane][cycle] = Hash.new
      end
      if @corrected_int[lane][cycle][tile].nil?
        @corrected_int[lane][cycle][tile] = {
          :tile_cnt => 0,
          :a_int => 0,
          :c_int => 0,
          :g_int => 0,
          :t_int => 0,
          :a_called_int => 0,
          :c_called_int => 0,
          :g_called_int => 0,
          :t_called_int => 0,
          :n_calls => 0,
          :a_calls => 0,
          :c_calls => 0,
          :g_calls => 0,
          :t_calls => 0,
          :snr => 0.0
        }
      end
      # Only last of multiple records for lane/tile/cycle will be used.
      @corrected_int[lane][cycle][tile][:tile_cnt] += 1
      @corrected_int[lane][cycle][tile][:a_int] = record[4]
      @corrected_int[lane][cycle][tile][:c_int] = record[5]
      @corrected_int[lane][cycle][tile][:g_int] = record[6]
      @corrected_int[lane][cycle][tile][:t_int] = record[7]
      @corrected_int[lane][cycle][tile][:a_called_int] = record[8]
      @corrected_int[lane][cycle][tile][:c_called_int] = record[9]
      @corrected_int[lane][cycle][tile][:g_called_int] = record[10]
      @corrected_int[lane][cycle][tile][:t_called_int] = record[11]
      @corrected_int[lane][cycle][tile][:n_calls] = record[12]
      @corrected_int[lane][cycle][tile][:a_calls] = record[13]
      @corrected_int[lane][cycle][tile][:c_calls] = record[14]
      @corrected_int[lane][cycle][tile][:g_calls] = record[15]
      @corrected_int[lane][cycle][tile][:t_calls] = record[16]
      @corrected_int[lane][cycle][tile][:snr] = record[17]
    end
    puts "Found #{cycle} cycles." if @verbose

    (1..8).each do |lane|
      (1..@corrected_int_max_cycle).each do |cycle|
        data = @corrected_int[lane][cycle]
        next if data.nil?
        tile_cnt = 0
        data.keys().each do |tile|
          if data[:a_int_sum].nil?
            data.merge!({
              :a_int_sum => 0,
              :c_int_sum => 0,
              :g_int_sum => 0,
              :t_int_sum => 0,
              :a_called_int_sum => 0,
              :c_called_int_sum => 0,
              :g_called_int_sum => 0,
              :t_called_int_sum => 0,
              :n_calls => 0,
              :a_calls => 0,
              :c_calls => 0,
              :g_calls => 0,
              :t_calls => 0,
              :snr_sum => 0.0
            })
          end
          data[:a_int_sum] += data[tile][:a_int]
          data[:c_int_sum] += data[tile][:c_int]
          data[:g_int_sum] += data[tile][:g_int]
          data[:t_int_sum] += data[tile][:t_int]
          data[:a_called_int_sum] += data[tile][:a_called_int]
          data[:c_called_int_sum] += data[tile][:c_called_int]
          data[:g_called_int_sum] += data[tile][:t_called_int]
          data[:t_called_int_sum] += data[tile][:g_called_int]
          data[:n_calls] += data[tile][:n_calls]
          data[:a_calls] += data[tile][:a_calls]
          data[:c_calls] += data[tile][:c_calls]
          data[:g_calls] += data[tile][:g_calls]
          data[:t_calls] += data[tile][:t_calls]
          data[:snr_sum] += data[tile][:snr]
          tile_cnt += 1
        end

        data[:a_int] = data[:a_int_sum] / tile_cnt
        data[:c_int] = data[:c_int_sum] / tile_cnt
        data[:g_int] = data[:g_int_sum] / tile_cnt
        data[:t_int] = data[:t_int_sum] / tile_cnt
        data[:a_called_int] = data[:a_called_int_sum] / tile_cnt
        data[:c_called_int] = data[:c_called_int_sum] / tile_cnt
        data[:g_called_int] = data[:g_called_int_sum] / tile_cnt
        data[:t_called_int] = data[:t_called_int_sum] / tile_cnt
        data[:snr_avg] = data[:snr_sum] / tile_cnt

      end
    end
  end

  def read_corrected_int_metrics_v3(ios)
    record_len = readb(ios, 1, 'C', :required => true, :single => true)
    puts "Record Length: #{record_len}" if @verbose
    @corrected_int_max_cycle = 0
    eof = false
    while !eof do
      record = readb(ios, record_len, 'S7L5')
      break if record.nil?
      lane = record[0]
      tile = record[1]
      cycle = record[2]
      @corrected_int_max_cycle = cycle if cycle > @corrected_int_max_cycle
      if @extraction[lane].nil?
        STDERR.puts "read_corrected_int_metrics_v3(): error: invalid lane #{lane}"
        if @force
          next
        else
          exit 1
        end
      end
      if @corrected_int[lane][cycle].nil?
        @corrected_int[lane][cycle] = Hash.new
      end
      if @corrected_int[lane][cycle][tile].nil?
        @corrected_int[lane][cycle][tile] = {
          :tile_cnt => 0,
          :a_called_int => 0,
          :c_called_int => 0,
          :g_called_int => 0,
          :t_called_int => 0,
          :n_calls => 0,
          :a_calls => 0,
          :c_calls => 0,
          :g_calls => 0,
          :t_calls => 0,
        }
      end
      # Only last of multiple records for lane/tile/cycle will be used.
      @corrected_int[lane][cycle][tile][:tile_cnt] += 1
      @corrected_int[lane][cycle][tile][:a_called_int] = record[3]
      @corrected_int[lane][cycle][tile][:c_called_int] = record[4]
      @corrected_int[lane][cycle][tile][:g_called_int] = record[5]
      @corrected_int[lane][cycle][tile][:t_called_int] = record[6]
      @corrected_int[lane][cycle][tile][:n_calls] = record[7]
      @corrected_int[lane][cycle][tile][:a_calls] = record[8]
      @corrected_int[lane][cycle][tile][:c_calls] = record[9]
      @corrected_int[lane][cycle][tile][:g_calls] = record[10]
      @corrected_int[lane][cycle][tile][:t_calls] = record[11]
    end
    puts "Found #{cycle} cycles." if @verbose

    (1..8).each do |lane|
      (1..@corrected_int_max_cycle).each do |cycle|
        data = @corrected_int[lane][cycle]
        next if data.nil?
        tile_cnt = 0
        data.keys().each do |tile|
          if data[:a_int_sum].nil?
            data.merge!({
              :a_called_int_sum => 0,
              :c_called_int_sum => 0,
              :g_called_int_sum => 0,
              :t_called_int_sum => 0,
              :n_calls => 0,
              :a_calls => 0,
              :c_calls => 0,
              :g_calls => 0,
              :t_calls => 0,
            })
          end
          data[:a_called_int_sum] += data[tile][:a_called_int]
          data[:c_called_int_sum] += data[tile][:c_called_int]
          data[:g_called_int_sum] += data[tile][:t_called_int]
          data[:t_called_int_sum] += data[tile][:g_called_int]
          data[:n_calls] += data[tile][:n_calls]
          data[:a_calls] += data[tile][:a_calls]
          data[:c_calls] += data[tile][:c_calls]
          data[:g_calls] += data[tile][:g_calls]
          data[:t_calls] += data[tile][:t_calls]
          tile_cnt += 1
        end
        data[:a_called_int] = data[:a_called_int_sum] / tile_cnt
        data[:c_called_int] = data[:c_called_int_sum] / tile_cnt
        data[:g_called_int] = data[:g_called_int_sum] / tile_cnt
        data[:t_called_int] = data[:t_called_int_sum] / tile_cnt
      end
    end
  end

  def make_plots
    if !@read_lengths.nil?
      @read_starts = []
      cycle = 1
      @read_lengths.each do |len|
        @read_starts.push(cycle) unless cycle == 1
        cycle += len.to_i
      end
    end

    make_raw_intensity_plots(@lane) unless @extraction_file.nil?
    make_focus_plots(@lane) unless @extraction_file.nil?
    make_corrected_intensity_plots(@lane) unless @corrected_int_file.nil?
    make_basecall_plots(@lane) unless @corrected_int_file.nil?
  end

  def make_raw_intensity_plots(lane)
    tmpio = Tempfile.new('interop')
    puts "Writing data to #{tmpio.path}..." if @verbose
    STDERR.puts "RAW INTENSITY PLOTS" if @debug
    tmpio.puts "a c g t"
    STDERR.puts if @debug
    (1..@max_cycle).each do |cycle|
      cdata = @extraction[lane][cycle]
      next if @force and cdata.nil?
      line_out = "#{cdata[:a_int]} #{cdata[:c_int]} #{cdata[:g_int]} #{cdata[:t_int]}\n"
      tmpio.print line_out
      STDERR.print line_out if @debug
    end
    tmpio.close

    dirname = File.expand_path(File.dirname(__FILE__))
    run_plotter(dirname+"/plot_intensity_summary.r", tmpio.path, @raw_summary_plot) unless @raw_summary_plot.nil?
    run_plotter(dirname+"/plot_intensity_details.r", tmpio.path, @raw_details_plot) unless @raw_details_plot.nil?

    tmpio.unlink
  end

  def make_focus_plots(lane)
    tmpio = Tempfile.new('interop')
    puts "Writing data to #{tmpio.path}..." if @verbose
    STDERR.puts "FOCUS PLOTS" if @debug
    tmpio.puts "a c g t"
    STDERR.puts "a c g t" if @debug
    (1..@max_cycle).each do |cycle|
      cdata = @extraction[lane][cycle]
      next if @force and cdata.nil?
      line_out = "#{cdata[:a_fwhm]} #{cdata[:c_fwhm]} #{cdata[:g_fwhm]} #{cdata[:t_fwhm]}\n"
      tmpio.print line_out
      STDERR.print line_out if @debug
    end
    tmpio.close

    dirname = File.expand_path(File.dirname(__FILE__))
    run_plotter(dirname+"/plot_fwhm_summary.r", tmpio.path, @focus_summary_plot) unless @focus_summary_plot.nil?

    tmpio.unlink
  end

  def make_corrected_intensity_plots(lane)
    tmpio = Tempfile.new('interop')
    puts "Writing data to #{tmpio.path}..." if @verbose
    STDERR.puts "CORRECTED INTENSITY PLOTS" if @debug
    tmpio.puts "a c g t"
    STDERR.puts "a c g t" if @debug
    (1..@max_cycle).each do |cycle|
			puts cycle
      cdata = @corrected_int[lane][cycle]
      next if @force and cdata.nil?
      line_out = "#{cdata[:a_int]} #{cdata[:c_int]} #{cdata[:g_int]} #{cdata[:t_int]}\n"
      tmpio.print line_out
      STDERR.print line_out if @debug
    end
    tmpio.close

    dirname = File.expand_path(File.dirname(__FILE__))
    run_plotter(dirname+"/plot_intensity_summary.r", tmpio.path, @corr_summary_plot) unless @corr_summary_plot.nil?
    run_plotter(dirname+"/plot_intensity_details.r", tmpio.path, @corr_details_plot) unless @corr_details_plot.nil?

    tmpio.unlink
  end

  def make_basecall_plots(lane)
    tmpio = Tempfile.new('interop')
    puts "Writing data to #{tmpio.path}..." if @verbose
    STDERR.puts "BASECALL PLOTS" if @debug
    tmpio.puts "a c g t n"
    STDERR.puts "a c g t n" if @debug
    (1..@max_cycle).each do |cycle|
      cdata = @corrected_int[lane][cycle]
      next if @force and cdata.nil?
      sum = cdata[:a_calls] + cdata[:c_calls] + cdata[:g_calls] +
        cdata[:t_calls] + cdata[:n_calls]
      line_out = "#{cdata[:a_calls] * 100.0 / sum} " +
          "#{cdata[:c_calls] * 100.0 / sum} " +
          "#{cdata[:g_calls] * 100.0 / sum} " +
          "#{cdata[:t_calls] * 100.0 / sum} " +
          "#{cdata[:n_calls] * 100.0 / sum}\n"
      tmpio.print line_out
      STDERR.print line_out if @debug
    end
    tmpio.close

    dirname = File.expand_path(File.dirname(__FILE__))
    run_plotter(dirname+"/plot_call_summary.r", tmpio.path, @call_summary_plot) unless @call_summary_plot.nil?
    run_plotter(dirname+"/plot_call_details.r", tmpio.path, @call_details_plot) unless @call_details_plot.nil?

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
      STDERR.puts "error running R to make plot"
      exit 1
    end
  end

  def main
    parse_args
    read_metrics
    make_plots
  end
end

IlluminaIntensities.new.main
