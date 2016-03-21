#!/usr/bin/env python

import re
import os
import sys
import pdb
import glob
import dxpy
import numpy
import shutil
import fnmatch

# Compare lane.html between DNAnexus and SCG

# First read all the files listed in DX lane.html directory and parse out runs
# Use that to create lane hash (?)

dx_htmls_dir = '/srv/gsfs0/projects/gbsc/workspace/pbilling/dx_lane_htmls'
scg_htmls_dir = '/srv/gsfs0/projects/gbsc/workspace/pbilling/scg_lane_htmls'
scg_runs_dir = '/srv/gsfs0/projects/seq_center/Illumina/RunsInProgress'

lanes = {}

def parse_demultiplex_stats(html_file, lane_name):
    elements = html_file.split('.')
    name_elements = elements[0].split('_')

    match = re.search(r'L(\d)', name_elements[-1])
    if match:
        lane_index = int(match.group(1))
 
    all_perc_perfect_barcode = []
    all_perc_one_mismatch_barcode = []
    all_perc_pf = []
    all_perc_q30_bases = []
    all_mean_quality = []
    all_read_num = []
    all_yield_mbases = []

    with open(html_file, 'r') as HTML:
        lines = HTML.readlines()
        for i in range(0, len(lines)):
            lane_match = re.search('<td>%d</td>' % lane_index, lines[i])
            if lane_match:
                yield_line =                        lines[i + 7]
                perc_pf_line =                      lines[i + 8]
                read_num_line =                     lines[i + 9]
                perc_perfect_barcode_line =         lines[i + 11]
                perc_one_mismatch_barcode_line =    lines[i + 12]
                perc_q30_bases_line =               lines[i + 13]
                mean_quality_line =                 lines[i + 14]

                perc_perfect_barcode = parse_html_value(perc_perfect_barcode_line, lane_name)
                perc_one_mismatch_barcode = parse_html_value(perc_one_mismatch_barcode_line, lane_name)
                perc_pf = parse_html_value(perc_pf_line, lane_name)
                perc_q30_bases = parse_html_value(perc_q30_bases_line, lane_name)
                mean_quality = parse_html_value(mean_quality_line, lane_name)
                read_num = parse_html_value(read_num_line, lane_name)

                yield_mbases_raw = parse_html_value(yield_line, lane_name)
                if mean_quality == 'NA' or perc_pf == 'NA' or yield_mbases_raw == 'NA':
                    continue
                yield_elements = yield_mbases_raw.split(',')
                yield_mbases = ''.join(yield_elements) # Remove commas from mbase value

                try:
                    all_perc_perfect_barcode.append(float(perc_perfect_barcode))
                except:
                    "Could not get perc_perfect_barcode for %s" % lane_name
                try:
                    all_perc_one_mismatch_barcode.append(float(perc_one_mismatch_barcode))
                except:
                    "Could not get perc_one_mismatch_barcode for %s" % lane_name
                try:
                    all_perc_pf.append(float(perc_pf))
                except:
                    "Could not get perc_pf for %s" % lane_name
                try:
                    all_perc_q30_bases.append(float(perc_q30_bases))
                except:
                    "Could not get perc_q30_bases for %s" % lane_name
                try:
                    all_mean_quality.append(float(mean_quality))
                except:
                    "Could not get mean_quality for %s" % lane_name
                try:
                    all_read_num.append(int(read_num))
                except:
                    "Could not get read_num for %s" % lane_name
                try:
                    all_yield_mbases.append(int(yield_mbases))
                except:
                    "Could not get yield_mbases for %s" % lane_name

        perc_perfect_barcode = numpy.mean(all_perc_perfect_barcode)
        perc_one_mismatch_barcode = numpy.mean(all_perc_one_mismatch_barcode)
        perc_pf = numpy.mean(all_perc_pf)
        perc_q30_bases = numpy.mean(all_perc_q30_bases)
        mean_quality = numpy.mean(all_mean_quality)
        read_num = numpy.sum(all_read_num)
        yield_mbases = numpy.sum(all_yield_mbases)

        lane_output = {
                       "yield_mbases": yield_mbases,
                       "perc_pf": perc_pf,
                       "read_num": read_num,
                       "perc_perfect_barcode": perc_perfect_barcode,
                       "perc_one_mismatch_barcode": perc_one_mismatch_barcode,
                       "perc_q30_bases": perc_q30_bases,
                       "mean_quality": mean_quality
                      }
        return (lane_output)

def parse_lane_html(html_file, lane_name):
    elements = html_file.split('.')
    lane_name = elements[0]
    name_elements = lane_name.split('_')

    match = re.search(r'L(\d)', name_elements[-1])
    if match:
        lane_index = int(match.group(1))

    with open(html_file, 'r') as HTML:
        lines = HTML.readlines()
        for i in range(0, len(lines)):
            lane_match = re.search('<td>%d</td>' % lane_index, lines[i])

            if lane_match:
                pf_clusters_line = lines[i + 1]
                perc_perfect_barcode_line = lines[i + 3]
                perc_one_mismatch_barcode_line = lines[i + 4]
                yield_line = lines[i + 5]
                perc_pf_line = lines[i + 6]
                perc_q30_bases_line = lines[i + 7]
                mean_quality_line = lines[i + 8]
                
                perc_perfect_barcode = parse_html_value(perc_perfect_barcode_line, lane_name)
                perc_one_mismatch_barcode = parse_html_value(perc_one_mismatch_barcode_line, lane_name)
                perc_pf = parse_html_value(perc_pf_line, lane_name)
                perc_q30_bases = parse_html_value(perc_q30_bases_line, lane_name)
                mean_quality = parse_html_value(mean_quality_line, lane_name)
                pf_clusters = parse_html_value(pf_clusters_line, lane_name)

                yield_mbases_raw = parse_html_value(yield_line, lane_name)
                yield_elements = yield_mbases_raw.split(',')
                yield_mbases = ''.join(yield_elements) # Remove commas from mbase value

                lane_output = {
                               "yield_mbases": yield_mbases,
                               "perc_pf": perc_pf,
                               "pf_clusters": pf_clusters,
                               "perc_perfect_barcode": perc_perfect_barcode,
                               "perc_one_mismatch_barcode": perc_one_mismatch_barcode,
                               "perc_q30_bases": perc_q30_bases,
                               "mean_quality": mean_quality
                              }

                return (lane_output)

def parse_html_value(html_line, lane_name):
    html_match = re.search('<td>(.+)</td>', html_line)
    if html_match:
        value = html_match.group(1)
        return value
    else:
        print 'Error: could not find value in html line: %s, Lane %s' % (html_line, lane_name)
        return 'NA'

def get_scg_html_files(scg_runs_dir, scg_htmls_dir):
    print 'Info: Getting SCG runs info'
    scg_runs = next(os.walk(scg_runs_dir))[1]
    for run_name in scg_runs:
        run_path = '%s/%s' % (scg_runs_dir, run_name)
        os.chdir(run_path)

        stats_htm_gen = glob.iglob('Unaligned_L*/Basecall_Stats_*/Demultiplex_Stats.htm')
        lane_html_1_gen = glob.iglob('Unaligned_L*/Reports/*/all/all/all/lane.html')
        lane_html_2_gen = glob.iglob('Unaligned_L*/html/*/all/all/all/lane.html')
        lane_html_3_gen = glob.iglob('Unaligned_L*/Reports/html/*/all/all/all/lane.html')

        stats_files = list(stats_htm_gen)
        stats_files += list(lane_html_1_gen)
        stats_files += list(lane_html_2_gen)
        stats_files += list(lane_html_3_gen)

        for stats_file in stats_files:
            elements = stats_file.split('/')
            basename = elements[-1]
            match = re.search(r'Unaligned_L(\d)', elements[0])
            if match:
                lane_index = int(match.group(1))
                stats_name = '%s_L%d.%s' % (run_name, lane_index, basename)
                stats_path = os.path.join(scg_htmls_dir, stats_name)
            shutil.copy(stats_file, stats_path)

def main():

    home = os.getcwd()
    os.chdir(dx_htmls_dir)

    lane_metrics = {
                    'dx': {},
                    'scg': {}
    }

    # Download all lane.html files from DNAnexus using API call
    # 1. Find lane.html files
    lane_html_file_gen = dxpy.find_data_objects(name="*.lane.html", name_mode="glob")

    # 2. Parse DNAnexus lane.html files
    for file_id in lane_html_file_gen:
        print 'Info: Getting dxfile info: %s' % file_id['id']
        dxfile = dxpy.DXFile(dxid=file_id['id'], project=file_id['project'])
        
        # 3. Get metadata properties
        properties = dxfile.get_properties()
        try:
            run_name = properties['run_name']
            run_date = properties['run_date']
            lane_index = int(properties['lane_index'])
        except:
            dxproject = dxpy.DXProject(dxid=dxfile.project)
            project_name = dxproject.name
            name_elements = project_name.split('_')

            run_name = '_'.join(name_elements[:-1])
            lane_index_match = re.search(r'L(\d)', name_elements[-1])
            lane_index = int(lane_index_match.group(1))

        lane_name = '%s_L%d' % (run_name, lane_index)
        filename = '%s.lane.html' % lane_name

        # 4. Download dx files locally
        print 'Info: Downloading run %s lane %d' % (run_name, lane_index)
        dxpy.download_dxfile(dxid = file_id['id'], project = file_id['project'], 
                             filename = filename)

        # 5. Parse lane.html file
        print 'Info: Parsing lane html file: %s' % dxfile.name
        lane_output = parse_lane_html(filename, lane_name)
        lane_metrics['dx'][lane_name] = lane_output
      
    # 6. Get SCG files (lane.html/Demultiplex_stats.htm)
    print 'Info: Getting SCG runs info'
    get_scg_html_files(scg_runs_dir, scg_htmls_dir)

    # 7. Parse SCG stats files
    os.chdir(scg_htmls_dir)
    for html_file in os.listdir('.'):
        if fnmatch.fnmatch(html_file, '*.lane.html'):
            elements = html_file.split('.')
            lane_name = elements[0]
            lane_metrics['scg'][lane_name] = parse_lane_html(html_file, lane_name)
        elif fnmatch.fnmatch(html_file, '*.Demultiplex_Stats.htm'):
            elements = html_file.split('.')
            lane_name = elements[0]
            lane_metrics['scg'][lane_name] = parse_demultiplex_stats(html_file, lane_name)

    # 8. Write metrics to outfile
    os.chdir(home)
    outfile2 = 'dx_scg_lane_metrics2.txt'
    out_str = ''
    with open(outfile2, 'w') as OUT2:
        OUT2.write('lane_name\tdx_yield_mbases\tdx_perc_pf\tdx_perc_q30_bases\tdx_mean_quality\tscg_yield_mbases\tscg_perc_pf\tscg_perc_q30_bases\tscg_mean_quality\n')
        for lane in lane_metrics['dx']: 
            # Get DNAnexus values
            out_str = '%s\t' % lane
            try:
                out_str += '%d\t' % int(lane_metrics['dx'][lane]['yield_mbases'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get yield_mbases value for dx:%s' % lane
            try:
                out_str += '%s\t' % str(lane_metrics['dx'][lane]['perc_pf'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get perc_pf value for dx:%s' % lane
            try:
                out_str += '%s\t' % str(lane_metrics['dx'][lane]['perc_q30_bases'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get perc_q30_bases value for dx:%s' % lane
            try:
                out_str += '%s\t' % str(lane_metrics['dx'][lane]['mean_quality'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get mean_quality value for dx:%s' % lane
           # Get SCG values
            try:
                out_str += '%d\t' % int(lane_metrics['scg'][lane]['yield_mbases'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get yield_mbases value for scg:%s' % lane
            try:
                out_str += '%s\t' % str(lane_metrics['scg'][lane]['perc_pf'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get perc_pf value for scg:%s' % lane
            try:
                out_str += '%s\t' % str(lane_metrics['scg'][lane]['perc_q30_bases'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get perc_q30_bases value for scg:%s' % lane
            try:
                out_str += '%s\n' % str(lane_metrics['scg'][lane]['mean_quality'])
            except:
                out_str += 'NA\n'
                print 'Warning: Could not get mean_quality value for scg:%s' % lane
            OUT2.write(out_str)

if __name__ == '__main__':
    main()
