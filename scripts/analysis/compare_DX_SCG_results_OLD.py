#!/usr/bin/env python

import re
import os
import sys
import pdb
import glob
import dxpy

# Compare lane.html between DNAnexus and SCG

# First read all the files listed in DX lane.html directory and parse out runs
# Use that to create lane hash (?)

dx_runs_dir = '/srv/gsfs0/projects/gbsc/workspace/pbilling/dx_lane_htmls'
scg_runs_dir = '/srv/gsfs0/projects/gbsc/SeqCenter/Illumina/RunsInProgress'

lanes = {}

def parse_hiseq4000_data(run_dir, run_name, lane_index, fcid):
    lane_path = '%s/%s/Unaligned_L%d' % (run_dir, run_name, lane_index)

    lane_html_path_1 = '%s/Reports/%s/all/all/all/lane.html' % (lane_path, fcid)
    lane_html_path_2 = '%s/html/%s/all/all/all/lane.html' % (lane_path, fcid)
    lane_html_path_3 = '%s/Reports/html/%s/all/all/all/lane.html' % (lane_path, fcid)

    if os.path.exists(lane_html_path_1):
        lane_html_path = lane_html_path_1
    elif os.path.exists(lane_html_path_2):
        lane_html_path = lane_html_path_2
    elif os.path.exists(lane_html_path_3):
        lane_html_path = lane_html_path_3
    else:
        print "Error: Cannot find lane %d path for %s" % (lane_index, run_name)
        sys.exit()

    print 'Info: lane.html path: %s' % lane_html_path
    with open(lane_html_path, 'r') as HTML:
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
                
                pf_clusters = parse_html_value(pf_clusters_line, run_name, lane_index)
                perc_perfect_barcode = parse_html_value(perc_perfect_barcode_line, run_name, lane_index)
                perc_one_mismatch_barcode = parse_html_value(perc_one_mismatch_barcode_line, run_name, lane_index)
                perc_pf = parse_html_value(perc_pf_line, run_name, lane_index)
                perc_q30_bases = parse_html_value(perc_q30_bases_line, run_name, lane_index)
                mean_quality = parse_html_value(mean_quality_line, run_name, lane_index)
                
                yield_mbases_raw = parse_html_value(yield_line, run_name, lane_index)
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

def parse_pipeline_html_data(run_dir, run_name, lane_index):
    lane_path = '%s/%s/Unaligned_L%d' % (run_dir, run_name, lane_index)
    
    demulti_stats_gen = glob.iglob('%s/Basecall_Stats_*/Demultiplex_Stats.htm' % lane_path)
    demulti_stats_paths = list(demulti_stats_gen)
    
    if len(demulti_stats_paths) == 1:
        lane_html_path = demulti_stats_paths[0]
        print 'Info: Found lane.html file for: %s' % lane_html_path
    else:
        print "Error: Cannot find lane %d path for %s" % (lane_index, run_name)
        return None
 
    with open(lane_html_path, 'r') as HTML:
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

                perc_perfect_barcode = parse_html_value(perc_perfect_barcode_line, run_name, lane_index)
                perc_one_mismatch_barcode = parse_html_value(perc_one_mismatch_barcode_line, run_name, lane_index)
                perc_pf = parse_html_value(perc_pf_line, run_name, lane_index)
                perc_q30_bases = parse_html_value(perc_q30_bases_line, run_name, lane_index)
                mean_quality = parse_html_value(mean_quality_line, run_name, lane_index)
                read_num = parse_html_value(read_num_line, run_name, lane_index)

                yield_mbases_raw = parse_html_value(yield_line, run_name, lane_index)
                if mean_quality == 'NA' or perc_pf == 'NA' or yield_mbases_raw == 'NA':
                    continue
                yield_elements = yield_mbases_raw.split(',')
                yield_mbases = ''.join(yield_elements) # Remove commas from mbase value

                #lane_name = '%s_L%d' % (run_name, lane_index)
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

def parse_lane_html(lane_html_path, run_name, lane_index):
    with open(lane_html_path, 'r') as HTML:
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
                
                perc_perfect_barcode = parse_html_value(perc_perfect_barcode_line, run_name, lane_index)
                perc_one_mismatch_barcode = parse_html_value(perc_one_mismatch_barcode_line, run_name, lane_index)
                perc_pf = parse_html_value(perc_pf_line, run_name, lane_index)
                perc_q30_bases = parse_html_value(perc_q30_bases_line, run_name, lane_index)
                mean_quality = parse_html_value(mean_quality_line, run_name, lane_index)
                pf_clusters = parse_html_value(pf_clusters_line, run_name, lane_index)

                yield_mbases_raw = parse_html_value(yield_line, run_name, lane_index)
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

def parse_html_value(html_line, run_name, lane_index):
    html_match = re.search('<td>(.+)</td>', html_line)
    if html_match:
        value = html_match.group(1)
        return value
    else:
        print 'Error: could not find value in html line: %s, %s, Lane %d' % (html_line, run_name, lane_index)
        return 'NA'

def main():

    home = os.getcwd()
    os.chdir(dx_runs_dir)

    lane_metrics = {
                    'dx': {},
                    'scg': {}
    }

    miseq_list = ['HOLMES','SPENSER']
    hiseq4000_list = ['GADGET','COOPER']
    hiseq2000_list = ['BRISCOE','HAVERS','LYNLEY','MARPLE','MONK','PINKERTON','TENNISON']
    ga2_list = ['MAGNUM']
    all_machines = miseq_list + hiseq4000_list + hiseq2000_list + ga2_list

    # Download all lane.html files from DNAnexus using API call
    # 1. Find lane.html files
    lane_html_file_gen = dxpy.find_data_objects(name="*.lane.html", name_mode="glob")

    # 2. Parse DNAnexus lane.html files
    for file_id in lane_html_file_gen:
        print 'Info: Getting dxfile info: %s' % file_id['id']
        dxfile = dxpy.DXFile(dxid=file_id['id'], project=file_id['project'])
        


        # 2. Get metadata properties
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

        # 3. Download dx files locally
        print 'Info: Downloading run %s lane %d' % (run_name, lane_index)
        dxpy.download_dxfile(dxid = file_id['id'], project = file_id['project'], 
                             filename = dxfile.name)

        # 4. Parse lane.html file
        print 'Info: Parsing lane html file: %s' % dxfile.name
        lane_output = parse_lane_html(dxfile.name, run_name=run_name, lane_index=lane_index)
        lane_metrics['dx'][lane_name] = lane_output
      
    # 5. Find and parseSCG file demultiplex.stats)
    print 'Info: Getting SCG runs info'
    scg_runs = next(os.walk(scg_runs_dir))[1]
    for run in scg_runs:
        print 'Info: Getting SCG run info for %s' % run
        try:
            elements = run.split('_')
            machine = elements[1]
            fcid = elements[3][1:]    # Ignore first character; A/B designation
            fcid_miseq = elements[3]
        except:
           print 'Warning: Could not parse sequencing elements from directory: %s' % run
           continue

        run_path = '%s/%s' % (scg_runs_dir, run)
        run_sub_dirs = next(os.walk(run_path))[1]
         
        for sub_dir in run_sub_dirs:
            #print '%s sub-directory: %s' % (run, sub_dir)
            #lane_match = re.search('^Unaligned_L(\d)$', sub_dir)
            lane_match = re.search(r'Unaligned_L(\d)', sub_dir)
            if lane_match:
                lane_index = int(lane_match.group(1))
                print 'Found lane %d in %s: %s' % (lane_index, run, sub_dir)
                if machine in hiseq4000_list:
                    lane_output = parse_hiseq4000_data(run_dir = scg_runs_dir, 
                                                         run_name = run,
                                                         lane_index = lane_index,
                                                         fcid = fcid
                                                        )
                elif machine in hiseq2000_list or machine in ga2_list:
                    lane_output = parse_pipeline_html_data(run_dir = scg_runs_dir, 
                                                           run_name = run, 
                                                           lane_index = lane_index
                                                          )
                elif machine in miseq_list:
                    lane_output = parse_pipeline_html_data(run_dir = scg_runs_dir, 
                                                           run_name = run, 
                                                           lane_index = lane_index
                                                          )
                else:
                    print "Cannot find machine for run: %s" % run

                lane_name = '%s_L%d' % (run, lane_index)
                lane_metrics['scg'][lane_name] = lane_output

    # 6. Write metrics to outfile
    # lane_name system yield_mbases perc_pf read_num pf_clusters perc_perfect_barcode perc_one_mismatch_barcode
    # perc_q30_bases mean_quality
    os.chdir(home)
    outfile = 'dx_scg_lane_metrics.txt'
    with open(outfile, 'w') as OUT:
        OUT.write('lane_name\tsystem\tyield_mbases\tperc_pf\tread_num\tpf_clusters\tperc_perfect_barcode\tperc_one_mismatch_barcode\tperc_q30_bases\tmean_quality\n') 
        for system in lane_metrics:
            for lane in lane_metrics[system]:
                out_str = '%s\t%s\t' % (lane, system)
                try:
                    out_str += '%d\t' % int(lane_metrics[system][lane]['yield_mbases'])
                except:
                    out_str += 'NA\t'
                    print 'Warning: Could not get yield_mbases value for %s:%s' % (system, lane)
                try:
                    out_str += '%s\t' % str(lane_metrics[system][lane]['perc_pf'])
                except:
                    out_str += 'NA\t'
                    print 'Warning: Could not get perc_pf value for %s:%s' % (system, lane)
                try:
                    out_str += '%d\t' % int(lane_metrics[system][lane]['read_num'])
                except:
                    out_str += 'NA\t' 
                try:    
                    out_str += '%s\t' % str(lane_metrics[system][lane]['pf_clusters'])
                except:
                    out_str += 'NA\t'
                    print 'Warning: Could not get pf_clusters value for %s:%s' % (system, lane)
                try:
                    out_str += '%s\t' % str(lane_metrics[system][lane]['perc_perfect_barcode'])
                except:
                    out_str += 'NA\t'
                    print 'Warning: Could not get perc_perfect_barcode value for %s:%s' % (system, lane)
                try:
                    out_str += '%s\t' % str(lane_metrics[system][lane]['perc_one_mismatch_barcode'])
                except:
                    out_str += 'NA\t'
                    print 'Warning: Could not get perc_one_mismatch_barcode value for %s:%s' % (system, lane)
                try:
                    out_str += '%s\t' % str(lane_metrics[system][lane]['perc_q30_bases'])
                except:
                    out_str += 'NA\t'
                try:
                    out_str += '%s\n' % str(lane_metrics[system][lane]['mean_quality'])
                except:
                    out_str += 'NA\n'
                OUT.write(out_str)

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
                print 'Warning: Could not get yield_mbases value for %s:%s' % (system, lane)
            try:
                out_str += '%s\t' % str(lane_metrics['dx'][lane]['perc_pf'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get perc_pf value for %s:%s' % (system, lane)
            try:
                out_str += '%s\t' % str(lane_metrics['dx'][lane]['perc_q30_bases'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get perc_q30_bases value for %s:%s' % (system, lane)
            try:
                out_str += '%s\t' % str(lane_metrics['dx'][lane]['mean_quality'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get mean_quality value for %s:%s' % (system, lane)
           # Get SCG values
            try:
                out_str += '%d\t' % int(lane_metrics['scg'][lane]['yield_mbases'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get yield_mbases value for %s:%s' % (system, lane)
            try:
                out_str += '%s\t' % str(lane_metrics['scg'][lane]['perc_pf'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get perc_pf value for %s:%s' % (system, lane)
            try:
                out_str += '%s\t' % str(lane_metrics['scg'][lane]['perc_q30_bases'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get perc_q30_bases value for %s:%s' % (system, lane)
            try:
                out_str += '%s\n' % str(lane_metrics['scg'][lane]['mean_quality'])
            except:
                out_str += 'NA\n'
                print 'Warning: Could not get mean_quality value for %s:%s' % (system, lane)
            OUT2.write(out_str)

if __name__ == '__main__':
    main()
