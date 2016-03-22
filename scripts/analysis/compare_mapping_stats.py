#!/usr/bin/env python

import re
import os
import sys
import pdb
import glob
import dxpy
import json
import numpy
import shutil
import fnmatch
import datetime

def parse_stats_json(json_file):
    with open(json_file, 'r') as JSON:
        data = json.load(JSON)
 
    post_filter_reads = 0
    mapped_pf_reads_1 =  0
    mapped_pf_reads_2 = 0
    unique_pf_reads_1 = 0
    unique_pf_reads_2 = 0
    insert_size_list = []

    for sample in data:
        #pdb.set_trace()
        if len(sample) == 1:
            # No mapping stats
            post_filter_reads = 'NA'
            mapped_pf_reads_1 = 'NA'
            mapped_pf_reads_2 = 'NA'
            unique_pf_reads_1 = 'NA'
            unique_pf_reads_2 = 'NA'
            mean_insert_size = 'NA'
            break
        
        post_filter_reads += int(sample['Pair']['Post-Filter Reads'])
        mapped_pf_reads_1 += int(sample['Read 1']['Mapped PF Reads'])
        mapped_pf_reads_2 += int(sample['Read 2']['Mapped PF Reads'])
        for category in sample['Read 1']['Unique']:
            unique_pf_reads_1 += sample['Read 1']['Unique'][category]
        for category in sample['Read 2']['Unique']:
            unique_pf_reads_2 += sample['Read 2']['Unique'][category]
        insert_size_list.append(int(sample['Mean Insert Size']))
    try:
        mean_insert_size = int(numpy.mean(insert_size_list))
    except:
        mean_insert_size = 'NA'

    lane_metrics = {
                    'post_filter_reads': post_filter_reads,
                    'mapped_pf_reads_1': mapped_pf_reads_1,
                    'mapped_pf_reads_2': mapped_pf_reads_2,
                    'unique_pf_reads_1': unique_pf_reads_1,
                    'unique_pf_reads_2': unique_pf_reads_2,
                    'mean_insert_size': mean_insert_size
                   }
    return lane_metrics
  
def parse_stats_csv(csv_path):
    lane_metrics = {}
    with open(csv_path, 'r') as CSV:
        for line in CSV:
            post_filter_match = re.search(r'^Post-Filter Reads,"([\d,]+)"', line)
            mapped_pf_reads_1_match = re.search(r'^Mapped PF Reads \(Read 1\),\"([\d,]+)\"', line)
            mapped_pf_reads_2_match = re.search(r'^Mapped PF Reads \(Read 2\),\"([\d,]+)\"', line)
            unique_pf_reads_1_match = re.search(r'^Uniquely-Mapped PF Reads \(Read 1\),\"([\d,]+)\"', line)
            unique_pf_reads_2_match = re.search(r'^Uniquely-Mapped PF Reads \(Read 2\),\"([\d,]+)\"', line)
            insert_size_match = re.search(r'^Insert Size,([\d,]+)', line)

            if post_filter_match:
                lane_metrics['post_filter_reads'] = int(post_filter_match.group(1).replace(',','')) * 2
            elif mapped_pf_reads_1_match:
                lane_metrics['mapped_pf_reads_1'] = int(mapped_pf_reads_1_match.group(1).replace(',',''))
            elif mapped_pf_reads_2_match:
                lane_metrics['mapped_pf_reads_2'] = int(mapped_pf_reads_2_match.group(1).replace(',',''))
            elif unique_pf_reads_1_match:
                lane_metrics['unique_pf_reads_1'] = int(unique_pf_reads_1_match.group(1).replace(',',''))
            elif unique_pf_reads_2_match:
                lane_metrics['unique_pf_reads_2'] = int(unique_pf_reads_2_match.group(1).replace(',',''))
            elif insert_size_match:
                lane_metrics['mean_insert_size'] = int(insert_size_match.group(1).replace(',',''))
    return lane_metrics

def get_dx_stats_jsons(output_dir):

    sample_stats_json_gen = dxpy.find_data_objects(name="sample_stats.json", name_mode="exact")

    # 2. Parse DNAnexus lane.html files
    for file_id in sample_stats_json_gen:
        print 'Info: Getting dxfile info: %s' % file_id['id']
        dxfile = dxpy.DXFile(dxid=file_id['id'], project=file_id['project'])
        
        dxproject = dxpy.DXProject(dxid=dxfile.project)
        project_name = dxproject.name
        project_elements = project_name.split('_')
        print project_elements

        run_name = '_'.join(project_elements[:-1])
        lane_index_match = re.search(r'L(\d)', project_elements[-1])
        if lane_index_match:
            lane_index = int(lane_index_match.group(1))

            filename = '%s.sample_stats.json' % project_name
            filepath = os.path.join(output_dir, filename)

            # 4. Download dx files locally
            print 'Info: Downloading run %s lane %d' % (run_name, lane_index)
            dxpy.download_dxfile(dxid = file_id['id'], 
                                 project = file_id['project'], 
                                 filename = filepath
                                )
        else:
            print 'Skipping %s' % run_name

def parse_dx_stats(stats_json_dir, lane_metrics):
    for file in os.listdir(stats_json_dir):
        match = re.search(r'.*.sample_stats.json', file)
        if match:
            elements = file.split('.')
            lane_name = elements[0]
            json_path = os.path.join(stats_json_dir, file)
            print 'Info: Getting dx mapping stats for %s' % file
            lane_metrics['dx'][lane_name] = parse_stats_json(json_file=json_path)

def parse_scg_stats(stats_csv_dir, lane_metrics):
    for file in os.listdir(stats_csv_dir):
        match = re.search(r'.*.stats.csv', file)
        if match:
            elements = file.split('.')
            lane_name = elements[0]
            lane_path = os.path.join(stats_csv_dir, file)
            print 'Info: Getting mapping stats for %s' % file
            lane_metrics['scg'][lane_name] = parse_stats_csv(csv_path=lane_path)

def write_output(output_dir, lane_metrics):

    timestamp = str(datetime.datetime.now()).split()[0] # yyyy-mm-dd
    out_file = '%s_mapping_stats.txt' % timestamp
    out_path = os.path.join(output_dir, out_file)
    with open(out_path, 'w') as OUT:
        dx_header = 'dx_post_filter_reads\tdx_mapped_pf_1\tdx_mapped_pf_2\tdx_unique_pf_1\tdx_unique_pf_2\tdx_insert_size\t'
        scg_header = 'scg_post_filter_reads\tscg_mapped_pf_1\tscg_mapped_pf_2\tscg_unique_pf_1\tscg_unique_pf_2\tscg_insert_size\n'
        header = 'lane_name\t' + dx_header + scg_header
        OUT.write(header)
        for lane in lane_metrics['dx']: 
            # Get DNAnexus values
            out_str = '%s\t' % lane
            try:
                out_str += '%d\t' % int(lane_metrics['dx'][lane]['post_filter_reads'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get post_filter_reads value for dx:%s' % lane
            try:
                out_str += '%s\t' % str(lane_metrics['dx'][lane]['mapped_pf_reads_1'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get mapped_pf_reads_1 value for dx:%s' % lane
            try:
                out_str += '%s\t' % str(lane_metrics['dx'][lane]['mapped_pf_reads_2'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get mapped_pf_reads_2 value for dx:%s' % lane
            try:
                out_str += '%s\t' % str(lane_metrics['dx'][lane]['unique_pf_reads_1'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get unique_pf_reads_1 value for dx:%s' % lane
            try:
                out_str += '%d\t' % int(lane_metrics['dx'][lane]['unique_pf_reads_2'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get unique_pf_reads_2 value for scg:%s' % lane
            try:
                out_str += '%s\t' % str(lane_metrics['dx'][lane]['mean_insert_size'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get mean_insert_size value for scg:%s' % lane
            # Get SCG values
            try:
                out_str += '%d\t' % int(lane_metrics['scg'][lane]['post_filter_reads'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get post_filter_reads value for scg:%s' % lane
            try:
                out_str += '%s\t' % str(lane_metrics['scg'][lane]['mapped_pf_reads_1'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get mapped_pf_reads_1 value for scg:%s' % lane
            try:
                out_str += '%s\t' % str(lane_metrics['scg'][lane]['mapped_pf_reads_2'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get mapped_pf_reads_2 value for scg:%s' % lane
            try:
                out_str += '%s\t' % str(lane_metrics['scg'][lane]['unique_pf_reads_1'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get unique_pf_reads_1 value for scg:%s' % lane
            try:
                out_str += '%d\t' % int(lane_metrics['scg'][lane]['unique_pf_reads_2'])
            except:
                out_str += 'NA\t'
                print 'Warning: Could not get unique_pf_reads_2 value for scg:%s' % lane
            try:
                out_str += '%s\n' % str(lane_metrics['scg'][lane]['mean_insert_size'])
            except:
                out_str += 'NA\n'
                print 'Warning: Could not get mean_insert_size value for scg:%s' % lane
            OUT.write(out_str)


def main():

    dx_mapping_stats_dir = '/srv/gsfs0/projects/gbsc/workspace/pbilling/dx_mapping_stats'
    scg_mapping_stats_dir = '/srv/gsfs0/projects/gbsc/workspace/pbilling/scg_mapping_stats'
    analysis_dir = '/srv/gsfs0/projects/gbsc/workspace/pbilling/seq_stats_analysis'
    scg_2016_pub_dir = '/srv/gsfs0/projects/gbsc/SeqCenter/Illumina/PublishedResults/2016'

    # Test
    #scg_mapping_stats_dir = '/srv/gsfs0/projects/gbsc/workspace/pbilling/scg_test'
    # Get files
    #get_dx_stats_jsons(output_dir = dx_mapping_stats_dir)
    #get_scg_stats_csvs(scg_pub_dir = scg_2016_pub_dir)

    # For DX files, I think I will need to use json module to do parsing/addition
    lane_metrics = {
                    'dx': {},
                    'scg': {}
                   }

    parse_scg_stats(scg_mapping_stats_dir, lane_metrics)
    parse_dx_stats(dx_mapping_stats_dir, lane_metrics)

    write_output(output_dir=analysis_dir, lane_metrics=lane_metrics)
    
    #pdb.set_trace()

if __name__ == '__main__':
    main()
