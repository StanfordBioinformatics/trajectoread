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

# Compare lane.html between DNAnexus and SCG

# First read all the files listed in DX lane.html directory and parse out runs
# Use that to create lane hash (?)


lanes = {}

def parse_dx_stats_json(json_file):
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
    mean_insert_size = numpy.mean(insert_size_list)
       
    lane_metrics = {
                    'post_filter_reads': post_filter_reads,
                    'mapped_pf_reads_1': mapped_pf_reads_1,
                    'mapped_pf_reads_2': mapped_pf_reads_2,
                    'unique_pf_reads_1': unique_pf_reads_1,
                    'unique_pf_reads_2': unique_pf_reads_2,
                    'mean_insert_size': mean_insert_size
                   }
    return lane_metrics
  

def parse_sample_stats_csv(csv_path):
    lane_metrics = {}
    with open(csv_path, 'r') as CSV:
        for line in CSV:
            post_filter_match = re.search(r'Post-Filter Reads,"([\d,]+)"', line)
            failed_reads_match = re.search(r'Failed Reads,\"([\d,]+)\"', line)
            mapped_pf_reads_1_match = re.search(r'Mapped PF Reads \(Read 1\),\"([\d,]+)\"', line)
            mapped_pf_reads_2_match = re.search(r'Mapped PF Reads \(Read 2\),\"([\d,]+)\"', line)
            unique_pf_reads_1_match = re.search(r'Uniquely-Mapped PF Reads \(Read 1\),\"([\d,]+)\"', line)
            unique_pf_reads_2_match = re.search(r'Uniquely-Mapped PF Reads \(Read 2\),\"([\d,]+)\"', line)
            insert_size_match = re.search(r'Insert Size,([\d,]+)', line)

            if post_filter_match:
                lane_metrics['post_filter'] = int(post_filter_match.group(1).replace(',',''))
            elif failed_reads_match:
                lane_metrics['failed_reads_match'] = int(failed_reads_match.group(1).replace(',',''))
            elif mapped_pf_reads_1_match:
                lane_metrics['mapped_pf_reads_1'] = int(mapped_pf_reads_1_match.group(1).replace(',',''))
            elif mapped_pf_reads_2_match:
                lane_metrics['mapped_pf_reads_2'] = int(mapped_pf_reads_2_match.group(1).replace(',',''))
            elif unique_pf_reads_1_match:
                lane_metrics['unique_pf_reads_1'] = int(unique_pf_reads_1_match.group(1).replace(',',''))
            elif unique_pf_reads_2_match:
                lane_metrics['unique_pf_reads_2'] = int(unique_pf_reads_2_match.group(1).replace(',',''))
            elif insert_size_match:
                lane_metrics['insert_size'] = int(insert_size_match.group(1).replace(',',''))
    print lane_metrics
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
            lane_metrics['dx'][lane_name] = parse_dx_stats_json(json_file=json_path)

def parse_scg_stats(stats_csv_dir, lane_metrics):
    for file in os.listdir(stats_csv_dir):
        match = re.search(r'.*.stats.csv', file)
        if match:
            elements = file.split('.')
            lane_name = elements[0]
            lane_path = os.path.join(scg_csv_dir, file)
            print 'Info: Getting mapping stats for %s' % file
            lane_metrics['scg'][lane_name] = parse_scg_stats_csv(csv_path=lane_path)

def main():

    dx_mapping_stats_dir = '/srv/gsfs0/projects/gbsc/workspace/pbilling/dx_mapping_stats'
    scg_mapping_stats_dir = '/srv/gsfs0/projects/gbsc/workspace/pbilling/scg_mapping_stats'
    scg_2016_pub_dir = '/srv/gsfs0/projects/gbsc/SeqCenter/Illumina/PublishedResults/2016'

    # Get files
    #get_dx_stats_jsons(output_dir = dx_mapping_stats_dir)
    #get_scg_stats_csvs(scg_pub_dir = scg_2016_pub_dir)

    ## CSV stats
    # Total Reads
    # Post-Filter Reads
    # Failed Reads
    # Mapped PF Reads (Read 1)
    # Mapped PF Reads (Read 2)
    # Uniquely-Mapped PF Reads (Read 1)
    # Uniquely-Mapped PF Reads (Read 2)
    # Insert Size

    ## QC report (HOW DO I GET THESE?)
    # Post-Filter Reads
    # Mapped PF Reads (Read 1)
    # Mapped PF Reads (Read 2)
    # Uniquely-Mapped PF Reads (Read 1)
    # Uniquely-Mapped PF Reads (Read 2)
    # Mean Insert Size

    # For DX files, I think I will need to use json module to do parsing/addition
    lane_metrics = {
                    'dx': {},
                    'scg': {}
                   }

    # Parse SCG stats.csv files
    # parse_scg_stats(scg_mapping_stats_dir, lane_metrics)
    parse_dx_stats(dx_mapping_stats_dir, lane_metrics)

    pdb.set_trace()
    
if __name__ == '__main__':
    main()
