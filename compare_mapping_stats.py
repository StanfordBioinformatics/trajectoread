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


lanes = {}

def parse_sample_stats_json(json_file):

def parse_scg_stats_csv(csv_file):
    elements = csv_file.split('.')
    lane_name = elements[0]

    with open csv_file as CSV:
        for line in CSV:
            post_filter_match = re.search(r'Post-Filter Reads,"([\d,]+)",', line)
            failed_reads_match = re.search(r'Failed Reads,"([\d,]+)",', line)
            mapped_pf_reads_1_match = re.search(r'Mapped PF Reads (Read 1),"([\d,]+)",')
            mapped_pf_reads_2_match = re.search(r'Mapped PF Reads (Read 2),"([\d,]+)",')
            unique_pf_reads_1_match = re.search(r'Uniquely-Mapped PF Reads (Read 1),"([\d,]+)",')
            unique_pf_reads_2_match = re.search(r'Uniquely-Mapped PF Reads (Read 2),"([\d,]+)",')
            insert_size_match = re.search(r'Insert Size,([\d,]+),')

            lane_metrics = {}
            if post_filter_match:
                lane_metrics['post_filter'] = post_filter_match.group(1)
            if failed_reads_match:
                lane_metrics['failed_reads_match'] = failed_reads_match.group(1)
            if mapped_pf_reads_1_match:
                lane_metrics['mapped_pf_reads_1'] = mapped_pf_reads_1_match.group(1)
            if mapped_pf_reads_2_match:
                lane_metrics['mapped_pf_reads_2'] = mapped_pf_reads_2_match.group(1)
            if unique_pf_reads_1_match:
                lane_metrics['unique_pf_reads_1'] = unique_pf_reads_1_match.group(1)
            if uniuqe_pf_reads_2_match:
                lane_metrics['unique_pf_reads_2'] = unique_pf_reads_2_match.group(1)
            if insert_size_match:
                lane_metrics['insert_size'] = insert_size_match.group(1)
            return lane_metrics


def get_scg_stats_csvs(scg_pub_dir):
    print 'Info: Getting SCG runs info'
    scg_runs = next(os.walk(scg_pub_dir))[1]
    for run_name in scg_runs:
        run_path = '%s/%s' % (scg_runs_dir, run_name)

        statscsv_gen = glob.iglob('%s/%s_L*_stats.csv' % (run_path, run_name))

        stats_csv_files = list(stats_htm_gen)

        for stats_file in stats_files:
            elements = stats_file.split('/')
            stats_name = '%s_%s.stats.csv' % (elements[0], elements[1])
            stats_path = os.path.join(scg_mapping_stats_dir, stats_name)
            shutil.copy(stats_file, stats_path)

def get_dx_sample_stats_jsons(output_dir):

    sample_stats_json_gen = dxpy.find_data_objects(name="sample_stats.json", name_mode="exact")

    # 2. Parse DNAnexus lane.html files
    for file_id in sample_stats_json_gen:
        print 'Info: Getting dxfile info: %s' % file_id['id']
        dxfile = dxpy.DXFile(dxid=file_id['id'], project=file_id['project'])
        
        dxproject = dxpy.DXProject(dxid=dxfile.project)
        project_name = dxproject.name
        project_elements = project_name.split('_')

        run_name = '_'.join(name_elements[:-1])
        lane_index_match = re.search(r'L(\d)', name_elements[-1])
        lane_index = int(lane_index_match.group(1))

        filename = '%s.sample_stats.json' % project_name
        filepath = os.path.join(output_dir, filename)

        # 4. Download dx files locally
        print 'Info: Downloading run %s lane %d' % (run_name, lane_index)
        dxpy.download_dxfile(dxid = file_id['id'], 
                             project = file_id['project'], 
                             filename = filepath
                            )

def main():

    dx_mapping_stats_dir = '/srv/gsfs0/projects/gbsc/workspace/pbilling/dx_mapping_stats'
    scg_mapping_stats_dir = '/srv/gsfs0/projects/gbsc/workspace/pbilling/scg_mapping_stats'
    scg_2016_pub_dir = '/srv/gsfs0/projects/gbsc/SeqCenter/Illumina/PublishedResults/2016'

    # Get files
    get_dx_sample_stats_jsons(output_dir = dx_mapping_stats_dir)
    get_scg_stats_csvs(scg_pub_dir = scg_2016_pub_dir)

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

    # Parse files
    for file in scg_mapping_stats_dir:
        match = re.search('(\w+_L\d).stats.csv', file)
        if match:
            lane_name = match.group(1)
            lane_metrics['scg'][lane_name] = parse_sample_scg_stats_csv(csv_file=file)


    






if __name__ == '__main__':
    main()
