#!/usr/bin/env python

import os
import re
import glob
import shutil
import argparse

def get_scg_stats_csvs(scg_pub_dir, scg_mapping_stats_dir, year, month):
    print 'Info: Getting mapping stats for runs from %s %s' % (month, year)
    scg_runs = next(os.walk(scg_pub_dir))[1]
    for run_name in scg_runs:
        run_dir = os.path.join(scg_pub_dir, run_name)

        stats_csv_gen = glob.iglob('%s/*_stats.csv' % run_dir)
        stats_files = list(stats_csv_gen)

        for stats_file in stats_files:
            path_elements = stats_file.split('/')
            basename = path_elements[-1]
            match = re.search(r'([\w_-]+)_L(\d)_stats.csv', basename)
            if match:
                run_name = match.group(1)
                lane_index = int(match.group(2))
                stats_name = '%s_L%d.stats.csv' % (run_name, lane_index)
                stats_path = os.path.join(scg_mapping_stats_dir, stats_name)
            shutil.copy(stats_file, stats_path)

def main():

    scg_mapping_stats_dir = '/srv/gsfs0/projects/gbsc/workspace/pbilling/scg_mapping_stats'
    scg_pub_dir = '/srv/gsfs0/projects/seq_center/Illumina/PublishedResults'

    parser_description = 'Grab all lane mapping stats CSV files from a published '
    parser_description += 'month directory'
    parser = argparse.ArgumentParser(description=parser_description)
    parser.add_argument('-m', '--month', type=str, dest='month', 
                        help='Published runs month directory (jan)')
    parser.add_argument('-y', '--year', type=int, dest='year', default=2016, 
                        help='Published runs year directory (2016)')
    args = parser.parse_args()

    month = args.month
    year = str(args.year)

    scg_pub_dir = os.path.join(scg_pub_dir, year, month)

    print 'Info: Copying stats.csv files from %s to %s' % (scg_pub_dir, scg_mapping_stats_dir)
    get_scg_stats_csvs(scg_pub_dir = scg_pub_dir,
                       scg_mapping_stats_dir = scg_mapping_stats_dir,
                       year = year, 
                       month = month
                      )



if __name__ == "__main__":
    main()
