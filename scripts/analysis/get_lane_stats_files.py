#!/usr/bin/env python

import re
import os
import glob
import shutil

scg_runs_dir = '/srv/gsfs0/projects/gbsc/SeqCenter/Illumina/RunsInProgress'
scg_lane_html_dir = '/srv/gsfs0/projects/gbsc/workspace/pbilling/scg_lane_htmls'

def main():
    home = os.getcwd()

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

        for stats_file in stats_files
            elements = stats_file.split('/')
            basename = elements[-1]
            match = re.search(r'Unaligned_L(\d)', elements[0])
            if match:
                lane_index = match.group(1)
                stats_name = '%s_L%d_%s' % (run_name, lane_index, basename)
                stats_path = os.path.join(scg_lane_html_dir, stats_name)
            shutil.copy(stats_file, stats_path)
