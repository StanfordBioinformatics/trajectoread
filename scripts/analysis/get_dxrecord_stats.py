#!/usr/bin/env python

import re
import sys
import dxpy
from collections import defaultdict

def get_live_record_stats(outfile, stats_dict):
    ''' Description: Get all DXRecord objects created after trajectoread go-live
    date.
    '''

    # Go-live date (ETC milliseconds)
    go_live_date = 1459753200000
    go_live_timestamp = 160404
    live_records = dxpy.find_data_objects(classname='record', created_after=go_live_date)

    #n = 0
    for record_id in live_records:
        #n += 1
        record = dxpy.DXRecord(dxid=record_id['id'], project=record_id['project'])
        properties = record.get_properties()
        details = record.get_details()

        try:
            # Details
            user = details['user'].rstrip()
            upload_date = details['uploadDate']
            run_name = details['run']
            lane_index = details['lane']
            run_date = int(run_name.split('_')[0])

            # Properties
            status = properties['status']
            release_date = properties['releaseDate']
            lab_name = properties['lab_name'].rstrip()
            seq_instrument = properties['seq_instrument']
            mapper = properties['mapper']
        except:
            print 'Could not get record info for %s' % record
            continue

        compute_time = str(int(release_date) - int(upload_date))
        compute_time_hours = float(compute_time) / 1000 / 60 / 60
        compute_time_hours = str("{0:.2f}".format(compute_time_hours))

        output_list = [run_name,
                       lane_index, 
                       user, 
                       lab_name, 
                       seq_instrument, 
                       mapper, 
                       upload_date,
                       release_date,
                       compute_time,
                       compute_time_hours
                      ]

        output_dict = {
                       'user': user,
                       'lab_name': lab_name,
                       'seq_instrument': seq_instrument,
                       'mapper': mapper,
                       'upload_date': upload_date,
                       'release_date': release_date,
                       'compute_time': compute_time,
                       'compute_time_hours': compute_time_hours
                      }

        if status == 'released' and run_date >= go_live_timestamp:
            print '%s %s' % (run_name, lane_index)
            output_str = '\t'.join(output_list)
            output_str = output_str.rstrip()
            output_str += '\n'
            with open(outfile, 'a') as OUT:
                OUT.write(output_str)

            stats_dict[run_name][lane_index] = output_dict
        #if n >= 5:
        #    sys.exit()

def get_job_runtime_stats(outfile):
    ''' Description: Find all jobs run after go-live date and then get runtime info
    for the relevant ones.
    '''

    go_live_date = 1459753200000
    relevant_jobs = ['bcl2fastq', 
                     'bwa_controller',
                     'qc_controller', 
                     'generate_qc_report',
                     'release_lane'
                    ]
    job_gen = dxpy.find_executions(classname = 'job', 
                                   created_after = go_live_date, 
                                   state = 'done',
                                   launched_by = 'user-pbilling'
                                  )
    #n = 0
    for job_id in job_gen:
        #n +=1
        job = dxpy.DXJob(job_id['id'])
        job_name = job.describe()['executableName']
        print job_name

        if job_name in relevant_jobs:
            job_description = job.describe()
            job_dxid = job_description['id']
            job_start = job_description['created']
            job_end = job_description['modified']
            job_project_dxid = job_description['project']

            compute_time = str(int(job_end) - int(job_start))
            compute_time_hours = float(compute_time) / 1000 / 60 / 60
            compute_time_hours = str("{0:.2f}".format(compute_time_hours))

            # Get sequencing instrument
            job_project = dxpy.DXProject(job_project_dxid)
            project_name = job_project.describe()['name']
            seq_instrument = project_name.split('_')[1]

            output_list = [project_name,
                           seq_instrument,
                           job_name,
                           job_dxid,
                           str(job_start),
                           str(job_end),
                           compute_time,
                           compute_time_hours
                          ]
            print '%s %s' % (project_name, job_name)
            output_str = '\t'.join(output_list)
            output_str = output_str.rstrip()
            output_str += '\n'

            with open(outfile, 'a') as OUT:
                OUT.write(output_str)
        #if n >= 30:
        #    sys.exit()

def parse_lane_html(html_file, lane_name, outfile):
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

                output_list = [str(lane_name),
                               str(name_elements[1]),
                               str(pf_clusters),
                               str(yield_mbases),
                               str(mean_quality),
                               str(perc_q30_bases)
                              ]
                
                with open(outfile, 'a') as OUT:
                    out_str = '\t'.join(output_list)
                    out_str += '\n'
                    OUT.write(out_str)

                return (lane_output)

def parse_html_value(html_line, lane_name):
    html_match = re.search('<td>(.+)</td>', html_line)
    if html_match:
        value = html_match.group(1)
        return value
    else:
        print 'Error: could not find value in html line: %s, Lane %s' % (html_line, lane_name)
        return 'NA'

def main():

    # Add info from each function to a lane stats dict
    #run_stats = {run1: {lane1: {values}, lane2: {values}}}
    run_stats = defaultdict(dict)

    record_stats_file = '160502_trajectoread_dxrecord_stats.txt'
    job_runtime_stats_file = '160502_trajectoread_dxjob_stats.txt'
    lane_html_stats_file = '160504_trajectoread_lanehtml_stats.txt'

    # Figure 1A, 1B
    #get_live_record_stats(outfile=record_stats_file, stats_dict=run_stats)
    # Figure 2
    #get_job_runtime_stats(outfile=job_runtime_stats_file)

    # Download all lane.html files from DNAnexus using API call
    # 1. Find lane.html files
    go_live_date = 1459753200000
    lane_html_file_gen = dxpy.find_data_objects(name="*.lane.html", 
                                                name_mode="glob",
                                                created_after=go_live_date)

    # 2. Parse DNAnexus lane.html files
    #n = 0
    for file_id in lane_html_file_gen:
        #n += 1
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
        platform = lane_name.split('_')[1]

        # 4. Download dx files locally
        print 'Info: Downloading run %s lane %d' % (run_name, lane_index)
        dxpy.download_dxfile(dxid = file_id['id'], 
                             project = file_id['project'], 
                             filename = filename
                            )

        # 5. Parse lane.html file
        print 'Info: Parsing lane html file: %s' % dxfile.name
        lane_output = parse_lane_html(html_file = filename, 
                                      lane_name = lane_name,
                                      outfile = lane_html_stats_file
                                     )
        #lane_metrics['dx'][lane_name] = lane_output

        #if n >= 5:
        #    sys.exit()


if __name__ == "__main__":
    main()