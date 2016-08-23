#!/usr/bin/env python

import os
import pdb
import sys
import dxpy
import json
import datetime

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'../../../'))
from scgpm_lims import Connection
from scgpm_lims import RunInfo

# Get LIMS info from dnanexus_environment.json file
with open('../../dnanexus_environment.json', 'r') as ENV:
    dx_env = json.load(ENV)
lims_url = dx_env['lims_url']
lims_token = dx_env['lims_token']

# Open CSV output file
timestamp = str(datetime.datetime.now()).split()[0]     # yyyy-mm-dd
output_file = 'dx_project_billing_%s.csv' % timestamp
OUT = open(output_file, 'w')

field_list = ['year',
              'month',
              'queue',
              'project_name',
              'project_id',
              'dnanexus_billed_to',
              'submitter_name',
              'platform_name',
              'read1_cycles',
              'read2_cycles',
              'sequencing_run',
              'sequencing_lane',
              'account1_id',
              'account1_perc',
              'account2_id',
              'account2_perc',
              'account3_id',
              'account3_perc']
header = ','.join(field_list)
header += '\n'
OUT.write(header)

# Create generator object for all projects
billing_org = 'org-scgpm'
project_generator = dxpy.find_projects(billed_to=billing_org)

# For every project...
#i = 0
for project_info in project_generator:
    #if i >= 50:
    #    break
    # Get name of project
    project_id = project_info['id']
    project_name = dxpy.DXProject(project_id).describe()['name']

    # Parse sequencing run and lane index from project name
    elements = project_name.split('_')
    if elements[0] == 'dev' or len(elements) != 5:
        continue

    try:
        year = '20%d' % int(elements[0][0:2])
        month = int(elements[0][2:4])
    except:
        continue

    run_name = '_'.join(elements[:-1])
    lane_index = elements[-1][1]

    print project_name
    print run_name

    try:
        # Look up run info & lane info from lims
        connection = Connection(lims_url=lims_url, lims_token=lims_token)
        run_info = RunInfo(conn=connection, run=run_name)
        lane_info = run_info.get_lane(lane_index)

        # Get dna_library info for lane
        library_id = int(lane_info['dna_library_id'])
        library_info = connection.getdnalibraryinfo(library_id)

        read1_cycles = run_info.get_read1_cycles()
        read2_cycles = run_info.get_read2_cycles()
        platform_name = run_info.data['platform_name']

        '''
        print '\nRUN INFO\n'
        print run_info.data

        print '\nLANE INFO\n'
        print lane_info
        for e in lane_info:
            print e

        print '\nLIBRARY INFO\n'
        print library_info
        '''

        # Get billing accounts & percents
        billing_info = {
                        'submitter_name': str(lane_info['submitter']),
                        'dnanexus_billed_to': billing_org,
                        'project_name': project_name,
                        'project_id': project_id,
                        'sequencing_run': run_name,
                        'sequencing_lane': str(lane_index),
                        'platform_name': str(platform_name),
                        'read1_cycles': str(read1_cycles),
                        'read2_cycles': str(read2_cycles),
                        'account1_id': str(library_info['billing_account']),
                        'account1_perc': str(library_info['billing_account_percent']),
                        'account2_id': str(library_info['billing_account2']),
                        'account2_perc': str(library_info['billing_account2_percent']),
                        'account3_id': str(library_info['billing_account3']),
                        'account3_perc': str(library_info['billing_account3_percent']),
                        'queue': str(lane_info['queue']),
                        'year': str(year),
                        'month': str(month)
                       }
    except:
        billing_info = {
                'dnanexus_billed_to': billing_org,
                'project_name': project_name,
                'project_id': project_id,
                'sequencing_run': run_name,
                'sequencing_lane': lane_index,
                'platform_name': 'NA',
                'read1_cycles': 'NA',
                'read2_cycles': 'NA',
                'submitter_name': 'NA',
                'account1_id': 'NA',
                'account1_perc': 'NA',
                'account2_id': 'NA',
                'account2_perc': 'NA',
                'account3_id': 'NA',
                'account3_perc': 'NA'
               }

    # Print to CSV
    output_list = []
    output_str = ''
    for field in field_list:
        output_list.append(billing_info[field])
    output_str = ','.join(output_list)
    output_str += '\n'
    OUT.write(output_str)

    #i += 1

    # Update DXRecord