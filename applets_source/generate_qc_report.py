#!/usr/bin/env python

import sys
import dxpy
import subprocess
import json
import re
import textwrap
import collections
import os

MISMATCH_PER_CYCLE_STATS_FN = 'mismatch_per_cycle.stats'
RUN_DETAILS_JSON_FN = 'run_details.json'
SAMPLE_STATS_JSON_FN = 'sample_stats.json'
BARCODES_JSON_FN = 'barcodes.json'
TOOLS_USED_TXT_FN = 'tools_used.txt'

class FlowcellLane:

    def __init__(self, record_dxid, fastqs=None, interop_tar=None, dashboard_project_dxid=None):

        self.dashboard_record_dxid = record_dxid
        self.dashboard_project_dxid = dashboard_project_dxid
        if not self.dashboard_project_dxid:
            self.dashboard_project_dxid = 'project-BY82j6Q0jJxgg986V16FQzjx'
        self.dashboard_record = dxpy.DXRecord(dxid = self.dashboard_record_dxid, 
                                              project = self.dashboard_project_dxid
                                             )

        self.fastq_dxids = fastqs
        self.interop_dxid = interop_tar
        self.samples_dicts = None

        # Get relevant dashboard details
        self.details = self.dashboard_record.get_details()
        self.run_name = self.details['run']
        self.lane_index = self.details['lane']
        self.library_name = self.details['library']
        self.project_dxid = self.details['laneProject']

        # Get relevant dashboard properties
        self.properties = self.dashboard_record.get_properties()
        self.flowcell_id = self.properties['flowcell_id']
        self.lab_name = self.properties['lab_name']
        self.operator = 'None'     # Still need to grab this info

        # Get mapping info for mapped lanes
        self.mapper = self.properties['mapper']
        if self.mapper == 'None':
            self.mapper = None
            self.ref_genome_dxid = None
            self.reference_genome = None
        else:
            self.ref_genome_dxid = self.properties['reference_genome_dxid']
            self.reference_genome = self.details['mappingReference']
   
    def update_status(self, status):
        status_options = ['uploading', 'running_pipeline', 'running_casava', 'ready',
                          'reviewing', 'released'
                         ]
        if not status in status_options:
            print "Lane status: \"%s\" not a valid status option." % status
            print "Valid status options:"
            print status_options
        else:
            properties = {'status': status}
            dxpy.api.record_set_properties(object_id = self.dashboard_record_dxid, 
                                           input_params = {
                                                           'project': self.dashboard_project_dxid,
                                                           'properties': properties
                                                          }
                                          )
    def update_properties(self, properties):
        if isinstance(properties, dict):
            dxpy.api.record_set_properties(object_id = self.dashboard_record_dxid, 
                                           input_params = {
                                                           'project': self.dashboard_project_dxid,
                                                           'properties': properties
                                                          }
                                          )
        else:
            print 'Error: input is not of type dict; record properties not updated'
            print properties

def download_file(file_dxid):
    """
    Args    : dx_file - a file object ID on DNAnexus to the current working directory.
    Returns : str. Path to downloaded file.
    """
    dx_file = dxpy.DXFile(file_dxid)
    filename = dx_file.describe()['name']
    dxpy.download_dxfile(dxid=dx_file.get_id(), filename=filename)
    return filename

def create_tools_used_file(tools_used):
    """
    Args : tools_used - dict. Should be the value of the 'tools_used' parameter to main().
    """
    # First read in all of the commands used.
    tools_used_dict = collections.defaultdict(lambda: collections.Counter())
    # DEV: Commented out old code that wasn't working for me- I think 'tools_used' is list not dict
    #for tools_used_files in tools_used:
    #    for tools_used_file in tools_used_files:
    #        tools_used_fn = download_file(tools_used_file)
    #        with open(tools_used_fn) as fh:
    #            curr_json = json.loads(fh.read())
    #            for command in curr_json['commands']:
    #                #there are two keys in the curr_json:
    #                # 1) "commands". Value is a list of command-line strings.
    #                # 2) "name". Value is the user-friendly name of the applet that ran the commands.
    #                tools_used_dict[curr_json['name']][command] += 1

    for tools_used_file in tools_used:
        tools_used_fn = download_file(tools_used_file)
        with open(tools_used_fn) as fh:
            curr_json = json.loads(fh.read())
            for command in curr_json['commands']:
                #there are two keys in the curr_json:
                # 1) "commands". Value is a list of command-line strings.
                # 2) "name". Value is the user-friendly name of the applet that ran the commands.
                tools_used_dict[curr_json['name']][command] += 1


    # Now group them, format them, and print them out to file.
    tw = textwrap.TextWrapper(subsequent_indent='   ')
    with open(TOOLS_USED_TXT_FN, 'w') as fh:
        for key in tools_used_dict:
            fh.write(key.upper() + '\n')
            # Create the commands along with # of calls and sort them
            commands = []
            for command in tools_used_dict[key]:
                command += ' (x{0})'.format(tools_used_dict[key][command])
                commands.append(command)
            commands.sort()
            # And now format the commands and print them.
            for command in commands:
                fh.write(tw.fill(command) + '\n')
            fh.write('\n')
    return TOOLS_USED_TXT_FN

def group_files_by_barcode(barcoded_files):
    """
    Group FASTQ files by sample according to their SampleID and Index
    properties. Returns a dict mapping (SampleID, Index) tuples to lists of
    files.
    Note - since I have casava outputting each barcode read in a single file, the value of each group should be a single file for single-end sequencing,
     or two files for PE sequencing.
    """
    
    print("Grouping files by barcode")
    dxfiles = [dxpy.DXFile(item) for item in barcoded_files]
    sample_dict = {}

    for dxfile in dxfiles:
        props = dxfile.get_properties()
        barcode =  props["barcode"] #will be NoIndex if non-multiplex (see bcl2fatq UG sectino "FASTQ Files")
        if barcode not in sample_dict:
            sample_dict[barcode] = []
        dxlink = dxpy.dxlink(dxfile)
        sample_dict[barcode].append(dxlink)
    print("Grouped barcoded files as follows:")
    print(sample_dict)
    return sample_dict

@dxpy.entry_point("main")
def main(record_dxid, output_folder, qc_stats_jsons, tools_used, fastqs, interop_tar, 
         mismatch_metrics=[], paired_end=True, mark_duplicates=False, 
         dashboard_project_dxid=None):

    lane = FlowcellLane(record_dxid=record_dxid, fastqs=fastqs, interop_tar=interop_tar, 
                        dashboard_project_dxid=dashboard_project_dxid)
    
    # Now handle the generation of the QC PDF report.
    run_details = {'run_name': lane.run_name,
                   'flow_cell': lane.flowcell_id, #should be the same for all fq files
                   'lane': lane.lane_index,
                   'library': lane.library_name,
                   'operator': lane.operator,
                   'genome_name': lane.reference_genome,
                   'lab_name': lane.lab_name,
                   'mapper': lane.mapper}     # DEV: change this to be 'aligner' in 'create_pdf_reports.py' for consistency

    output_project = lane.project_dxid

    # Download and prepare necessary files
    #if mismatch_metrics != None:
    if len(mismatch_metrics) > 0:
        mismatch_stats_files = [download_file(mismatch_file) for mismatch_file in mismatch_metrics]
    
    interop_file = download_file(interop_tar)
    tools_used_fn = create_tools_used_file(tools_used)
    sample_dict = group_files_by_barcode(fastqs)
    
    barcodes = sample_dict.keys()

    # Merge the individual sample stats into one file.
    qc_stats_json_files = [download_file(qc_stats_file) for qc_stats_file in qc_stats_jsons]
    qc_stats = [json.loads(open(qc_json_file).read()) for qc_json_file in qc_stats_json_files]
    with open(SAMPLE_STATS_JSON_FN, 'w') as fh:
        fh.write(json.dumps(qc_stats))

    with open(RUN_DETAILS_JSON_FN, 'w') as fh:
        fh.write(json.dumps(run_details))

    with open(BARCODES_JSON_FN, 'w') as fh:
        fh.write(json.dumps(barcodes))

    # Now actually make the call to create the pdf.
    #qc_report_file = run_details['run_name'].replace(' ', '_') + '_qc_report.pdf'
    qc_report_file = '%s_L%d_QC_Report.pdf' % (lane.run_name, int(lane.lane_index))
    cmd = 'python /create_pdf_reports.py '
    #if mismatch_metrics != None:
    if len(mismatch_metrics) > 0:
        cmd += '--mismatch_files {0} '.format(' '.join(mismatch_stats_files))
    else:
        cmd += '--basic '
    cmd += '--interop {0} '.format(interop_file)
    cmd += '--run_details {0} '.format(RUN_DETAILS_JSON_FN)
    cmd += '--sample_stats {0} '.format(SAMPLE_STATS_JSON_FN)
    cmd += '--barcodes {0} '.format(BARCODES_JSON_FN)
    cmd += '--tools_used {0} '.format(tools_used_fn)
    if paired_end:
        cmd += '--paired_end '
    if mark_duplicates:
        cmd += '--collect_duplicate_info '
    cmd += '--out_file {0} '.format(qc_report_file)

    print cmd
    subprocess.check_call(cmd, shell=True)

    # Upload files
    misc_subfolder = output_folder + '/miscellany'
    qc_pdf_report = dxpy.upload_local_file(filename = qc_report_file, 
                                           project = output_project,
                                           folder = output_folder,
                                           parents = True
                                          )
    run_details_json_fid = dxpy.upload_local_file(filename = RUN_DETAILS_JSON_FN,
                                                  project = output_project,
                                                  folder = misc_subfolder,
                                                  parents = True
                                                 )
    barcodes_json_fid = dxpy.upload_local_file(filename = BARCODES_JSON_FN,
                                               project = output_project,
                                               folder = misc_subfolder,
                                               parents = True
                                              )
    sample_stats_json_fid = dxpy.upload_local_file(filename = SAMPLE_STATS_JSON_FN,
                                                   project = output_project,
                                                   folder = misc_subfolder,
                                                   parents = True
                                                  )

    output= {}
    output['qc_pdf_report'] = dxpy.dxlink(qc_pdf_report)
    output['run_details_json'] = dxpy.dxlink(run_details_json_fid)
    output['barcodes_json'] = dxpy.dxlink(barcodes_json_fid)
    output['sample_stats_json'] = dxpy.dxlink(sample_stats_json_fid)  

    #lane.update_status('ready')
    properties = {
                  'status': 'ready',
                  'qcReportID': '%s:%s' % (qc_pdf_report.project, qc_pdf_report.id),
                  'analysis_finished': 'true'
                 }
    lane.update_properties(properties)
    return output