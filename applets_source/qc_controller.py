#!/usr/bin/python
''' Description: Controller applet for perfomring QC operations on 
sample data output from an illumina flowcell lane. Uses fastq files
or fastq and bam files as input. Calls qc_sample.py applet to perform
sample level operations.
Date: 1/17/2016
Author: Paul Billing-Ross
'''

''' Functions:
qc_sample

'''

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

    def __init__(self, record_dxid, fastqs=None, bams=None, bais=None, 
                    dashboard_project_dxid=None):

        self.dashboard_record_dxid = record_dxid
        self.dashboard_project_dxid = dashboard_project_dxid
        if not self.dashboard_project_dxid:
            self.dashboard_project_dxid = 'project-BY82j6Q0jJxgg986V16FQzjx'
        self.dashboard_record = dxpy.DXRecord(dxid = self.dashboard_record_dxid, 
                                              project = self.dashboard_project_dxid
                                             )

        self.fastq_dxids = fastqs
        self.bam_dxids = bams
        self.bai_dxids = bais
        self.interop_dxid = None
        self.samples_dicts = None

        # Get reference genome information and dx references
        self.properties = self.dashboard_record.get_properties()
        self.ref_genome_dxid = self.properties['reference_genome_dxid']
        self.reference_genome = self.properties['reference_genome']
        self.lane_project_dxid = self.properties['lane_project_dxid']
        self.run_name = self.properties['run']
        self.flowcell_id = self.properties['flowcell_id']
        self.lane_index = self.properties['lane_index']
        self.library_name = self.properties['library']
        self.operator = self.properties['operator']
        self.aligner = self.properties['aligner']
        self.lab_name = self.properties['lab_name']

        # Get fastq files information and dx references
        if not self.fastq_dxids:
            self.fastq_dxids = self.find_fastq_files()
        if not self.bam_dxids:
            self.bam_dxids = self.find_bam_files()
        if not self.interop_dxid:
            self.interop_dxid = self.find_interop_file()
        self.samples_dicts = self.set_sample_files()

        # Add in checks to make sure all files accounted for
        if not self.samples_dicts:
            print('Error: sample dictionaries containing bam and fastq files' +
                    'were not generated')
            sys.exit()  # DEV: use more specific errors (?) if possible
        elif not self.interop_dxid:
            print('Error: InterOp.tar file has not been located')
            sys.exit()

    def find_fastq_files(self):
        '''
        Description: Returns a dict of all fastq files in the lane project;
        key = fastq filename, 
        value = fastq dxid

        DEV: Instead of returning a generator, I think this should return dxids
        for each fastq file. Same for interop, and bam files.
        '''
        fastq_dxids = []
        fastq_files_generator = dxpy.find_data_objects(classname='file', 
            name='*.fastq.gz', name_mode='glob', project=self.lane_project_dxid, 
            folder='/')
        for fastq_dict in self.fastq_files_generator: 
            fastq_dxid = fastq_dict['id']
            fastq_dxids.append(fastq_dxid)
        return fastq_dxids 

    def find_interop_file(self):

        interop_name = '%s.InterOp.tar.gz' % self.run_name
        interop_file = dxpy.find_one_data_object(classname = 'file',
                                                 name = interop_name, 
                                                 name_mode = 'exact', 
                                                 project = self.lane_project_dxid,
                                                 folder = '/', 
                                                 zero_ok = False, 
                                                 more_ok = True
                                                )
        return interop_file['id']

    def find_bam_files(self):
        ''' DEV: add functionality to also find BAI files
        '''

        bam_dxids = []
        bam_files_generator = dxpy.find_data_objects(classname='file',
            name='*.bam', name_mode='glob', project=self.lane_project_dxid,
            folder='/')
        for bam_dict in self.bam_files_generator:
            bam_dxid = bam_dict['id']
            bam_dxids.append(bam_dxid)
        return bam_dxids
        
    def set_sample_files(self):
        '''
        Description: Returns a dict of sample fastq files; 
        key = barcode/index, 
        value = dict of fastq dxids;
            key = read index ['1'/'2'],
            value = fastq dxid
        ''' 

        self.samples_dicts = {}
        for fastq_dxid in self.fastq_dxids:    
            fastq_file = dxpy.DXFile(fastq_dxid)
            fastq_name = fastq_file.describe()['name']
            elements = fastq_name.split('_')
            barcode = elements[5]
            read = elements[6]
            if barcode in self.samples_dicts.keys():
                self.samples_dicts[barcode][int(read)] = fastq_dxid
            else:
                self.samples_dicts[barcode] = {int(read) : fastq_dxid}
        
        for bam_dxid in self.bam_dxids:
            bam_file = dxpy.DXFile(bam_dxid)
            bam_name = bam_file.describe()['name']
            elements = bam_name.split('_')
            barcode = elements[5]
            if barcode in self.samples_dicts.keys():
                self.samples_dicts[barcode]['bam'] = bam_dxid
            else:
                print 'Error: Unmatched bam file with barcode: %s' % barcode
                sys.exit()
        return(self.samples_dicts)

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

@dxpy.entry_point("run_qc_sample")
def qc_sample(fastq_files, sample_name, properties=None, aligner=None, 
    genome_fasta_file = None, fastq_files2=None, bam_file=None):

    # DEV: change to be dynamically gotten
    # find mapping app
    qc_sample_applet_name = 'qc_sample' 
    qc_sample_applet_dxid = dxpy.find_one_data_object(classname='applet',
        name=qc_sample_applet_name, name_mode='exact', project='project-BpP8Yg00zz07ffjx0Ggjk71f',
        folder='/builds/2016-01-20_fc6411e', zero_ok=False, more_ok=False)
    qc_sample_applet = dxpy.DXApplet(qc_sample_applet_dxid['id'])

    fastq_files = [dxpy.dxlink(x) for x in fastq_files]
    fastq_files2 = [dxpy.dxlink(x) for x in fastq_files2]
    qc_input = {'fastq_files': fastq_files, 'sample_name': sample_name}
                
    if properties:
        qc_input['properties'] = properties
    if aligner:
        qc_input['aligner'] = aligner
        qc_input['genome_fasta_file'] = dxpy.dxlink(genome_fasta_file)
        qc_input['fastq_files2'] = fastq_files2
        qc_input['bam_file'] = dxpy.dxlink(bam_file)
    qc_job = qc_sample_applet.run(qc_input)

    output = {'alignment_summary_metrics': {'job': qc_job.get_id(), 'field': 'alignment_summary_metrics'},
            'fastqc_reports': {'job': qc_job.get_id(), 'field': 'fastqc_reports'},
            'insert_size_metrics': {'job': qc_job.get_id(), 'field': 'insert_size_metrics'},
            'mismatch_metrics': {'job': qc_job.get_id(), 'field': 'mismatch_metrics'},
            'qc_json_file': {'job': qc_job.get_id(), 'field': 'json_output_file'},
            'tools_used': {'job': qc_job.get_id(), 'field': 'tools_used'}}
    return output

@dxpy.entry_point("generate_qc_pdf_report")
def generate_qc_pdf_report(**job_inputs):
    """
    Run create_pdf_reports.py to create a PDF report from the JSON statistics.
    """
    # Download and prepare necessary files
    print("process_lane.generate_qc_pdf_report job inputs: ")
    print(job_inputs)
    if 'mismatch_metrics' in job_inputs:
        mismatch_stats_files = [download_file(mismatch_file) for mismatch_file in job_inputs['mismatch_metrics']]
    interop_file = download_file(job_inputs['interop_file'])

    tools_used_fn = create_tools_used_file(job_inputs['tools_used'])

    # Merge the individual sample stats into one file.
    samples_stats_json_files = [download_file(samples_stats_file) for samples_stats_file in job_inputs['samples_stats_json_files']]
    samples_stats = [json.loads(open(input_json_file).read()) for input_json_file in samples_stats_json_files]
    with open(SAMPLE_STATS_JSON_FN, 'w') as fh:
        fh.write(json.dumps(samples_stats))

    with open(RUN_DETAILS_JSON_FN, 'w') as fh:
        fh.write(json.dumps(job_inputs['run_details']))

    with open(BARCODES_JSON_FN, 'w') as fh:
        fh.write(json.dumps(job_inputs['barcodes']))

    # Now actually make the call to create the pdf.
    ofn = job_inputs['run_details']['run_name'].replace(' ', '_') + '_qc_report.pdf'
    cmd = 'python /create_pdf_reports.py '
    if 'mismatch_metrics' in job_inputs:
        cmd += '--mismatch_files {0} '.format(' '.join(mismatch_stats_files))
    else:
        cmd += '--basic '
    cmd += '--interop {0} '.format(interop_file)
    cmd += '--run_details {0} '.format(RUN_DETAILS_JSON_FN)
    cmd += '--sample_stats {0} '.format(SAMPLE_STATS_JSON_FN)
    cmd += '--barcodes {0} '.format(BARCODES_JSON_FN)
    cmd += '--tools_used {0} '.format(tools_used_fn)
    if job_inputs['paired_end']:
        cmd += '--paired_end '
    if job_inputs['mark_duplicates']:
        cmd += '--collect_duplicate_info '
    cmd += '--out_file {0} '.format(ofn)

    print cmd
    subprocess.check_call(cmd, shell=True)

    qc_pdf_report = dxpy.upload_local_file(ofn)
    run_details_json_fid = dxpy.upload_local_file(RUN_DETAILS_JSON_FN)
    barcodes_json_fid = dxpy.upload_local_file(BARCODES_JSON_FN)
    sample_stats_json_fid = dxpy.upload_local_file(SAMPLE_STATS_JSON_FN)
    #dashboard_record_object = app_utils.get_dashboard_record_object(job_inputs['dashboard_record_id'])
    #props = dashboard_record_object.get_properties()
    #props[conf.qcReportIdAttrName] = dxpy.PROJECT_CONTEXT_ID + ':' + qc_pdf_report.get_id()
    #props[conf.pipelineStageAttrName] = conf.pipelineStageAttrValues["complete"]
    #dashboard_record_object.set_properties(props)

    return {'qc_pdf_report': dxpy.dxlink(qc_pdf_report), 
            'run_details_json': dxpy.dxlink(run_details_json_fid), 
            'barcodes_json': dxpy.dxlink(barcodes_json_fid), 
            'sample_stats_json': dxpy.dxlink(sample_stats_json_fid)
            }

@dxpy.entry_point("main")
def main(record_id, fastqs=None, bams=None, bais=None, dashboard_project_id=None):

    lane = FlowcellLane(record_dxid=record_id, fastqs=fastqs, bams=bams, 
                        bais=bais, dashboard_project_dxid=dashboard_project_id)

    output = {"alignment_summary_metrics": [], 
                "fastqc_reports": [], 
                "insert_size_metrics": [],
                "mismatch_metrics": [],
                "qc_json_files": [],
                "tools_used": []}

    # DEV: In 'test' mode, I want to skip this section and use supplied
    #       inputs. Change this to be a function in 'Sample' object (?)
    for barcode in lane.samples_dicts:
        fastq_files = [lane.samples_dicts[barcode][1]]
        fastq_files2 = [lane.samples_dicts[barcode][2]]
        qc_job = dxpy.new_dxjob(fn_input={
            "fastq_files": fastq_files,
            "fastq_files2": fastq_files2, 
            "sample_name": barcode, 
            "properties": None,
            "aligner": lane.aligner, 
            "genome_fasta_file": lane.ref_genome_dxid,
            "bam_file": lane.samples_dicts[barcode]['bam']
            }, fn_name="run_qc_sample")
        #qc_jobs.append(qc_job)     # Artifact of previous approach (?)
        output["alignment_summary_metrics"].append({"job": qc_job.get_id(), "field": "alignment_summary_metrics"})
        output["fastqc_reports"].append({"job": qc_job.get_id(), "field": "fastqc_reports"})
        output["insert_size_metrics"].append({"job": qc_job.get_id(), "field": "insert_size_metrics"})
        output["mismatch_metrics"].append({"job": qc_job.get_id(), "field": "mismatch_metrics"})
        output["qc_json_files"].append({"job": qc_job.get_id(), "field": "qc_json_file"})
        output["tools_used"].append({"job": qc_job.get_id(), "field": "tools_used"})


    # Now handle the generation of the QC PDF report.
    run_details = {'run_name': lane.run_name,
                'flow_cell': lane.flowcell_id, #should be the same for all fq files
                'lane': lane.lane_index,
                'library': lane.library_name,
                'operator': lane.operator,
                'genome_name': lane.reference_genome,
                'lab_name': lane.lab_name,
                'mapper': lane.aligner}     # DEV: change this to be 'aligner' in 'create_pdf_reports.py' for consistency

    # test data
    #output = {'alignment_summary_metrics': [{u'$dnanexus_link': u'file-BpPvQK00Bz3x94FXjQ5bq2G5'}],
    #            'fastqc_reports': [{u'$dnanexus_link': u'file-BpPvQ400Bz3z1qqFxvYjKpj2'}, 
    #                                {u'$dnanexus_link': u'file-BpPvQ680Bz3qjK1jbxK1vP7v'}],
    #            'insert_size_metrics': [{u'$dnanexus_link': u'file-BpPvQ100Bz3kQVyQKQy2Z9jV'}],
    #            'mismatch_metrics': [{u'$dnanexus_link': u'file-BpPvPPj0Bz3qXffFGv8f9xBX'}],
    #            'qc_json_files': [{u'$dnanexus_link': u'file-BpPvX900Bz3Y43B48Zy10vb8'}],
    #            'tools_used': [{u'$dnanexus_link': u'file-BpPvXfj0Bz3x94FXjQ5bq3gg'}]
    #            }
    qc_pdf_report_input = {'samples_stats_json_files': output['qc_json_files'],
                            'dashboard_record_id': lane.dashboard_record_dxid,
                            'run_details': run_details,
                            'barcodes': lane.samples_dicts.keys(),
                            'mark_duplicates': False,
                            'interop_file': lane.interop_dxid,
                            'tools_used': output['tools_used'],
                            'paired_end': True}
    if lane.reference_genome and lane.aligner:
        qc_pdf_report_input['mismatch_metrics'] = output['mismatch_metrics']

    # Spawn QC PDF Report job
    qc_pdf_report_job = dxpy.new_dxjob(fn_input=qc_pdf_report_input, fn_name="generate_qc_pdf_report")

    output["qc_pdf_report"] = qc_pdf_report_job.get_output_ref("qc_pdf_report")
    output["run_details_json"] = qc_pdf_report_job.get_output_ref("run_details_json")
    output["barcodes_json"] = qc_pdf_report_job.get_output_ref("barcodes_json")
    output["sample_stats_json"] = qc_pdf_report_job.get_output_ref("sample_stats_json")    

    return output

dxpy.run()










