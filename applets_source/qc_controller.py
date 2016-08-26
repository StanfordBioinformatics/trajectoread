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

    def __init__(self, record_id):

        self.record_id = record_id.strip()
        record_project = self.record_id.split(':')[0]
        record_dxid = self.record_id.split(':')[1]
        self.record = dxpy.DXRecord(dxid=record_dxid, project=record_project)

        # Get relevant dashboard details
        self.details = self.record.get_details()
        self.run_name = self.details['run']
        self.lane_index = self.details['lane']
        self.library_name = self.details['library']
        self.project_id = self.details['laneProject']

        # Get relevant dashboard properties
        self.properties = self.record.get_properties()
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

    def find_fastq_files(self):
        '''
        DEPRECATED 7/29/2016
        Description: Returns a dict of all fastq files in the lane project;
        key = fastq filename, 
        value = fastq dxid

        DEV: Instead of returning a generator, I think this should return dxids
        for each fastq file. Same for interop, and bam files.
        '''
        fastq_dxids = []
        fastq_files_generator = dxpy.find_data_objects(classname='file', 
            name='*.fastq.gz', name_mode='glob', project=self.project_id, 
            folder='/')
        for fastq_dict in self.fastq_files_generator: 
            fastq_dxid = fastq_dict['id']
            fastq_dxids.append(fastq_dxid)
        return fastq_dxids 

    def find_interop_file(self):
        ''' DERECATED '''
        interop_name = '%s.InterOp.tar.gz' % self.run_name
        interop_file = dxpy.find_one_data_object(classname = 'file',
                                                 name = interop_name, 
                                                 name_mode = 'exact', 
                                                 project = self.project_id,
                                                 folder = '/', 
                                                 zero_ok = False, 
                                                 more_ok = True
                                                )
        return interop_file['id']

    def find_bam_files(self):
        ''' DEV: DEPRECATED
                 add functionality to also find BAI files
        '''

        bam_dxids = []
        bam_files_generator = dxpy.find_data_objects(classname='file',
            name='*.bam', name_mode='glob', project=self.project_id,
            folder='/')
        bam_files = list(bam_files_generator)
        
        if len(bam_files) < 1:
            print 'Info: No bam files found.'
            pass
        else:
            for bam_dict in bam_files:
                bam_dxid = bam_dict['id']
                bam_dxids.append(bam_dxid)
            return bam_dxids
        
    def set_sample_files(self):
        '''
        DEPRECATED
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
        
        if self.bam_dxids != None:
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
            dxpy.api.record_set_properties(object_id = self.dashboard_record_id, 
                                           input_params = {
                                                           'project': self.dashboard_project_id,
                                                           'properties': properties
                                                          }
                                          )

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

def group_files_by_read(fastq_files):
    """
    Function : Groups a list of FASTQ files by the values of their Read property that indicates the read number.
                       Returns a dict mapping each observed value of the property (or 'none' if a file does not have a value
                         for the property) to a list of the files with that value. Within each group, the files are sorted by their
                       value of the Chunk property (to ensure that left and right reads of a given chunk are handled together.
    Args     : fastq_files - a list of dxpy.DXFile objects representing FASTQ files.
    Returns  : dict.
    """

    #print("Grouping Fastq files by read number")
    fastq_dxfiles = [dxpy.DXFile(item) for item in fastq_files]
    read_dict = {}

    for fastq_dxfile in fastq_dxfiles:
        props = fastq_dxfile.get_properties()
        read_num = props["read"]
        if read_num not in ["1", "2", "none"]:
            raise dxpy.AppError("%s has invalid Read property: %s" % (fastq_dxfile.get_id(), read_num))
        if read_num not in read_dict:
            read_dict[read_num] = []
        fastq_dxlink = dxpy.dxlink(fastq_dxfile)
        read_dict[read_num].append(fastq_dxlink)

    #for read_num in read_dict:
    #    read_dict[read_num] = sorted(read_dict[read_num], key=chunk_property)

    return read_dict

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

@dxpy.entry_point("run_qc_sample")
def qc_sample(fastq_files, sample_name, applet_id, applet_project, output_project, output_folder, properties=None, 
    aligner=None, genome_fasta_file = None, fastq_files2=None, bam_file=None):

    '''
    qc_sample_applet_name = 'qc_sample' 
    qc_sample_applet_dxid = dxpy.find_one_data_object(classname = 'applet',
                                                      name = qc_sample_applet_name, 
                                                      name_mode = 'exact', 
                                                      project = applet_project,
                                                      folder = applet_folder, 
                                                      zero_ok = False, 
                                                      more_ok = False)
    qc_sample_applet = dxpy.DXApplet(qc_sample_applet_dxid['id'])
    '''

    qc_sample_applet = dxpy.DXApplet(dxid=applet_id, project=applet_project)

    fastq_files = [dxpy.dxlink(x) for x in fastq_files]
    if fastq_files2:
        fastq_files2 = [dxpy.dxlink(x) for x in fastq_files2]

    qc_input = {
                'fastq_files': fastq_files, 
                'sample_name': sample_name,
                'output_project': output_project,
                'output_folder': output_folder
               }
    if fastq_files2:  
        qc_input['fastq_files2'] = fastq_files2           
    if properties:
        qc_input['properties'] = properties
    if aligner:
        qc_input['aligner'] = aligner
        qc_input['genome_fasta_file'] = dxpy.dxlink(genome_fasta_file)
        qc_input['bam_file'] = bam_file
    print 'QC Input:'
    print qc_input
    qc_job = qc_sample_applet.run(qc_input)

    output = {}
    output["fastqc_reports"] = ({"job": qc_job.get_id(), "field": "fastqc_reports"})
    output["qc_json_file"] = ({"job": qc_job.get_id(), "field": "json_output_file"})
    output["tools_used"] = ({"job": qc_job.get_id(), "field": "tools_used"})

    if bam_file != None:
        if genome_fasta_file != None:
            output["alignment_summary_metrics"] = ({"job": qc_job.get_id(), "field": "alignment_summary_metrics"})
        if aligner != None:
            output["mismatch_metrics"] = ({"job": qc_job.get_id(), "field": "mismatch_metrics"})
        if fastq_files2 != None:
            output["insert_size_metrics"] = ({"job": qc_job.get_id(), "field": "insert_size_metrics"})
    
    return output

@dxpy.entry_point("main")
def main(record_id, worker_id, worker_project, output_folder, fastqs, bams=None):

    lane = FlowcellLane(record_id=record_id)

    output = {
              "alignment_summary_metrics": [], 
              "fastqc_reports": [], 
              "insert_size_metrics": [],
              "mismatch_metrics": [],
              "qc_stats_jsons": [],
              "tools_used": []
             }

    print "Grouping fastq files by barcode"
    fastq_dict = group_files_by_barcode(fastqs)
    if bams != None:
        print "Grouping bam files by barcode"
        bam_dict = group_files_by_barcode(bams)
    
    jobs = []
    for barcode in fastq_dict:
        print 'Processing sample: %s' % barcode
        read_dict = group_files_by_read(fastq_dict[barcode])
        
        fastq_files2 = None

        if "1" in read_dict and "2" in read_dict:
            # Paired-end sample
            fastq_files = read_dict['1']
            fastq_files2 = read_dict['2']
        else:
            fastq_files = read_dict['1']

        print("fastq_files: {}".format(fastq_files))
        print("fastq_files2: {}".format(fastq_files2))
        
        qc_job_input = {'fastq_files': fastq_files,
                        'sample_name': barcode,
                        'applet_id': worker_id,
                        'applet_project': worker_project,
                        'output_folder': output_folder,
                        'output_project': lane.project_id,
                        'properties': None
                        }
        # Optional qc job inputs
        if fastq_files2:
            qc_job_input['fastq_files2'] = fastq_files2
        if lane.mapper != None:
            qc_job_input['aligner'] = lane.mapper
            qc_job_input['genome_fasta_file'] = lane.ref_genome_dxid
            
            if len(bam_dict[barcode]) != 1:
                print 'Error: Should be one file in bam_dict[%s], found: %d' % (barcode, len(bam_dict[barcode]))
                print bam_dict[barcode]
                sys.exit()
            else:
                qc_job_input['bam_file'] = bam_dict[barcode][0] # sample dict is only fastq files
        qc_job = dxpy.new_dxjob(fn_input=qc_job_input, fn_name="run_qc_sample")
   
        output["fastqc_reports"].append({
                                         "job": qc_job.get_id(), 
                                         "field": "fastqc_reports"
                                        })
        output["qc_stats_jsons"].append({
                                         "job": qc_job.get_id(), 
                                         "field": "qc_json_file"
                                        })
        output["tools_used"].append({
                                     "job": qc_job.get_id(), 
                                     "field": "tools_used"
                                    })

        if bams != None:
            if lane.reference_genome != None:
                output["alignment_summary_metrics"].append({"job": qc_job.get_id(), "field": "alignment_summary_metrics"})
            if lane.mapper != None:
                output["mismatch_metrics"].append({"job": qc_job.get_id(), "field": "mismatch_metrics"})
            if fastq_files2 != None:
                try:
                    output["insert_size_metrics"].append({"job": qc_job.get_id(), "field": "insert_size_metrics"})
                except:
                    error = 'Error: Could not get insert size metrics for %s. ' % barcode
                    error += 'Check distribution of FR vs RF orientation reads in sample. '
                    error += 'May need to increase MINIMUM_PCT option.'
                    print error             
    return output


dxpy.run()










