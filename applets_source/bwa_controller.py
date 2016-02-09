#!/usr/bin/python
''' 
Description : This applet use bwa-mem to map fastq reads to a reference genome.
    The applet identifies all the fastq files in a sequencing lane project, and
    groups by them by sample/barcode. It then finds the appropriate reference genome
    object and mapping app (currentlly the published 'BWA-MEM FASTQ Read Mapper').
    The one or two (paired-end) fastq files as well as the reference genome and
    mapper app are passed as arguments to a method that spawns a separate child 
    process for each mapping instance. Child processes are spawned using a DX entry
    point (@dxpy.entry_point("bwa-mem_app")). In this way, mapping for each sample
    is performed in a separate child process. 

Args : DNAnexus ID of dashboard record for sequencing lane project
Returns : 
Author : pbilling
'''

import sys
import dxpy

class FlowcellLane:

    def __init__(self, record_dxid, fastqs=None, dashboard_project_dxid=None):

        self.dashboard_record_dxid = record_dxid
        self.dashboard_project_dxid = dashboard_project_dxid
        if not self.dashboard_project_dxid:
            self.dashboard_project_dxid = 'project-BY82j6Q0jJxgg986V16FQzjx'

        # Get reference genome information and dx references
        self.dashboard_record = dxpy.DXRecord(dxid = self.dashboard_record_dxid, 
                                              project = self.dashboard_project_dxid)
        self.properties = self.dashboard_record.get_properties()
        self.ref_genome_index_dxid = self.properties['reference_index_dxid']
        self.lane_project_dxid = self.properties['lane_project_dxid']
        self.mapper = self.properties['mapper']
        self.reference_genome_dxid = self.properties['reference_genome_dxid']
        self.reference_index_dxid = self.properties['reference_index_dxid']

        self.fastq_dxids = fastqs
        self.samples_dicts = None

        # Get fastq files information and dx references
        if not self.fastq_dxids:
            self.fastq_dxids = self.find_fastq_files()
        self.samples_dicts = self.set_sample_files()

        if not self.samples_dicts:
            print('Error: sample dictionaries containing bam and fastq files' +
                    'were not generated')
            sys.exit()  # DEV: use more specific errors (?) if possible

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
        
        return(self.samples_dicts)

class MapperApp:

    def __init__(self, name='bwa_mem_fastq_read_mapper', version='1.5.0'):
        # Currently doesn't look like search function allows option to search for particular version
        # Only version option is 'all_versions' boolean which indicates whether to get default or all
        
        self.name = name
        self.version = version
        self.dxid = None
        self.object = None

        # Get mapper app dxid
        app_generator = dxpy.find_apps(name=name, all_versions=False)   # all_versions will not get most recent
        if not list(app_generator):
            # raise dxpy.AppError('Unable to find app called %s' % name)
            print 'Error: Could not find any app with name: %s' % name
            sys.exit()
        else:
            app_generator = dxpy.find_apps(name=name, all_versions=False)
            for app in app_generator:
                app_description = dxpy.api.app_describe(app['id'])
                app_version = app_description['version']
                if app_version == self.version:
                    self.dxid = app['id']
                    break
                else:
                    print app_version
        if not self.dxid:
            print 'Could not find app: %s, version: %s' % (self.name, self.version)
            sys.exit()
        self.object = dxpy.DXApp(dxid=self.dxid)     # bwa_mem : app-BXQy79Q0y7yQJVff3j9Y2B83
        #self.object = dxpy.find_one_data_object(name=self.name, classname='applet', return_handler=True, zero_ok=False, project='project-B406G0x2fz2B3GVk65200003')
        #self.dxid = self.object.get_id()

    def describe(self):
        print 'DNAnexus app name: %s, version: %s, dxid: %s' % (self.name, self.version, self.dxid)

@dxpy.entry_point("run_map_sample")
def run_map_sample(fastq_files, genome_fasta_file, genome_index_file, mapper, 
                   fastq_files2=None, mark_duplicates=False, sample_name=None, 
                   properties=None):
    
    mapper_applet_name = 'map_sample'
    applet_folder = '/builds/%s' % applet_build_version
    mapper_applet = dxpy.find_one_data_object(classname='applet',
                                              name=mapper_applet_name,
                                              name_mode='exact',
                                              project=applet_project,
                                              folder=applet_folder,
                                              zero_ok=False,
                                              more_ok=False)
    
    mapper_input = {
                    "fastq_files": fastq_files,
                    "fastq_files2": fastq_files2,
                    "genome_fasta_file": genome_fasta_file,
                    "genome_index_file": genome_index_file,
                    "mapper": mapper,
                    "sample_name": sample_name,
                    "mark_duplicates": mark_duplicates
                   }
    map_sample_job = mapper_applet.run(mapper_input)

@dxpy.entry_point("run_bwa_mem")
def run_bwa_mem(sample, fastq_dict, mapper_app_dxid, ref_genome_index, project_id):
    '''
    Description: Maps sample fastq files to a reference genome
    Args:
        sample (dict) - sample[<barcode>] = [<fastq files>]
        mapper (dxid) 
        ref_genome (dxid)
    '''
    
    dxpy.set_workspace_id(project_id)
    # Create dict to store mapper app inputs
    mapper_app = dxpy.DXApp(mapper_app_dxid)
    mapper_input = {'genomeindex_targz' : dxpy.dxlink(ref_genome_index)}    # hg19 : file-B6qq53v2J35Qyg04XxG0000V

    # Add fastq files to mapper app input dict
    if len(fastq_dict) == 0:
        print 'Error: No fastq files listed for sample %s' % sample
        sys.exit()
    elif len(fastq_dict) == 1:
        mapper_input['reads_fastqgz'] = dxpy.dxlink(fastq_dict['1'])
    elif len(fastq_dict) == 2:
        mapper_input['reads_fastqgz'] = dxpy.dxlink(fastq_dict['1'])
        mapper_input['reads2_fastqgz'] = dxpy.dxlink(fastq_dict['2'])
    else:
        print 'Error: More than 2 fastq files passed for mapping sample %s' % sample
        sys.exit()
    print mapper_input

    mapper_job = mapper_app.run(mapper_input)
    mapper_output = {
        "BAM": {"job": mapper_job.get_id(), "field": "sorted_bam"},
        "BAI": {"job": mapper_job.get_id(), "field": "sorted_bai"}
        }
    return mapper_output

@dxpy.entry_point("test_mapping")
def test_mapping():
    dxpy.set_workspace_id('project-BpBjyqQ0Jk0Xv2B11Q8P6X59')
    applet = dxpy.find_one_data_object(name='bwa_mem_fastq_read_mapper', classname='applet', return_handler=True, zero_ok=False, project='project-B406G0x2fz2B3GVk65200003')
    applet.run({
        'genomeindex_targz': dxpy.dxlink('file-B6qq53v2J35Qyg04XxG0000V'),
        'reads_fastqgz': dxpy.dxlink('file-BpBjzFQ0Jk0Xk73YqQgJKg9Z'),
        'reads2_fastqgz': dxpy.dxlink('file-BpBk0400Jk0Xk73YqQgJKg9f')
        })

@dxpy.entry_point("main")
def main(record_dxid, applet_project, applet_build_version, fastqs=None, dashboard_project_id=None, mark_duplicates=False):

    lane = FlowcellLane(record_dxid=record_dxid, fastqs=fastqs, 
                        dashboard_project_dxid=dashboard_project_id)

    # Change workspace to lane project 
    # DEV: Does not have an effect on job "Monitor"
    dxpy.set_workspace_id(lane.lane_project_dxid)
    
    ## Stock DNAnexus BWA-MEM app
    #mapper_app_name = 'bwa_mem_fastq_read_mapper'
    #mapper_app_version = '1.5.0'
    #mapper_app = MapperApp(name=mapper_app_name, version=mapper_app_version)   # DXApp object

    # SCGPM custom map_sample applet
    mapper_applet_name = 'map_sample'
    applet_folder = '/builds/%s' % applet_build_version
    mapper_applet = dxpy.find_one_data_object(classname='applet',
                                              name=mapper_applet_name,
                                              name_mode='exact',
                                              project=applet_project,
                                              folder=applet_folder,
                                              zero_ok=False,
                                              more_ok=False)

    # DNAnexus reference genome files project dxid: project-BQpp3Y804Y0xbyG4GJPQ01xv
    # For each sample, start a child process to call bwa-mem and perform mapping
    # UPDATE: Don't need to create child process (I think)- app does that on its own
    
    output = {"bams": [], "bais": []}

    for barcode in lane.samples_dicts:
        fastq_dict = lane.samples_dicts[barcode]
        # Add fastq files to mapper app input dict
        if len(fastq_dict) == 0:
            print 'Error: No fastq files listed for sample %s' % sample
            sys.exit()
        elif len(fastq_dict) == 1:
            fastq_files = [fastq_dict['1']]
            fastq_files2 = None
        elif len(fastq_dict) == 2:
            fastq_files = [fastq_dict['1']]
            fastq_files2 = [fastq_dict['2']]
        else:
            print 'Error: More than 2 fastq files passed for mapping sample %s' % sample
            sys.exit()

        map_sample_job = dxpy.new_dxjob(fn_input={
                                                  "fastq_files": fastq_files,
                                                  "fastq_files2": fastq_files2,
                                                  "genome_fasta_file": lane.reference_genome_dxid,
                                                  "genome_index_file": lane.reference_index_dxid,
                                                  "mapper": lane.mapper,
                                                  "sample_name": barcode,
                                                  "mark_duplicates": mark_duplicates
                                                 }, fn_name="run_map_sample") 
                                        )

    #for barcode in lane.samples_dicts:
    #    bwa_mapper_job = dxpy.new_dxjob(fn_input={
    #        "sample": barcode,
    #        "fastq_dict": lane.samples_dicts[barcode], 
    #        "mapper_app_dxid": mapper_app.dxid, 
    #        "ref_genome_index": lane.ref_genome_index_dxid,
    #        "project_id": lane.lane_project_dxid
    #        }, fn_name="bwa_mem")
    #    output["bams"].append({"job": bwa_mapper_job.get_id(), "field": "BAM"})
    #    output["bais"].append({"job": bwa_mapper_job.get_id(), "field": "BAI"})
    #return output

dxpy.run()
