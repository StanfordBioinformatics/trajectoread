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

    def __init__(self, dashboard_record_dxid, dashboard_project_dxid='project-BY82j6Q0jJxgg986V16FQzjx'):

        self.dashboard_record_dxid = dashboard_record_dxid
        self.dashboard_project_dxid = dashboard_project_dxid
        self.dashboard_record = dxpy.DXRecord(dxid = self.dashboard_record_dxid, 
                                                project = self.dashboard_project_dxid)
        self.properties = self.dashboard_record.get_properties()

        # Get reference genome information and dx references
        self.ref_genome_dxid = self.properties['reference_genome_dxid']
        # DEV: change this name
        self.lane_project_dxid = self.properties['lane_project_dxid']
        self.fastq_files_generator = None
        self.samples_dicts = None

        # Get fastq files information and dx references
        #self.fastq_files_dicts = self.find_fastq_files()
        #self.samples_dicts = self.set_sample_files()
        self.find_fastq_files()
        self.set_sample_files()

    def find_fastq_files(self):
        '''
        Description: Returns a dict of all fastq files in the lane project;
        key = fastq filename, 
        value = fastq dxid
        '''
        # Find all fastq.gz files in lane project
        #arguments = {'classname':'file', 'name':'*fastq.gz', 'name_mode':'glob',
        #                'project':self.lane_project_dxid, 'folder':'/'}
        self.fastq_files_generator = dxpy.find_data_objects(classname='file', 
            name='*.fastq.gz', name_mode='glob', project=self.lane_project_dxid, 
            folder='/')
        # {u'project': u'project-id', u'id': u'file-id'}
        return(self.fastq_files_generator)

    def set_sample_files(self):
        '''
        Description: Returns a dict of sample fastq files; 
        key = barcode/index, 
        value = dict of fastq dxids;
            key = read index ['1'/'2'],
            value = fastq dxid
        ''' 

        self.samples_dicts = {}
        for fastq_dict in self.fastq_files_generator:    
            # fastq_dict = {u'project': u'project-id', u'id': u'file-id'}
            fastq_file = dxpy.DXFile(fastq_dict['id'])
            fastq_name = fastq_file.describe()['name']
            elements = fastq_name.split('_')
            barcode = elements[5]
            read = elements[6]
            if barcode in self.samples_dicts.keys():
                self.samples_dicts[barcode][read] = fastq_dict['id']
            else:
                self.samples_dicts[barcode] = {read : fastq_dict['id']}
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
        app_generator = dxpy.find_apps(name=name, all_versions=True)
        if not list(app_generator):
            # raise dxpy.AppError('Unable to find app called %s' % name)
            print 'Error: Could not find any app with name: %s' % name
            sys.exit()
        else:
            for app in app_generator:
                app_description = dxpy.api.app_describe(app['id'])
                app_version = app_description['version']
                if app_version == self.version:
                    self.dxid = app['id']
                    break
        #self.object = dxpy.DXApp(dxid=self.dxid)     # bwa_mem : app-BXQy79Q0y7yQJVff3j9Y2B83
        self.object = dxpy.find_one_data_object(name=self.name, classname='applet', return_handler=True, zero_ok=False, project='project-B406G0x2fz2B3GVk65200003')

    def describe(self):
        print 'DNAnexus app name: %s, version: %s, dxid: %s' % (self.name, self.version, self.dxid)

@dxpy.entry_point("map_sample")
def map_sample(sample, fastq_dict, mapper_dxid, ref_genome, project_id):
    '''
    Description: Maps sample fastq files to a reference genome
    Args:
        sample (dict) - sample[<barcode>] = [<fastq files>]
        mapper (dxid) 
        ref_genome (dxid)
    '''
    
    dxpy.set_workspace_id(project_id)
    # Create dict to store mapper app inputs
    mapper_input = {'genomeindex_targz' : dxpy.dxlink(ref_genome)}    # hg19 : file-B6qq53v2J35Qyg04XxG0000V

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
    #pdb.set_trace()
    #sys.exit()
    mapper = dxpy.DXApplet(mapper_dxid)
    mapper.run(mapper_input)    # mapper : DXApp object

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
def main(record_id, dashboard_project_id=None):

    #dxpy.new_dxjob(fn_input={}, fn_name="test_mapping")
    #sys.exit()

    #dxpy.set_workspace_id('project-BpBjyqQ0Jk0Xv2B11Q8P6X59')
    #applet = dxpy.find_one_data_object(name='bwa_mem_fastq_read_mapper', classname='applet', return_handler=True, zero_ok=False, project='project-B406G0x2fz2B3GVk65200003')
    #applet.run({
    #    'genomeindex_targz': dxpy.dxlink('file-B6qq53v2J35Qyg04XxG0000V'),
    #    'reads_fastqgz': dxpy.dxlink('file-BpBjzFQ0Jk0Xk73YqQgJKg9Z'),
    #    'reads2_fastqgz': dxpy.dxlink('file-BpBk0400Jk0Xk73YqQgJKg9f')
    #    })

    #sys.exit()

    if not dashboard_project_id:
        lane = FlowcellLane(dashboard_record_dxid=record_id)
    else:
        lane = FlowcellLane(dashboard_record_dxid=record_id,
                            dashboard_project_dxid=dashboard_project_id)


    # Change workspace to lane project
    dxpy.set_workspace_id(lane.lane_project_dxid)
    
    # find mapping app
    mapper_app_name = 'bwa_mem_fastq_read_mapper'
    mapper_app_version = '1.5.0'
    mapper = MapperApp(name=mapper_app_name, version=mapper_app_version)   # DXApp object

    # DNAnexus reference genome files project dxid: project-BQpp3Y804Y0xbyG4GJPQ01xv
    # For each sample, start a child process to call bwa-mem and perform mapping
    # UPDATE: Don't need to create child process (I think)- app does that on its own
    for barcode in lane.samples_dicts:
        dxpy.new_dxjob(fn_input={
            "sample": barcode,
            "fastq_dict": lane.samples_dicts[barcode], 
            "mapper": mapper.dxid, 
            "ref_genome": lane.ref_genome_dxid,
            "project_id": lane.lane_project_dxid
            }, fn_name="map_sample")

        #mapper_input = {'genomeindex_targz' : dxpy.dxlink(lane.ref_genome_dxid)}    # hg19 : file-B6qq53v2J35Qyg04XxG0000V
        #fastq_dict = lane.samples_dicts[barcode]

        # Add fastq files to mapper app input dict
        #if len(fastq_dict) == 0:
        #    print 'Error: No fastq files listed for sample %s' % sample
        #    sys.exit()
        #elif len(fastq_dict) == 1:
        #    mapper_input['reads_fastqgz'] = dxpy.dxlink(fastq_dict['1'])
        #elif len(fastq_dict) == 2:
        #    mapper_input['reads_fastqgz'] = dxpy.dxlink(fastq_dict['1'])
        #    mapper_input['reads2_fastqgz'] = dxpy.dxlink(fastq_dict['2'])
        #else:
        #    print 'Error: More than 2 fastq files passed for mapping sample %s' % sample
        #    sys.exit()

        #mapper.object.run(mapper_input)    # mapper : DXApp object


