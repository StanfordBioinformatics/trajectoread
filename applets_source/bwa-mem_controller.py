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
        self.dashboard_record = dxpy.DXRecord(dxid = self.dashboard_record_dxid,project = self.dashboard_project_dxid)
        self.properties = self.dashboard_record.get_properties()

        # Get fastq files information and dx references
        self.fastq_files = _find_fastq_files()
        self.samples = _get_sample_files()

        # Get reference genome information and dx references
        self.ref_genome_dxid = self.properties['reference_genome_dxid']

    def _get_fastq_files():
        '''
        Description: Returns a dict of all fastq files in the lane project; key = fastq filename, value = fastq dxid
        '''

    def _get_sample_files():
        '''
        Description: Returns a dict of sample fastq files; key = barcode/index, value = list of fastq dxids
        ''' 

class MapperApp:

    def __init__(self, name='bwa_mem_fastq_read_mapper', version='1.5.0'):
        # Currently doesn't look like search function allows option to search for particular version
        # Only version option is 'all_versions' boolean which indicates whether to get default or all
        
        self.name = name
        self.version = version
        self.app_dxid = None

        # Get mapper app dxid
        app_generator = dxpy.find_apps(name=name, all_versions=True)
        for app in app_generator:
            app_description = dxpy.api.describe(app['id'])
            app_version = app_description['version']
            if app_version == self.version:
                self.app_dxid = app['id']
                break
        app_object = dxpy.DXApp(dxid=self.app_dxid)
        return app_object

    def describe(self):
        print 'DNAnexus app name: %s, version: %s, dxid: %s' % (self.name, self.version, self.app_dxid)

@dxpy.entry_point("map_sample")
def map_sample(sample, fastq_list, mapper, ref_genome):
    '''
    Description: Maps sample fastq files to a reference genome
    Args:
        sample (dict) - sample[<barcode>] = [<fastq files>]
        mapper (dxid) 
        ref_genome (dxid)
    '''
    
    # Create dict to store mapper app inputs
    mapper_input = {'-igenomeindex_targz' : dxpy.dxlink(ref_genome),}

    # Add fastq files to mapper app input dict
    if len(fastq_list) == 0:
        print 'Error: No fastq files listed for sample %s' % sample
        sys.exit()
    elif len(fastq_list) == 1:
        mapper_input['-ireads_fastqgz'] = dxpy.dxlink(fastq_list[0])
    elif len(fastq_list) == 2:
        mapper_input['-ireads_fastqgz'] = dxpy.dxlink(fastq_list[0])
        mapper_input['-ireads2_fastqgz'] = dxpy.dxlink(fastq_list[1])
    else:
        print 'Error: More than 2 fastq files passed for mapping sample %s' % sample
        sys.exit()

    mapper.run(mapper_input)

@dxpy.entry_point("main")
def main(record_id):


    lane = FlowcellLane(dashboard_record_dxid=record_id)

    # find mapping app
    mapper_app_name = 'bwa_mem_fastq_read_mapper'
    mapper_app_version = '1.5.0'
    mapper_dxid = MapperApp(name=mapper_app_title, version=mapper_app_version)


    # DNAnexus reference genome files project dxid: project-BQpp3Y804Y0xbyG4GJPQ01xv

    for barcode in lane.samples:
        dxpy.new_dxjob(fn_input={
            "sample": barcode,
            "fastqs": lane.samples[barcode] 
            "mapper": mapper_dxid, 
            "ref_genome": lane.ref_genome_dxid 
            }, fn_name="map_sample")



