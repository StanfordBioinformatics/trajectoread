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

import re
import sys
import dxpy

class FlowcellLane:

    def __init__(self, record_id, fastqs=None):

        self.record_id = record_id
        record_project = self.record_id.split(':')[0]
        record_dxid = self.record_id.split(':')[1]
        self.record = dxpy.DXRecord(dxid=record_dxid, project=record_project)
        
        # Get record details
        self.details = self.record.get_details()
        self.project_id = self.details['laneProject']
        self.mapping_reference = self.details['mappingReference']
        self.lane_index = int(self.details['lane'])
        self.run_name = self.details['run']
        self.run_date = self.run_name.split('_')[0]
        self.library_id = self.details['library_id']
        self.lane_id = self.details['lane_id']
        
        # Parse library name ("DL_set2_rep1 rcvd 1/4/16")
        library_label = self.details['library']
        elements = library_label.split('rcvd')
        library_name = elements[0].rstrip()
        self.library_name = re.sub(r"[^a-zA-Z0-9]+", "-", library_name)

        # Get record properties
        self.properties = self.record.get_properties()    
        self.mapper = self.properties['mapper']
        self.reference_genome_dxid = self.properties['reference_genome_dxid']
        self.reference_index_dxid = self.properties['reference_index_dxid']
        self.flowcell_id = self.properties['flowcell_id']

        self.fastq_dxids = fastqs

    def find_fastq_files(self):
        '''
        Description: Returns a dict of all fastq files in the lane project;
        key = fastq filename, 
        value = fastq dxid

        DEV: Instead of returning a generator, I think this should return dxids
        for each fastq file. Same for interop, and bam files.
        '''
        fastq_dxids = []
        fastq_files_generator = dxpy.find_data_objects(classname = 'file', 
                                                       name = '*.fastq.gz', 
                                                       name_mode = 'glob', 
                                                       project = self.project_id, 
                                                       folder = '/'
                                                      )
        for fastq_dict in self.fastq_files_generator: 
            fastq_dxid = fastq_dict['id']
            fastq_dxids.append(fastq_dxid)
        return fastq_dxids 

    def set_sample_files(self):
        '''
        Description: Returns a dict of sample fastq files; 
        key = barcode/index, 
        value = dict of fastq dxids;
            key = read index [1/2],
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

def group_files_by_barcode(fastq_files):
    """
    Group FASTQ files by sample according to their SampleID and Index
    properties. Returns a dict mapping (SampleID, Index) tuples to lists of
    files.
    Note - since I have casava outputting each barcode read in a single file, the value of each group should be a single file for single-end sequencing,
     or two files for PE sequencing.
    """
    
    print("Grouping Fastq files by barcode")
    sample_dict = {}

    for fastq_file in fastq_files:
        props = fastq_file.get_properties()
        barcode =  props["barcode"] #will be NoIndex if non-multiplex (see bcl2fatq UG sectino "FASTQ Files")
        if barcode not in sample_dict:
            sample_dict[barcode] = []
        sample_dict[barcode].append(fastq_file)
    print("Grouped FASTQ files as follows:")
    print(sample_dict)
    return sample_dict

def group_files_by_read(fastq_files):
    """
    Function : Groups a list of FASTQ files by the values of their Read property that indicates the read number.
                       Returns a dict mapping each observed value of the property (or 'none' if a file does not have a value
                         for the property) to a list of the files with that value. Within each group, the files are sorted by their
                       value of the Chunk property (to ensure that left and right reads of a given chunk are handled together.
    Args     : fastq_files - a list of dxpy.DXFile objects representing FASTQ files.
    Returns  : dict.
    """

    print("Grouping Fastq files by read number")
    read_dict = {}

    for fastq_file in fastq_files:
        props = fastq_file.get_properties()
        read_num = props["read"]
        if read_num not in ["1", "2", "none"]:
            raise dxpy.AppError("%s has invalid Read property: %s" % (fastq_file.get_id(), read_num))
        if read_num not in read_dict:
            read_dict[read_num] = []
        read_dict[read_num].append(fastq_file)

    #for read_num in read_dict:
    #    read_dict[read_num] = sorted(read_dict[read_num], key=chunk_property)

    return read_dict

class MapperApp:
    ''' DEV: Deprecated now that we use our own custom applet'''

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
def run_map_sample(project_id, output_folder, fastq_files, genome_fasta_file, 
    genome_index_file, mapper, applet_id, applet_project, 
    fastq_files2=None, mark_duplicates=False, sample_name=None, properties=None):
    
    applet_name = 'map_sample'
    applet_folder = '/builds/%s' % applet_build_version
    
    '''
    mapper_applet_id = dxpy.find_one_data_object(classname = 'applet',
                                                 name = applet_name,
                                                 name_mode = 'exact',
                                                 project = applet_project,
                                                 folder = applet_folder,
                                                 zero_ok = False,
                                                 more_ok = False
                                                )
    mapper_applet = dxpy.DXApplet(dxid = mapper_applet_id['id'], 
                                  project = mapper_applet_id['project']
                                 )
    '''

    mapper_applet = dxpy.DXApplet(dxid=applet_id, project=applet_project)

    print 'Running map_sample'
    mapper_input = {
                    "project_id": project_id,
                    "output_folder": output_folder,
                    "fastq_files": fastq_files,
                    "genome_fasta_file": dxpy.dxlink(genome_fasta_file),
                    "genome_index_file": dxpy.dxlink(genome_index_file),
                    "mapper": mapper,
                    "sample_name": sample_name,
                    "mark_duplicates": mark_duplicates,
                    "properties": properties
                   }
    if fastq_files2:
        mapper_input['fastq_files2'] = fastq_files2
    map_sample_job = mapper_applet.run(mapper_input)
    mapper_output = {
        "bam": {"job": map_sample_job.get_id(), "field": "bam"},
        "bai": {"job": map_sample_job.get_id(), "field": "bai"},
        "tools_used": {"job": map_sample_job.get_id(), "field": "tools_used"}
    }
    return mapper_output

@dxpy.entry_point("run_bwa_mem")
def run_bwa_mem(sample, fastq_dict, mapper_app_dxid, ref_genome_index, project_id):
    '''
    Description: Maps sample fastq files to a reference genome
    Args:
        sample (dict) - sample[<barcode>] = [<fastq files>]
        mapper (dxid) 
        ref_genome (dxid)
    '''
    
    ## Stock DNAnexus BWA-MEM app
    #mapper_app_name = 'bwa_mem_fastq_read_mapper'
    #mapper_app_version = '1.5.0'
    #mapper_app = MapperApp(name=mapper_app_name, version=mapper_app_version)   # DXApp object

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
def main(record_id, worker_id, worker_project, fastqs, output_folder, mark_duplicates=False):

    output = {
              "bams": [],
              "bais": [], 
              "tools_used": []
             }
             
    lane = FlowcellLane(record_id=record_id, fastqs=fastqs)
    
    fastq_files = [dxpy.DXFile(item) for item in fastqs]
    sample_dict = group_files_by_barcode(fastq_files)

    for barcode in sample_dict:
        print 'Processing sample: %s' % barcode
        read_dict = group_files_by_read(sample_dict[barcode])

        fastq_files2 = None

        if "1" in read_dict and "2" in read_dict:
            # Sample is paired; there should be no files without a 'read'
            # property of "1" or "2"
            fastq_files = [dxpy.dxlink(item) for item in read_dict["1"]]
            fastq_files2 = [dxpy.dxlink(item) for item in read_dict["2"]]
        else:
            fastq_files = [dxpy.dxlink(item) for item in fastq_files]

        print("fastq_files: {}".format(fastq_files))
        print("fastq_files2: {}".format(fastq_files2))

        mapped_files_properties = {
                                   'barcode': barcode, 
                                   'run_date': lane.run_date,
                                   'library_id': lane.library_id,
                                   'lane_id': lane.lane_id,
                                   'mapper': lane.mapper,
                                   'mapping_reference': lane.mapping_reference,
                                   'library_name': lane.library_name
                                  }
        print 'Initiating map sample job'
        sample_name = 'SCGPM_%s_%s_%s_L%d_%s' % (lane.run_date, 
                                             lane.library_name, 
                                             lane.flowcell_id,
                                             lane.lane_index, 
                                             barcode
                                            )
        map_sample_job = dxpy.new_dxjob(fn_input={
                                                  "project_id": lane.project_id,
                                                  "output_folder": output_folder,
                                                  "fastq_files": fastq_files,
                                                  "fastq_files2": fastq_files2,
                                                  "genome_fasta_file": lane.reference_genome_dxid,
                                                  "genome_index_file": lane.reference_index_dxid,
                                                  "mapper": lane.mapper,
                                                  "sample_name": sample_name,
                                                  "mark_duplicates": mark_duplicates,
                                                  "applet_id": worker_id,
                                                  "applet_project": worker_project,
                                                  "properties": mapped_files_properties
                                                 }, 
                                        fn_name="run_map_sample"
                                       ) 
        output["bams"].append({"job": map_sample_job.get_id(), "field": "bam"})
        output["bais"].append({"job": map_sample_job.get_id(), "field": "bai"})
        output["tools_used"].append({"job": map_sample_job.get_id(), "field": "tools_used"})
    return output

dxpy.run()
