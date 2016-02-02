#!usr/bin/python
''' 
Description : This applet will convert bcl files output by illumina sequencing
    platforms to unmapped fastq files. It will be the first applet called by 
    most sequence processing workflows on DNAnexus.

    Most of the metadata necessary to run this applet should already be specified
    in the dashboard record by the 'autocopy' script and the 'initiate_workflow'
    applet. However, this applet will still need to pull barcode information from
    the LIMS.

Args : DNAnexus ID of dashboard record for sequencing lane project
Returns : 
Author : pbilling
'''

'''
# The workflow is not a custom script, its just a set of applet calls

## Input arguments
# Dashboard record
# I think if I set up the dashboard record properly, I can get all information from that

## Function overview:
# Update dashboard entry
# Get barcode information for sequencing lane
# Create sample sheet
# Run bcl2fastq to generate fastq files
# Upload fastq files
# Update dashboard entry

'''

import os
import re
import sys
import dxpy
import time
import shutil
import fnmatch
import datetime
import subprocess

from distutils.version import StrictVersion

class InputParameters:

    def __init__(self, params_dict):
        ''' Description: Parameter object to handle input parameters with goal
        of reducing bloat of 'main' method call, and increasing order and 
        readability of input parameters

        Input:
        params_dict (dictionary): All input parameters 
        '''
        
        # Required parameters
        self.record_dxid = params_dict['record_dxid']
        self.lane_data_tar = params_dict['lane_data_tar']
        self.metadata_tar = params_dict['metadata_tar']

        # Optional parameters
        if not 'dashboard_project_dxid' in params_dict.keys():
            self.dashboard_project_dxid = 'project-BY82j6Q0jJxgg986V16FQzjx'
        else:
            self.dashboard_project_dxid = params_dict['dashboard_project_dxid']

        if not 'test_mode' in params_dict.keys():
            self.test_mode = False
        else:
            self.test_mode = params_dict['test_mode']

        if not 'mismatches' in params_dict.keys():
            self.mismatches = 1
        else:
            self.mismatches = params_dict['mismatches']

        if not 'ignore_missing_stats' in params_dict.keys():
            self.ignore_missing_stats = True
        else:
            self.ignore_missing_stats = params_dict['ignore_missing_stats']

        if not 'ignore_missing_bcl' in params_dict.keys():
            self.ignore_missing_bcl = True
        else:
            self.ignore_missing_bcl = params_dict['ignore_missing_bcl']

        if not 'with_failed_reads' in params_dict.keys():
            self.with_failed_reads = True
        else:
            self.with_failed_reads = params_dict['with_failed_reads']

        if not 'tiles' in params_dict.keys():
            self.tiles = 1112
        else:
            self.tiles = params_dict['tiles']


class FlowcellLane:
    
    def __init__(self, dashboard_record_dxid, dashboard_project_dxid):
        
        self.dashboard_record_dxid = dashboard_record_dxid 
        self.dashboard_project_dxid = dashboard_project_dxid
        self.dashboard_record = dxpy.DXRecord(dxid = self.dashboard_record_dxid, 
                                              project = self.dashboard_project_dxid)
        self.properties = self.dashboard_record.get_properties()

        # For now just get/put everything in properties
        self.lane_project_dxid = self.properties['lane_project_dxid']
        self.run_name = self.properties['run']
        self.lane_index = int(self.properties['lane_index'])
        self.lims_url = self.properties['lims_url']
        self.lims_token = self.properties['lims_token']
        self.rta_version = self.properties['rta_version']

        self.lane_project = dxpy.DXProject(dxid = self.lane_project_dxid)
        self.home = os.getcwd()

        self.sample_sheet = None
        self.output_dir = None
        self.flowcell_id = None
        self.bcl2fastq_version = None

        # DEV: get this info from samplesheet - more robust to formatting changes
        # Get flowcell id
        #run_elements = self.run_name.split('_')
        #flowcell_info = run_elements[3]
        #self.flowcell_id = flowcell_info[1:6]

        # Choose bcl2fastq version based on rta_version
        if StrictVersion(self.rta_version) < StrictVersion('2.0.0'):
            self.bcl2fastq_version = 1
        elif StrictVersion(self.rta_version) >= StrictVersion('2.0.0'):
            self.bcl2fastq_version = 2

    def describe(self):
        print "Sequencing run: %s" % self.run_name
        print "Flowcell lane index: %s" % self.lane_index

    def unpack_tar(self, tar_file_dxlink):
        '''
        DEV: Eventually integrate dx-toolkit into trajectoread repo so I can 
             transition to using 'dx-download-all-inputs' to handle unpacking
             all input files
        Description: Download and untar metadata and lane data files 
                     (/Data/Intensities/BaseCalls)
        '''

        #metadata_tar_file = '%s.metadata.tar' % (self.run_name)
        #data_tar_file = '%s.L%d.tar' % (self.run_name, self.lane_index)

        # Find lane tar files on DNAnexus
        #metadata_dict = dxpy.find_one_data_object(name=metadata_tar_file, project=self.lane_project_dxid, folder='/', more_ok=False, zero_ok=False)
        #data_dict = dxpy.find_one_data_object(name=data_tar_file, project=self.lane_project_dxid, folder='/', more_ok=False, zero_ok=False)

        #metadata_dxid = metadata_dict['id']
        #data_dxid = data_dict['id']

        if dxpy.is_dxlink(tar_file_dxlink):
            file_handler = dxpy.get_handler(tar_file_dxlink)
            filename = file_handler.name
        else:
            print 'Error: Cannot unpack %s; not a valid DXLink object'
            sys.exit()

        # ('file-dxid', 'project-dxid') = dxpy.get_dxlink_ids(dxlink)
        file_dxid = dxpy.get_dxlink_ids(tar_file_dxlink)[0]
        project_dxid = dxpy.get_dxlink_ids(tar_file_dxlink)[1]

        # Download file from DNAnexus objectstore to virtual machine
        dxpy.download_dxfile(dxid=file_dxid, filename=filename, project=project_dxid)

        # Untar file
        command = 'tar -xf %s --owner root --group root --no-same-owner' % filename
        self.createSubprocess(cmd=command, pipeStdout=False)

    def upload_result_files(self):
        ''' DEV: Look into using glob.glob to find all fastq files instead of 
                 manually listing the directories and searching them, a la 
                 implementation in gbsc/gbsc_utils/demultiplexing.py.
        '''
        
        fastq_files = []
        lane_dir = self.home + '/Unaligned_L' + str(self.lane_index)

        # Upload fastq files generated by bcl2fastq version 1 (1.8.4)
        if self.bcl2fastq_version == 1:
            flowcell_dir = lane_dir + '/Project_' + self.flowcell_id
            sample_dir = flowcell_dir + '/Sample_lane%s' % self.lane_index

            # Upload all the fastq files from the lane directory (Unaligned_L%d)
            os.chdir(sample_dir)
            for filename in os.listdir('.'):
                if fnmatch.fnmatch(filename, '*.fastq.gz'):
                    scgpm_fastq_name = self.get_SCGPM_fastq_name_rta_v1(filename)
                    if not os.path.isfile(scgpm_fastq_name):
                        shutil.move(filename, scgpm_fastq_name)
                    fastq_file = dxpy.upload_local_file(filename=scgpm_fastq_name, properties=None, 
                                            project=self.lane_project_dxid, 
                                            folder='/', parents=True)
                    fastq_files.append(dxpy.dxlink(fastq_file))
        
        # Upload fastq files generated by bcl2fastq version 2 (2.17.-)
        elif self.bcl2fastq_version == 2:
            flowcell_dir = lane_dir + '/' + self.flowcell_id
            
            # Upload all the fastq files from the lane directory (Unaligned_L%d)            
            os.chdir(lane_dir)
            for filename in os.listdir('.'):
                if fnmatch.fnmatch(filename, '*.fastq.gz'):
                    scgpm_fastq_name = self.get_SCGPM_fastq_name_rta_v2(filename)
                    if not os.path.isfile(scgpm_fastq_name):
                        shutil.move(filename, scgpm_fastq_name)
                    fastq_file = dxpy.upload_local_file(filename=scgpm_fastq_name, properties=None, 
                                            project=self.lane_project_dxid, 
                                            folder='/', parents=True)
                    fastq_files.append(dxpy.dxlink(fastq_file))

            # Upload all the fastq files from the flowcell directory (Unaligned_L%d/<flowcell_id>)
            os.chdir(flowcell_dir)
            for filename in os.listdir('.'):
                if fnmatch.fnmatch(filename, '*.fastq.gz'):
                    scgpm_fastq_name = self.get_SCGPM_fastq_name_rta_v2(filename)
                    if not os.path.isfile(scgpm_fastq_name):
                        shutil.move(filename, scgpm_fastq_name)
                    fastq_file = dxpy.upload_local_file(filename=scgpm_fastq_name, properties=None, 
                                            project=self.lane_project_dxid, 
                                            folder='/', parents=True)
                    fastq_files.append(dxpy.dxlink(fastq_file))
        else:
            print 'Error: bcl2fastq applet not equipped to handle RTA version %d files' % self.bcl2fastq_version
            sys.exit()
        return(fastq_files)

    def create_sample_sheet(self):
        '''
        create_sample_sheet.py -r ${seq_run_name}
            -t ${UHTS_LIMS_TOKEN}   9af4cc6d83fbfd793fe4
            -u ${UHTS_LIMS_URL}     https://uhts.stanford.edu
            -b 2
            -l ${SGE_TASK_ID}
        '''

        ## Create samplesheet
        ## DEV: break up 'command' using +=
        command = 'python create_sample_sheet.py -r %s -t %s -u %s -b %d -l %s' % (self.run_name, self.lims_token, self.lims_url, self.bcl2fastq_version, self.lane_index)
        stdout,stderr = self.createSubprocess(cmd=command, pipeStdout=True)
        self.sample_sheet = '%s_L%d_samplesheet.csv' % (self.run_name, self.lane_index)
        stdout_elements = stdout.split()
        self.sample_sheet = stdout_elements[1]
        print 'This is the self.sample_sheet: %s' % self.sample_sheet
        
        # DEV: insert check so that samplesheet is only uploaded if does not exist.
        #      Also, maybe add it to output?
        dxpy.upload_local_file(filename = self.sample_sheet, 
                               properties = None, 
                               project = self.lane_project_dxid, 
                               folder = '/', 
                               parents = True
                              )
        return self.sample_sheet
        
    def get_flowcell_id(self):
        ''' Description: Get flowcell ID from samplesheet generated from LIMS.
        '''

        if not self.sample_sheet:
            warning = 'Warning: Cannot get flowcell ID without creating '
            warning += 'samplesheet. Creating samplesheet now.'
            self.sample_sheet = self.create_sample_sheet()

        get_flowcell_id_from_line = False
        with open(self.sample_sheet, 'r') as SAMPLE_SHEET:
            for line in SAMPLE_SHEET:
                elements = line.split(',')
                if get_flowcell_id_from_line == True:
                    self.flowcell_id = elements[0]
                    break
                elif elements[0] == 'FCID' or elements[0] == 'Sample_Project':
                    # RTA v1 == 'FCID', RTA v2 == 'Sample_Project'
                    get_flowcell_id_from_line = True
                else:
                    continue

        if not self.flowcell_id:
            print 'Error: Could not get flowcell ID from sample sheet'
            sys.exit()
        else:
            # Add flowcell ID as a record property
            input_params = {
                            'project': self.dashboard_project_dxid, 
                            'properties': {'flowcell_id': self.flowcell_id}
                           }
            print input_params
            print self.dashboard_record_dxid
            dxpy.api.record_set_properties(object_id = self.dashboard_record_dxid,
                                           input_params = input_params)
            return self.flowcell_id

    def get_use_bases_mask(self):
        '''
        command = "python calculate_use_bases_mask.py {runinfoFile} {sampleSheet} {lane}"
        gbsc_utils.createSubprocess(cmd=command)
        '''
        
        run_info_file = 'RunInfo.xml'
        
        # TEST LINK - remove for production
        #dxpy.download_dxfile(dxid='file-Bkv0yBQ0gq319q3bfx5B9Bzj', filename=run_info_file, project=self.lane_project_dxid) # HiSeq 4000
        #dxpy.download_dxfile(dxid='file-Bp5Q2x804f6Q7Xz4KjZQ2PK1', filename=run_info_file, project=self.lane_project_dxid)  # HiSeq 2000

        command = 'python calculate_use_bases_mask.py %s %s %s %d' % (
            run_info_file, self.sample_sheet, self.lane_index, self.bcl2fastq_version)
        stdout,stderr = self.createSubprocess(cmd=command, pipeStdout=True)
        self.use_bases_mask = stdout
        print 'This is use_bases_mask value: %s' % self.use_bases_mask

        use_bases_mask_file = 'use_bases_mask.txt'
        with open(use_bases_mask_file, 'w') as OUT:
            OUT.write(self.use_bases_mask)
        dxpy.upload_local_file(filename=use_bases_mask_file, properties=None, project=self.lane_project_dxid, folder='/', parents=True)
        return self.use_bases_mask

    def run_bcl2fastq(self, mismatches, ignore_missing_stats, ignore_missing_bcl, with_failed_reads, tiles, test_mode):
        '''
        DEV: Change definition line to "def run_bcl2fastq(self, **optional_params)"
        bcl2fastq --output-dir ${new_run_dir}/${seq_run_name}/Unaligned_L${SGE_TASK_ID}
            --sample-sheet ${new_run_dir}/${seq_run_name}/${seq_run_name}_L${SGE_TASK_ID}_samplesheet.csv
            --ignore-missing-bcls
            --ignore-missing-filter
            --ignore-missing-positions
            --barcode-mismatches 1
            --use-bases-mask ${SGE_TASK_ID}:Y*,n*,Y*
        '''

        self.output_dir = 'Unaligned_L%d' % self.lane_index

        # bcl2fastq version 2 for HiSeq 4000s
        if self.bcl2fastq_version == 2:
            self.output_dir = 'Unaligned_L%d' % self.lane_index
            #command = 'bcl2fastq --output-dir %s --sample-sheet %s --barcode-mismatches %d --use-bases-mask %d:%s' % (
            #    self.output_dir, self.sample_sheet, 1, int(self.lane_index), self.use_bases_mask)
            command = 'bcl2fastq ' 
            command += '--output-dir %s ' % self.output_dir
            command += '--sample-sheet %s ' % self.sample_sheet
            command += '--barcode-mismatches %d ' % mismatches
            command += '--use-bases-mask %d:%s ' % (int(self.lane_index), self.use_bases_mask)
            if test_mode:
                command += '--tiles %d ' % tiles
            stdout,stderr = self.createSubprocess(cmd=command, pipeStdout=True)
        
        # bcl2fastq version 1 for HiSeq 2000s/MiSeq (1.8.4)
        elif self.bcl2fastq_version == 1:

            configure_script_path = '/usr/local/bin/configureBclToFastq.pl'
            basecalls_dir = './Data/Intensities/BaseCalls'

            ### Ripped from casava_bcl_to_fastq.py applet source
            # Gather options for the configureBclToFastq.pl command
            opts = "--no-eamss --input-dir " + basecalls_dir + " --output-dir " +  self.output_dir 
            opts +=" --sample-sheet " + self.sample_sheet
            opts +=" --use-bases-mask " + self.use_bases_mask
            opts += " --fastq-cluster-count 0"  # I don't know what this does
            if test_mode:
                ignore_missing_bcl = True
            if mismatches:
                opts += " --mismatches " + str(mismatches)
            if ignore_missing_stats:
                opts += " --ignore-missing-stats"
            if ignore_missing_bcl:
                opts += " --ignore-missing-bcl"
            if  with_failed_reads:
                opts += " --with-failed-reads"
            if tiles:
                opts += " --tiles " + str(tiles)

            # Run it
            self.createSubprocess(cmd=configure_script_path + " " + opts,checkRetcode=True,pipeStdout=False)
            self.createSubprocess(cmd="nohup make -C " + self.output_dir + " -j `nproc`",checkRetcode=True,pipeStdout=False)   # Don't know what this does

        else:
            print 'Could not determine bcl2fastq version'
            print EMPTY_DEBUG_VARIABLE

    def get_rta_version(self, params_file):
        with open(params_file, 'r') as PARAM:
            for line in PARAM:
                match = re.search(r'<RTAVersion>([\d\.]+)</RTAVersion>', line)
                if match:
                    rta_version = match.group(1)
                    break
        return rta_version

    def createSubprocess(self, cmd, pipeStdout=False, checkRetcode=True):
        """
        Function : Creates a subprocess via a call to subprocess.Popen with the argument 'shell=True', and pipes stdout and stderr. Stderr is always  piped, but stdout can be turned off.
                 If the argument checkRetcode is True, which it is by defualt, then for any non-zero return code, an Exception is
                             raised that will print out the the command, stdout, stderr, and the returncode when not caught. Otherwise, the Popen instance will be return, in which case the caller must
                           call the instance's communicate() method (and not it's wait() method!!) in order to get the return code to see if the command was a success. communicate() will return
                             a tuple containing (stdout, stderr). But at that point, you can then check the return code with Popen instance's 'returncode' attribute.
        Args     : cmd   - str. The command line for the subprocess wrapped in the subprocess.Popen instance. If given, will be printed to stdout when there is an error in the subprocess.
                             pipeStdout - bool. True means to pipe stdout of the subprocess.
                             checkRetcode - bool. See documentation in the description above for specifics.
        Returns  : A two-item tuple containing stdout and stderr, respectively.
        """
        stdout = None
        if pipeStdout:
            stdout = subprocess.PIPE
            stderr = subprocess.PIPE
        popen = subprocess.Popen(cmd,shell=True,stdout=stdout,stderr=subprocess.PIPE)
        if checkRetcode:
            stdout,stderr = popen.communicate()
            if not stdout: #will be None if not piped
                stdout = ""
            stdout = stdout.strip()
            stderr = stderr.strip()
            retcode = popen.returncode
            if retcode:
                #below, I'd like to raise a subprocess.SubprocessError, but that doens't exist until Python 3.3.
                raise Exception("subprocess command '{cmd}' failed with returncode '{returncode}'.\n\nstdout is: '{stdout}'.\n\nstderr is: '{stderr}'.".format(cmd=cmd,returncode=retcode,stdout=stdout,stderr=stderr))
            return stdout,stderr
        else:
            return popen
            #return stdout,stderr

    def get_SCGPM_fastq_name_rta_v1(self, fastq_filename):
        '''
        Returns a fastq filename that matches SCGPM naming convention.
        Does not actually rename files.
        '''

        elements = fastq_filename.split('_')
        if len(elements) < 4 or len(elements) > 6:
            print 'WARNING: fastq filename has unusual number of elements : %s' % fastq
            sys.exit()
        
        # One barcode : lane1_TAGGCATG_L001_R2_001.fastq.gz
        # Two barcodes : lane1_TCTCGCGC-TCAGAGCC_L001_R2_001.fastq.gz
        elif len(elements) == 5:
            lane = elements[0]
            barcode = elements[1]
            read = elements[3]

            lane_index_match = re.match(r'lane(\d)', lane)
            if lane_index_match:
                lane_index = lane_index_match.group(1)
            else:
                print 'Could not determine lane index: %s' % lane
                sys.exit()
            read_index_match = re.match(r'R(\d)', read)
            #pdb.set_trace()
            if read_index_match:
                read_index = read_index_match.group(1)
            else:
                print 'Could not determine read index: %s' % read
                sys.exit()
            # new format : 151202_BRISCOE_0270_BC847TACXX_L1_TAGGCATG_1_pf.fastq.gz
            new_fastq_filename = '%s_L%s_%s_%s_pf.fastq.gz' % (self.run_name, lane_index, barcode, read_index)
        
        # No barcode : Undetermined_L001_R1_001.fastq.gz
        elif len(elements) == 4:
            lane = elements[1]
            read = elements[2]

            lane_index_match = re.match(r'L00(\d)', lane)
            if lane_index_match:
                lane_index = lane_index_match.group(1)
            else:
                print 'Could not determine lane index: %s' % lane
                sys.exit()
            read_index_match = re.match(r'R(\d)', read)
            if read_index_match:
                read_index = read_index_match.group(1)
            else:
                print 'Could not determine read index: %s' % read
            # new format : 151106_LYNLEY_0515_AC7F31ACXX_L1_unmatched_1_pf.fastq.gz
            new_fastq_filename = '%s_L%s_unmatched_%s_pf.fastq.gz' % (self.run_name, lane_index, read_index)
        
        else:
            print "Could not get metadata for:\nfastq: %s\nlane: %s\nrun: %s" % (fastq, lane, self.run_name)
            pass
        return new_fastq_filename

    def get_SCGPM_fastq_name_rta_v2(self, fastq_filename):
        '''
        Returns a fastq filename that matches SCGPM naming convention.
        Does not actually rename files.
        '''

        # DEV: I need a better way to do this.
        # Information I need: run, lane, read index, barcode
        # FlowcellLane object already has run, lane
        # Sample sheet has list of all barcodes
        # <run>_<lane>_<barcode>_<read_index>.fastq.gz
        # Need to be able to find read_index and barcode information
        elements = fastq_filename.split('_')
        if len(elements) < 5 or len(elements) > 7:
            print 'WARNING: fastq filename has unusual number of elements : %s' % fastq
            sys.exit()
        elif len(elements) == 7:
            # Two barcodes : lane1_TCTCGCGC_TCAGAGCC_S47_L001_R2_001.fastq.gz
            lane = elements[0]
            barcodes = (elements[1], elements[2])
            read = elements[5]

            lane_index_match = re.match(r'lane(\d)', lane)
            if lane_index_match:
                lane_index = lane_index_match.group(1)
            else:
                print 'Could not determine lane index: %s' % lane
                sys.exit()
            read_index_match = re.match(r'R(\d)', read)
            #pdb.set_trace()
            if read_index_match:
                read_index = read_index_match.group(1)
            else:
                print 'Could not determine read index: %s' % read
            
            # new format : 151202_BRISCOE_0270_BC847TACXX_L1_TAGGCATG-TAGGCATG_1_pf.fastq.gz
            new_fastq_filename = '%s_L%s_%s-%s_%s_pf.fastq.gz' % (self.run_name, lane_index, barcodes[0], barcodes[1], read_index) 
        elif len(elements) == 6:
            # One barcode : lane1_TCAGAGCC_S47_L001_R2_001.fastq.gz
            lane = elements[0]
            barcode = elements[1]
            read = elements[4]

            lane_index_match = re.match(r'lane(\d)', lane)
            if lane_index_match:
                lane_index = lane_index_match.group(1)
            else:
                print 'Could not determine lane index: %s' % lane
                sys.exit()
            read_index_match = re.match(r'R(\d)', read)
            #pdb.set_trace()
            if read_index_match:
                read_index = read_index_match.group(1)
            else:
                print 'Could not determine read index: %s' % read
                sys.exit()

            # new format : 151202_BRISCOE_0270_BC847TACXX_L1_TAGGCATG_1_pf.fastq.gz
            new_fastq_filename = '%s_L%s_%s_%s_pf.fastq.gz' % (self.run_name, lane_index, barcode, read_index)
        elif len(elements) == 5:
            # No barcode : Undetermined_S1_L001_R1_001.fastq.gz
            lane = elements[2]
            read = elements[3]

            lane_index_match = re.match(r'L00(\d)', lane)
            if lane_index_match:
                lane_index = lane_index_match.group(1)
            else:
                print 'Could not determine lane index: %s' % lane
                sys.exit()
            read_index_match = re.match(r'R(\d)', read)
            if read_index_match:
                read_index = read_index_match.group(1)
            else:
                print 'Could not determine read index: %s' % read
            
            # new format : 151106_LYNLEY_0515_AC7F31ACXX_L1_unmatched_1_pf.fastq.gz
            new_fastq_filename = '%s_L%s_unmatched_%s_pf.fastq.gz' % (self.run_name, lane_index, read_index)
        else:
            print "Could not get metadata for:\nfastq: %s\nlane: %s\nrun: %s" % (fastq, lane, self.run_name)
            pass
        return new_fastq_filename

@dxpy.entry_point("main")
def main(**applet_input):
    ''' Description: Use illumina bcl2fastq applet to perform demultiplex and 
    convert bcl files to fastq files. Currently handles files generated from
    RTA version 2.7.3 and earlier.

    Input:
    applet_input (dictionary): Input parameters specified when calling applet 
                               from DNAnexus
    '''

    output = {}
    params = InputParameters(applet_input)

    lane = FlowcellLane(dashboard_record_dxid = params.record_dxid,
                        dashboard_project_dxid = params.dashboard_project_dxid)
    lane.describe()
    
    print 'Downloading lane data'
    lane.unpack_tar(params.lane_data_tar)
    lane.unpack_tar(params.metadata_tar)
    
    print 'Creating sample sheet\n'
    lane.create_sample_sheet()

    print 'Parsing sample sheet to get flowcell ID'
    lane.get_flowcell_id()
    
    print 'Get use bases mask\n'
    lane.get_use_bases_mask()
    
    print 'Convert bcl to fastq files'
    lane.run_bcl2fastq(mismatches = params.mismatches,
                       ignore_missing_stats = params.ignore_missing_stats,
                       ignore_missing_bcl = params.ignore_missing_bcl,
                       with_failed_reads = params.with_failed_reads,
                       tiles = params.tiles,
                       test_mode = params.test_mode
                       )
    
    print 'Uploading fastq files back to DNAnexus'
    fastq_files = lane.upload_result_files()        # returns DXLink objects
    #print EMPTY_DEBUG_VARIABLE

    output = {}
    output['fastqs'] = fastq_files

    return output

dxpy.run()
