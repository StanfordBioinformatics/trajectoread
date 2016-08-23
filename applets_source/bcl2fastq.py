#!usr/bin/python
''' 
Description: Convert bcl files output by illumina sequencing platforms to unmapped 
             fastq files. It will be the first applet called by most sequence 
             processing workflows on DNAnexus.
Args : DNAnexus ID of dashboard record for sequencing lane project
Returns : 
Author : pbilling
'''


import os
import re
import sys
import dxpy
import time
import json
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
        self.record_id = params_dict['record_id']
        self.lane_data_tar = params_dict['lane_data_tar']
        self.metadata_tar = params_dict['metadata_tar']

        # Optional parameters
        if not 'output_folder' in params_dict.keys():
            self.output_folder = '/stage_bcl2fastq'
        else:
            self.output_folder = params_dict['output_folder']

        if not 'test_mode' in params_dict.keys():
            self.test_mode = False
        else:
            self.test_mode = params_dict['test_mode']

        if not 'barcode_mismatches' in params_dict.keys():
            self.mismatches = 1
        else:
            self.mismatches = params_dict['barcode_mismatches']

        if not 'ignore_missing_stats' in params_dict.keys():
            self.ignore_missing_stats = True
        else:
            self.ignore_missing_stats = params_dict['ignore_missing_stats']

        if not 'ignore_missing_bcl' in params_dict.keys():
            self.ignore_missing_bcl = True
        else:
            self.ignore_missing_bcl = params_dict['ignore_missing_bcl']

        if not 'ignore_missing_positions' in params_dict.keys():
            self.ignore_missing_positions = True
        else:
            self.ignore_missing_positions = params_dict['ignore_missing_positions']

        if not 'ignore_missing_filter' in params_dict.keys():
            self.ignore_missing_filter = True
        else:
            self.ignore_missing_filter = params_dict['ignore_missing_filter']

        if not 'with_failed_reads' in params_dict.keys():
            self.with_failed_reads = False
        else:
            self.with_failed_reads = params_dict['with_failed_reads']

        if not 'tiles' in params_dict.keys():
            self.tiles = 1112
        else:
            self.tiles = params_dict['tiles']

class FlowcellLane:
    
    def __init__(self, record_id):
        
        self.record_id = record_id.strip()
        record_project = self.record_id.split(':')[0]
        record_dxid = self.record_id.split(':')[1]
        self.record = dxpy.DXRecord(dxid=record_dxid, project=record_project)
        
        self.properties = self.record.get_properties()
        self.details = self.record.get_details()

        # Details (Used for Dashboard information)
        self.lane_project_id = self.details['laneProject']
        self.run_name = self.details['run']
        self.run_date = self.run_name.split('_')[0]
        self.lane_index = int(self.details['lane'])
        self.library_id = self.details['library_id']
        self.lane_id = self.details['lane_id']

        # Parse library name ("DL_set2_rep1 rcvd 1/4/16")
        library_label = self.details['library']
        elements = library_label.split('rcvd')
        library_name = elements[0].rstrip()
        self.library_name = re.sub(r"[^a-zA-Z0-9]+", "-", library_name)

        # Properties
        self.lims_url = self.properties['lims_url']
        self.lims_token = self.properties['lims_token']
        self.rta_version = self.properties['rta_version']
        self.seq_instrument = self.properties['seq_instrument']

        self.lane_project = dxpy.DXProject(dxid = self.lane_project_id)
        self.home = os.getcwd()

        self.sample_sheet = None
        self.output_dir = None
        self.bcl2fastq_version = None
        self.lane_barcode = None
        self.flowcell_id = None

        # Choose bcl2fastq version based on rta_version
        ## DEV: Update version to match official documentation: i.e. 1.18.54 or later
        if StrictVersion(self.rta_version) < StrictVersion('1.18.54'):
            self.bcl2fastq_version = 1
        elif StrictVersion(self.rta_version) >= StrictVersion('1.18.54'):
            self.bcl2fastq_version = 2

    def describe(self):
        print "Sequencing run: %s" % self.run_name
        print "Flowcell lane index: %s" % self.lane_index

    def unpack_tar(self, tar_file_dxlink):
        '''
        DEV: Eventually integrate dx-toolkit into trajectoread repo so I can 
             transition to using 'dx-download-all-inputs' to handle unpacking
             all input files.
             Pipeline used to store lane file dxids as project properties 
             and then pass to "dx download"
        Description: Download and untar metadata and lane data files 
                     (/Data/Intensities/BaseCalls)
        '''

        if dxpy.is_dxlink(tar_file_dxlink):
            file_handler = dxpy.get_handler(tar_file_dxlink)
            filename = file_handler.name
        else:
            print 'Error: Cannot unpack %s; not a valid DXLink object'
            sys.exit()

        # ('file-dxid', 'project-dxid') = dxpy.get_dxlink_ids(dxlink)
        file_dxid = dxpy.get_dxlink_ids(tar_file_dxlink)[0]
        project_id = dxpy.get_dxlink_ids(tar_file_dxlink)[1]

        # Download file from DNAnexus objectstore to virtual machine
        dxpy.download_dxfile(dxid=file_dxid, filename=filename, project=project_id)

        # Untar file
        ## DEV: Check if this is even in use anymore; also should have some method for 
        ## checking what type of compression was used.
        ## But I don't think this is in use
        command = 'tar -xf %s --owner root --group root --no-same-owner' % filename
        self.createSubprocess(cmd=command, pipeStdout=False)

    def upload_result_files(self, output_folder):
        ''' DEV: Look into using glob.glob to find all fastq files instead of 
                 manually listing the directories and searching them, a la 
                 implementation in gbsc/gbsc_utils/demultiplexing.py.
            DEV: Upload
        '''
        
        fastq_files = []
        fastqs_subfolder = output_folder + '/fastqs'
        misc_subfolder = output_folder + '/miscellany'
        lane_dir = self.home + '/Unaligned_L' + str(self.lane_index)

        # Upload lane.html file
        self.lane_barcode = self.get_lane_barcode()
        report_html_file = 'Unaligned_L%d/Reports/html/%s/all/all/all/lane.html' % (self.lane_index, self.lane_barcode)
        properties = {
                      'run_date':   str(self.run_date),
                      'run_name':   str(self.run_name),
                      'library_id': str(self.library_id),
                      'lane_id':    str(self.lane_id),
                      'lane_index': str(self.lane_index),
                      'library_name': str(self.library_name)
                     }
        lane_html_name = 'SCGPM_%s_%s_%s_L%d.lane.html' % (self.run_date,
                                                           self.library_name, 
                                                           self.flowcell_id,
                                                           self.lane_index 
                                                          )
        lane_html_file = dxpy.upload_local_file(filename = report_html_file,
                                                name =  lane_html_name,
                                                properties = properties, 
                                                project = self.lane_project_id, 
                                                folder = misc_subfolder, 
                                                parents = True
                                               )

        # Upload fastq files generated by bcl2fastq version 1 (1.8.4)
        ## Note: This version shouldn't ever be used anymore
        if self.bcl2fastq_version == 1:
            warning = 'Warning: Using bcl2fastq version 1.8.4. All sequencing platforms '
            warning += 'should be compliant with bcl2fastq (RTA >= 1.18.54)'
            print warning

            flowcell_dir = lane_dir + '/Project_' + self.flowcell_id
            sample_dir = flowcell_dir + '/Sample_lane%s' % self.lane_index

            # Upload all the fastq files from the lane directory (Unaligned_L%d)
            os.chdir(sample_dir)
            for filename in os.listdir('.'):
                if fnmatch.fnmatch(filename, '*.fastq.gz'):
                    scgpm_names = self.get_SCGPM_fastq_name_rta_v1(filename)
                    scgpm_fastq_name = scgpm_names[0]
                    barcode = scgpm_names[1]
                    read_index = scgpm_names[2]
                    fastq_name_v2 = 'SCGPM_%s_%s_%s_L%d_%s_R%d.fastq.gz' % (self.run_date,
                                                                        self.library_name,
                                                                        self.flowcell_id,
                                                                        self.lane_index,
                                                                        barcode,
                                                                        int(read_index)
                                                                       )
                    properties = {'barcode': barcode,
                                  'read': str(read),
                                  'run_date': self.run_date,
                                  'library_id': self.library_id,
                                  'lane_id': self.lane_id,
                                  'library_name': str(self.library_name)
                                 }

                    if not os.path.isfile(fastq_name_v2):
                        shutil.move(filename, fastq_name_v2)
                    fastq_file = dxpy.upload_local_file(filename = fastq_name_v2, 
                                                        properties = properties, 
                                                        project = self.lane_project_id, 
                                                        folder = fastqs_subfolder, 
                                                        parents = True)
                    fastq_files.append(dxpy.dxlink(fastq_file))
        
        # Upload fastq files generated by bcl2fastq version 2 (2.17.-)
        elif self.bcl2fastq_version == 2:
            flowcell_dir = lane_dir + '/' + self.flowcell_id
            
            # Upload all the fastq files from the lane directory (Unaligned_L%d)            
            os.chdir(lane_dir)
            for filename in os.listdir('.'):
                if fnmatch.fnmatch(filename, '*.fastq.gz'):
                    scgpm_names = self.get_SCGPM_fastq_name_rta_v2(filename)
                    scgpm_fastq_name = scgpm_names[0]
                    barcode = scgpm_names[1]
                    read_index = scgpm_names[2]
                    fastq_name_v2 = 'SCGPM_%s_%s_%s_L%d_%s_R%d.fastq.gz' % (self.run_date,
                                                                        self.library_name, 
                                                                        self.flowcell_id,
                                                                        self.lane_index,  
                                                                        barcode, 
                                                                        int(read_index)
                                                                       )
                    properties = {'barcode': barcode,
                                  'read': str(read_index),
                                  'run_date': self.run_date,
                                  'library_id': self.library_id,
                                  'lane_id': self.lane_id,
                                  'library_name': str(self.library_name)
                                 }

                    if not os.path.isfile(fastq_name_v2):
                        shutil.move(filename, fastq_name_v2)
                    fastq_file = dxpy.upload_local_file(filename = fastq_name_v2, 
                                                        properties = properties, 
                                                        project = self.lane_project_id, 
                                                        folder = fastqs_subfolder, 
                                                        parents = True)
                    fastq_files.append(dxpy.dxlink(fastq_file))

            # Upload all the fastq files from the flowcell directory (Unaligned_L%d/<flowcell_id>)
            os.chdir(flowcell_dir)
            for filename in os.listdir('.'):
                if fnmatch.fnmatch(filename, '*.fastq.gz'):
                    scgpm_names = self.get_SCGPM_fastq_name_rta_v2(filename)
                    scgpm_fastq_name = scgpm_names[0]
                    barcode = scgpm_names[1]
                    read_index = scgpm_names[2]
                    fastq_name_v2 = 'SCGPM_%s_%s_%s_L%d_%s_R%d.fastq.gz' % (self.run_date,
                                                                        self.library_name, 
                                                                        self.flowcell_id,
                                                                        self.lane_index,
                                                                        barcode, 
                                                                        int(read_index)
                                                                       )
                    properties = {'barcode': barcode,
                                  'read': str(read_index),
                                  'run_date': self.run_date,
                                  'library_id': self.library_id,
                                  'lane_id': self.lane_id,
                                  'library_name': str(self.library_name)
                                 }

                    if not os.path.isfile(fastq_name_v2):
                        shutil.move(filename, fastq_name_v2)
                    fastq_file = dxpy.upload_local_file(filename = fastq_name_v2, 
                                                        properties = properties, 
                                                        project = self.lane_project_id, 
                                                        folder = fastqs_subfolder, 
                                                        parents = True)
                    fastq_files.append(dxpy.dxlink(fastq_file))
        else:
            print 'Error: bcl2fastq applet not equipped to handle RTA version %d files' % self.bcl2fastq_version
            sys.exit()
        
        print 'Uploaded fastq files:'
        for dxlink in fastq_files:
            print dxlink

        output = {
                  'fastqs': fastq_files,
                  'lane_html': dxpy.dxlink(lane_html_file)
                 }
        return(output)

    def create_sample_sheet(self, output_folder):
        ''' Description:
        '''

        misc_subfolder = output_folder + '/miscellany'
        
        command = 'python create_sample_sheet.py -r %s ' % self.run_name
        command += '-t %s ' % self.lims_token
        command += '-u %s ' % self.lims_url
        command += '-b %d ' % int(self.bcl2fastq_version)
        command += '-l %d' % int(self.lane_index)
        
        stdout,stderr = self.createSubprocess(cmd=command, pipeStdout=True)
        
        self.sample_sheet = '%s_L%d_samplesheet.csv' % (self.run_name, self.lane_index)
        stdout_elements = stdout.split()
        self.sample_sheet = stdout_elements[1]
        print 'This is the self.sample_sheet: %s' % self.sample_sheet
        
        # DEV: insert check so that samplesheet is only uploaded if does not exist.
        #      Also, maybe add it to output?
        dxpy.upload_local_file(filename = self.sample_sheet, 
                               properties = None, 
                               project = self.lane_project_id, 
                               folder = misc_subfolder, 
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
                            'project': self.record.project, 
                            'properties': {'flowcell_id': self.flowcell_id}
                           }
            print input_params
            dxpy.api.record_set_properties(object_id = self.record.id,
                                           input_params = input_params)
            return self.flowcell_id

    def get_use_bases_mask(self, output_folder):
        '''
        command = "python calculate_use_bases_mask.py {runinfoFile} {sampleSheet} {lane}"
        gbsc_utils.createSubprocess(cmd=command)
        '''
        
        misc_subfolder = output_folder + '/miscellany'
        run_info_file = 'RunInfo.xml'

        command = 'python calculate_use_bases_mask.py %s ' % run_info_file
        command += '%s ' % self.sample_sheet
        command += '%d ' % int(self.lane_index)
        command += '%d' % int(self.bcl2fastq_version)

        stdout,stderr = self.createSubprocess(cmd=command, pipeStdout=True)
        self.use_bases_mask = stdout
        print 'This is use_bases_mask value: %s' % self.use_bases_mask

        use_bases_mask_file = 'use_bases_mask.txt'
        with open(use_bases_mask_file, 'w') as OUT:
            OUT.write(self.use_bases_mask)
        dxpy.upload_local_file(filename = use_bases_mask_file, 
                               properties = None, 
                               project = self.lane_project_id, 
                               folder = misc_subfolder, 
                               parents = True)
        return self.use_bases_mask

    def get_lane_barcode(self):
        run_params_file = 'runParameters.xml'
        if not os.path.isfile(run_params_file):
            print 'Error: Could not find %s' % run_params_file
            sys.exit()
        with open(run_params_file, 'r') as PARAM:
            for line in PARAM:
                match = re.search(r'<Barcode>([\w-]+)</Barcode>', line)
                if match:
                    lane_barcode = match.group(1)
                    return lane_barcode
        print 'Error: Could not determine lane barcode from %s' % run_params_file
        sys.exit()

    def run_bcl2fastq(self, mismatches, ignore_missing_stats, ignore_missing_bcl, 
                      ignore_missing_positions, ignore_missing_filter, with_failed_reads, 
                      tiles, test_mode, tools_used):
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

        ## DEV : set all --ignore flags to on by default (consistent with existing practices)

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
            if test_mode and tiles:
                command += '--tiles %d ' % tiles
            if with_failed_reads:
                command += '--with-failed-reads '
            if ignore_missing_bcl:
                command += '--ignore-missing-bcls '
            if ignore_missing_positions:
                command += '--ignore-missing-positions '
            if ignore_missing_filter:
                command += '--ignore-missing-filter '

            print 'Running bcl2fastq v2 with command:'
            print command

            tools_used['commands'].append(command)
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
            opts += " --fastq-cluster-count 0"  # Output it all in one file
            if ignore_missing_bcl:
                ignore_missing_bcl = True
            if mismatches:
                opts += " --mismatches " + str(mismatches)
            if ignore_missing_stats:
                opts += " --ignore-missing-stats"
            if ignore_missing_bcl:
                opts += " --ignore-missing-bcl"
            if  with_failed_reads:
                opts += " --with-failed-reads"
            if test_mode and tiles:
                opts += " --tiles " + str(tiles)

            # Run it
            print 'Running bcl2fastq v1 with options:'
            print opts
            command_1 = configure_script_path + " " + opts
            command_2 = "nohup make -C " + self.output_dir + " -j `nproc`"
            self.createSubprocess(cmd=configure_script_path + " " + opts, checkRetcode=True, pipeStdout=False)
            self.createSubprocess(cmd="nohup make -C " + self.output_dir + " -j `nproc`",checkRetcode=True,pipeStdout=False)
            
            tools_used['commands'].append(command_1)
            tools_used['commands'].append(command_2)
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
        Function: Creates a subprocess via a call to subprocess.Popen with the argument 
                  'shell=True', and pipes stdout and stderr. Stderr is always  piped, but 
                  stdout can be turned off. If the argument checkRetcode is True, which 
                  it is by default, then for any non-zero return code, an Exception is
                  raised that will print out the the command, stdout, stderr, and the 
                  returncode when not caught. Otherwise, the Popen instance will be 
                  return, in which case the caller must call the instance's communicate()
                  method (and not it's wait() method!) in order to get the return code to 
                  see if the command was a success. communicate() will return a tuple 
                  containing (stdout, stderr). But at that point, you can then check 
                  the return code with Popen instance's 'returncode' attribute.
        Args    : cmd (str): Command line argument for the subprocess wrapped in the 
                    subprocess.Popen instance. Will be printed to stdout when 
                    there is an error in the subprocess.
                  pipeStdout (bool): If 'True' pipe stdout of the subprocess.
                  checkRetcode (bool): See documentation above.
        Returns : A two-item tuple containing stdout and stderr, respectively.
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

        DEV: Look in gbsc_utils/illumina/fastq_file_name.py;
        '''

        elements = fastq_filename.split('_')
        if len(elements) < 4 or len(elements) > 6:
            print 'WARNING: fastq filename has unusual number of elements : %s' % fastq
            sys.exit()
        
        # One barcode : lane1_TAGGCATG_L001_R2_001.fastq.gz
        # Two barcodes : lane1_TCTCGCGC-TCAGAGCC_L001_R2_001.fastq.gz
        elif len(elements) == 5:
            lane = elements[0]
            read = elements[3]
            barcode = elements[1]

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
            # new format : 151202_BRISCOE_0270_BC847TACXX_L1_TAGGCATG_R1_pf.fastq.gz
            new_fastq_filename = '%s_L%s_%s_R%s_pf.fastq.gz' % (self.run_name, lane_index, barcode, read_index)
        
        # No barcode : Undetermined_L001_R1_001.fastq.gz
        elif len(elements) == 4:
            lane = elements[1]
            read = elements[2]
            barcode = 'unmatched'

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
            # new format : 151106_LYNLEY_0515_AC7F31ACXX_L1_unmatched_R1_pf.fastq.gz
            new_fastq_filename = '%s_L%s_%s_R%s_pf.fastq.gz' % (self.run_name, lane_index, barcode, read_index)
        
        else:
            print "Could not get metadata for:\nfastq: %s\nlane: %s\nrun: %s" % (fastq, lane, self.run_name)
            pass
        return (new_fastq_filename, barcode, read_index)

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
            barcode = '%s-%s' % (barcodes[0], barcodes[1])
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
            new_fastq_filename = '%s_L%s_%s_R%s_pf.fastq.gz' % (self.run_name, lane_index, barcode, read_index) 
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

            # new format : 151202_BRISCOE_0270_BC847TACXX_L1_TAGGCATG_R1_pf.fastq.gz
            new_fastq_filename = '%s_L%s_%s_R%s_pf.fastq.gz' % (self.run_name, lane_index, barcode, read_index)
        elif len(elements) == 5:
            # No barcode : Undetermined_S1_L001_R1_001.fastq.gz
            lane = elements[2]
            read = elements[3]
            barcode = 'unmatched'

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
            
            # new format : 151106_LYNLEY_0515_AC7F31ACXX_L1_unmatched_R1_pf.fastq.gz
            new_fastq_filename = '%s_L%s_%s_R%s_pf.fastq.gz' % (self.run_name, lane_index, barcode, read_index)
        else:
            print "Could not get metadata for:\nfastq: %s\nlane: %s\nrun: %s" % (fastq, lane, self.run_name)
            pass
        return (new_fastq_filename, barcode, read_index)

@dxpy.entry_point("main")
def main(**applet_input):
    ''' Description: Use illumina bcl2fastq applet to perform demultiplex and 
    convert bcl files to fastq files. Currently handles files generated from
    RTA version 2.7.3 and earlier.

    Input:
    applet_input (dictionary): Input parameters specified when calling applet 
                               from DNAnexus
    '''

    params = InputParameters(applet_input)
    tools_used_dict = {'name': 'Bcl to Fastq Conversion and Demultiplexing', 'commands': []}

    lane = FlowcellLane(record_id=params.record_id)
    lane.describe()
    
    print 'Downloading lane data'
    lane.unpack_tar(params.lane_data_tar)
    lane.unpack_tar(params.metadata_tar)
    
    print 'Creating sample sheet\n'
    lane.create_sample_sheet(params.output_folder)

    print 'Parsing sample sheet to get flowcell ID'
    lane.get_flowcell_id()
    
    print 'Get use bases mask\n'
    lane.get_use_bases_mask(params.output_folder)
    
    print 'Convert bcl to fastq files'
    lane.run_bcl2fastq(mismatches = params.mismatches,
                       ignore_missing_stats = params.ignore_missing_stats,
                       ignore_missing_bcl = params.ignore_missing_bcl,
                       with_failed_reads = params.with_failed_reads,
                       ignore_missing_positions = params.ignore_missing_positions,
                       ignore_missing_filter = params.ignore_missing_filter,
                       tiles = params.tiles,
                       test_mode = params.test_mode,
                       tools_used = tools_used_dict
                       )
    
    print 'Uploading fastq files back to DNAnexus'
    upload_output = lane.upload_result_files(params.output_folder)        # returns DXLink objects
    #print EMPTY_DEBUG_VARIABLE

    # Create tools used file
    tools_used_file = 'bcl2fastq_tools_used.json'
    with open(tools_used_file, 'w') as TOOLS:
        TOOLS.write(json.dumps(tools_used_dict))
    misc_subfolder = params.output_folder + '/miscellany'
    tools_used_id = dxpy.upload_local_file(filename = tools_used_file, 
                           properties = None, 
                           project = lane.lane_project_id, 
                           folder = misc_subfolder, 
                           parents = True
                          )

    output = {}
    output['fastqs'] = upload_output['fastqs']
    output['lane_html'] = upload_output['lane_html']
    output['tools_used'] = dxpy.dxlink(tools_used_id)

    print 'Output'
    print output
    return output

dxpy.run()
