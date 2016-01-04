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

## Inserting line to test whether I can grab commit data to label applets

'''

import dxpy
import subprocess
import os
import datetime
import time
import fnmatch

# Fx test 1: Create Flowcell class that is able to populate metadata from 
# dashboard record.

class FlowcellLane:
    
    def __init__(self, dashboard_record_dxid, dashboard_project_dxid='project-BY82j6Q0jJxgg986V16FQzjx'):
        
        self.dashboard_record_dxid = dashboard_record_dxid
        self.dashboard_project_dxid = dashboard_project_dxid
        self.dashboard_record = dxpy.DXRecord(dxid = self.dashboard_record_dxid,project = self.dashboard_project_dxid)
        self.properties = self.dashboard_record.get_properties()

        # For not just get/put everything in properties
        self.lane_project_dxid = self.properties['lane_project_dxid']
        self.run_name = self.properties['run']
        self.lane_index = int(self.properties['lane'])
        self.lims_url = self.properties['lims_url']
        self.lims_token = self.properties['lims_token']
        self.bcl2fastq_version = int(self.properties['bcl2fastq_version'])

        self.lane_project = dxpy.DXProject(dxid = self.lane_project_dxid)
        self.home = os.getcwd()

        self.sample_sheet = None
        self.output_dir = None
        self.flowcell_id = None

        run_elements = self.run_name.split('_')
        flowcell_info = run_elements[3]
        self.flowcell_id = flowcell_info[1:6]

    def describe(self):
        print "Sequencing run: %s" % self.run_name
        print "Flowcell lane index: %s" % self.lane_index

    def unpack_data(self):
        '''
        Download and untar metadata and lane data files (/Data/Intensities/BaseCalls)
        '''

        metadata_tar_file = '%s.metadata.tar' % (self.run_name)
        data_tar_file = '%s.L%d.tar' % (self.run_name, self.lane_index)

        # Find lane tar files on DNAnexus
        metadata_dict = dxpy.find_one_data_object(name=metadata_tar_file, project=self.lane_project_dxid, folder='/', more_ok=False, zero_ok=False)
        data_dict = dxpy.find_one_data_object(name=data_tar_file, project=self.lane_project_dxid, folder='/', more_ok=False, zero_ok=False)

        metadata_dxid = metadata_dict['id']
        data_dxid = data_dict['id']

        # Download files from DNAnexus objectstore to virtual machine
        dxpy.download_dxfile(dxid=metadata_dxid, filename=metadata_tar_file, project=self.lane_project_dxid)
        dxpy.download_dxfile(dxid=data_dxid, filename=data_tar_file, project=self.lane_project_dxid)

        # Untar files to recreate illumina data/directory structure
        command = 'tar -xf %s' % metadata_tar_file
        self.createSubprocess(cmd=command, pipeStdout=False)

        command = 'tar -xf %s' % data_tar_file
        self.createSubprocess(cmd=command, pipeStdout=False)

    def upload_result_files(self):

        # Dont tar files before uploading
        #fastq_tar_file = 'lane%d.fastq.tar' % self.lane_index
        #command = 'tar -cf %s %s' % (fastq_tar_file, self.output_dir)
        #self.createSubprocess(cmd=command, pipeStdout=False)
        #dxpy.upload_local_file(filename=fastq_tar_file, properties=None, project=self.lane_project_dxid, folder='/', parents=True)

        lane_dir = self.home + '/Unaligned_L' + str(self.lane_index)
        flowcell_dir = lane_dir + '/' + self.flowcell_id
        
        # Upload all the fastq files from the lane directory (Unaligned_L%d)
        os.chdir(lane_dir)
        for file in os.listdir('.'):
            if fnmatch.fnmatch(file, '*.fastq.gz'):
                dxpy.upload_local_file(filename=file, properties=None, project=self.lane_project_dxid, folder='/', parents=True)

        # Upload all the fastq files from the flowcell directory (Unaligned_L%d/<flowcell_id>)
        os.chdir(flowcell_dir)
        for file in os.listdir('.'):
            if fnmatch.fnmatch(file, '*.fastq.gz'):
                dxpy.upload_local_file(filename=file, properties=None, project=self.lane_project_dxid, folder='/', parents=True)

        # Don't need to upload all the files in the reports directory. We make our own.
    def create_sample_sheet(self):
        '''
        create_sample_sheet.py -r ${seq_run_name} \
            -t ${UHTS_LIMS_TOKEN} \
            -u ${UHTS_LIMS_URL} \
            -b 2 \
            -l ${SGE_TASK_ID}
        '''

        ## Create samplesheet
        command = 'python create_sample_sheet.py -r %s -t %s -u %s -b %d -l %s' % (self.run_name, self.lims_token, self.lims_url, self.bcl2fastq_version, self.lane_index)
        stdout,stderr = self.createSubprocess(cmd=command, pipeStdout=True)
        self.sample_sheet = '%s_L%d_samplesheet.csv' % (self.run_name, self.lane_index)
        stdout_elements = stdout.split()
        self.sample_sheet = stdout_elements[1]
        print 'This is the self.sample_sheet: %s' % self.sample_sheet
        #sample_sheet_name = os.path.basename(self.sample_sheet)
        dxpy.upload_local_file(filename=self.sample_sheet, properties=None, project=self.lane_project_dxid, folder='/', parents=True)
        #return self.sample_sheet
        
    def get_use_bases_mask(self):
        '''
        command = "python calculate_use_bases_mask.py {runinfoFile} {sampleSheet} {lane}"
        gbsc_utils.createSubprocess(cmd=command)
        '''
        run_info_file = 'RunInfo.xml'
        dxpy.download_dxfile(dxid='file-Bkv0yBQ0gq319q3bfx5B9Bzj', filename=run_info_file, project=self.lane_project_dxid)

        command = 'python calculate_use_bases_mask.py %s %s %s' % (
            run_info_file, self.sample_sheet, self.lane_index)
        stdout,stderr = self.createSubprocess(cmd=command, pipeStdout=True)
        self.use_bases_mask = stdout
        print 'This is use_bases_mask value: %s' % self.use_bases_mask

        use_bases_mask_file = 'use_bases_mask.txt'
        with open(use_bases_mask_file, 'w') as OUT:
            OUT.write(self.use_bases_mask)
        dxpy.upload_local_file(filename=use_bases_mask_file, properties=None, project=self.lane_project_dxid, folder='/', parents=True)
        return self.use_bases_mask

    def run_bcl2fastq(self):
        '''
        bcl2fastq --output-dir ${new_run_dir}/${seq_run_name}/Unaligned_L${SGE_TASK_ID} \
            --sample-sheet ${new_run_dir}/${seq_run_name}/${seq_run_name}_L${SGE_TASK_ID}_samplesheet.csv \
            --ignore-missing-bcls \
            --ignore-missing-filter \
            --ignore-missing-positions \
            --barcode-mismatches 1 \
            --use-bases-mask ${SGE_TASK_ID}:Y*,n*,Y*
        '''

        self.output_dir = 'Unaligned_L%d' % self.lane_index
        command = 'bcl2fastq --output-dir %s --sample-sheet %s --barcode-mismatches %d --use-bases-mask %d:%s' % (
            self.output_dir, self.sample_sheet, 1, int(self.lane_index), self.use_bases_mask)
        stdout,stderr = self.createSubprocess(cmd=command, pipeStdout=True)

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

@dxpy.entry_point("main")
def main(record_id):
    lane = FlowcellLane(dashboard_record_dxid=record_id)
    lane.describe()
    print 'Downloading lane data'
    lane.unpack_data()
    print 'Creating sample sheet\n'
    sample_sheet = lane.create_sample_sheet()
    print 'Get use bases mask\n'
    use_bases_mask = lane.get_use_bases_mask()
    print 'Convert bcl to fastq files'
    lane.run_bcl2fastq()
    print 'Upload fastq files tarball back to DNAnexus'
    lane.upload_result_files()

dxpy.run()
