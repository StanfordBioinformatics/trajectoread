#!/usr/bin/env python

''' Description: Workflow Manager
    1. Get run and lane info from LIMS using scgpm_lims
    5. Create dashboard record populated with information from LIMS
    6. Choose workflow based on mapping or not mapping
    7. Configure 'workflow_input'
    8. Call 'DXWorkflow.run(workflow_input={**input})
    8. Update record status to 'pipeline_running'
'''

import os
import pdb
import sys
import dxpy
import json
import time
import argparse

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'..'))
from scgpm_lims import Connection
from scgpm_lims import RunInfo

class LaneAnalysis:

    def __init__(self, run_name, lane_index, project_id, rta_version, lims_url, lims_token, 
                 release=False, test_mode=False):
        self.run_name = run_name
        self.project_id = project_id
        self.lane_index = lane_index
        self.rta_version = rta_version
        self.lims_url = lims_url
        self.lims_token = lims_token
        self.release = release
        self.test_mode = test_mode

        # Workflow variables
        self.workflow_name = None
        self.workflow_id = None
        self.workflow_project_id = None
        self.workflow_json_file = None
        self.workflow_inputs = None
        self.workflow_object = None

        self.analysis_input = None

        self.record_id = None
        self.dashboard_project_id = None
    
        self.metadata_tar_id = None
        self.interop_tar_id = None
        self.lane_tar_id = None

        self.connection = Connection(lims_url=lims_url, lims_token=lims_token)
        self.run_info = RunInfo(conn=self.connection, run=run_name)
        self.lane_info = self.run_info.get_lane(self.lane_index)

        # Bcl2fastq & demultiplexing variables
        self.barcode_mismatches = int(1)

        # Mapping variables
        try:
            self.mapper = self.lane_info['mapping_requests'][0]['mapping_program']
            if self.mapper == 'bwa':
                # Update March 1, 2016: Reverting back to bwa_aln since there is an issue reporting
                # unique vs non-unique reads with bwa-mem
                # Currently no option for users; only LIMS option we use is "bwa".
                # Defaulting to BWA_MEM
                # self.mapper = 'bwa_mem'
                self.mapper = 'bwa_aln' # Changed to bwa_aln since qc_sample only accepts 'bwa_mem' or 'bwa_aln'
                self.map_mismatches = self.lane_info['mapping_requests'][0]['max_mismatches']
                self.reference_genome = self.lane_info['mapping_requests'][0]['reference_sequence_name']
        except:
            print 'Warning: No mapping information found for %s' % self.run_name
            self.mapper = None
            self.map_mismatches = None
            self.reference_genome = None
    
        self.reference_genome_id = None
        self.reference_index_id = None
        if self.reference_genome:
            self.get_reference_ids()

        self.get_lane_input_files()
        
        self.workflow_inputs = {
                                'lane_data_tar': self.lane_tar_id,
                                'metadata_tar': self.metadata_tar_id,
                                'interop_tar': self.interop_tar_id,
                                'record_id': self.record_id,
                                'test_mode': self.test_mode,
                                'mismatches': self.barcode_mismatches,
                                'paired_end': self.run_info.data['paired_end']
        }
        

    def create_dxrecord(self):
        details = self._set_record_details()
        properties = self._set_record_properties()
        
        record_generator = dxpy.find_data_objects(classname = 'record', 
                              name = '%s_L%d' % (self.run_name, self.lane_index),
                              name_mode = 'exact',
                              project = self.dashboard_project_id,
                              folder = '/'
                             )
        records = list(record_generator)
        if len(records) > 0:
            self.record_id = records[0]['id']
        else:
            self.record_id = dxpy.api.record_new(input_params={
                                                        "project": self.dashboard_project_id,
                                                            "name": '%s_L%d' % (self.run_name, self.lane_index),
                                                            "types": ["SCGPMRun"],
                                                            "properties": properties,
                                                            "details": details
                                                           }
                                              )['id']
        dxpy.api.record_close(self.record_id)
    
    def choose_workflow(self, environment_json):
        with open(environment_json) as JSON:
            dnanexus_environment = json.load(JSON)

        # Determine appropriate workflow based on required operations
        operations = ['bcl2fastq', 'qc']    # Default operations for all analyses
        if self.reference_genome_id and self.reference_index_id:
            operations.append('bwa')
        if self.release:
            operations.append('release')

        for workflow in dnanexus_environment['production_workflows']:
            if set(operations) == set(workflow['operations']):
                self.workflow_name = workflow
                self.workflow_id = workflow['id']
                self.workflow_project_id = workflow['project_id']
                self.workflow_json_file = workflow['json_file']

    def configure_analysis(self, workflow_config_dir):

        # Get workflow configuration from JSON file
        workflow_json_path = os.path.join(workflow_config_dir, self.workflow_json_file)
        with open(workflow_json_path, 'r') as JSON:
            workflow_json = json.load(JSON)

        # Set workflow inputs
        self.analysis_input = {}
        for stage in workflow_json['stages']:
            for entry in stage['input']:
                key = '%d.%s' % (int(stage), entry)
                if stage['input'][entry][0] == '$':
                    # Value is a variable defined in workflow_inputs dict
                    elements = stage['input'][entry].split('-')
                    if elements[0] == '$dnanexus_link':
                        # Value needs to be of type dnanexus_link
                        variable = elements[1]
                        value = {'$dnanexus_link': self.workflow_inputs[variable]}
                    else:
                        # Get value directly from workflow_inputs dict
                        variable = stage['input'][entry][1:]
                        value = self.workflow_inputs[variable]
                else:
                    # Value is static and pre-defined in JSON file
                    static = stage['input'][entry]
                    value = static

                self.analysis_input[key] = value

    def configure_workflow(self):
        ## DEV: Will be deprecated in 1.05
        self.get_lane_input_files() 
        if self.release and self.reference_genome_id and self.reference_index_id:
            workflow_project_id = 'project-BqkQKbj0kjqjqj6bg8fp6pGk'  # 'WF_bcl2fastq_bwa_qc_release'
            workflow_name = 'WF_bcl2fastq_bwa_qc_release'

            self.workflow_input = {
                                   '0.output_folder': '/stage0_bcl2fastq',
                                   '0.lane_data_tar':{'$dnanexus_link': self.lane_tar_id}, 
                                   '0.metadata_tar':{'$dnanexus_link': self.metadata_tar_id}, 
                                   '0.record_id': self.record_id, 
                                   '0.test_mode': self.test_mode,     # Where to get this info?
                                   '0.mismatches': int(self.barcode_mismatches),
                                   '1.output_folder': '/stage1_bwa',
                                   '1.record_id': self.record_id,
                                   '2.output_folder': '/stage2_qc',
                                   '2.record_id': self.record_id,
                                   '3.output_folder': '/stage3_qc_report',
                                   '3.interop_tar': {'$dnanexus_link': self.interop_tar_id},
                                   '3.paired_end': self.run_info.data['paired_end'],
                                   '3.record_id': self.record_id,
                                   '4.record_id': self.record_id # release_lane applet
                                  }
        elif self.release and not self.reference_genome_id and not self.reference_index_id:
            workflow_project_id = 'project-Bv6gJv00vzgGZx4zZ5KKzJj0'  # 'WF_bcl2fastq_qc_release'
            workflow_name = 'WF_bcl2fastq_qc_release'

            self.workflow_input = {
                                   '0.output_folder': '/stage0_bcl2fastq',
                                   '0.lane_data_tar':{'$dnanexus_link': self.lane_tar_id}, 
                                   '0.metadata_tar':{'$dnanexus_link': self.metadata_tar_id}, 
                                   '0.record_id': self.record_id, 
                                   '0.test_mode': self.test_mode,
                                   '0.mismatches': int(self.barcode_mismatches),
                                   '1.output_folder': '/stage1_qc',
                                   '1.record_id': self.record_id,
                                   '2.output_folder': '/stage2_qc_report',
                                   '2.interop_tar': {'$dnanexus_link': self.interop_tar_id},
                                   '2.paired_end': self.run_info.data['paired_end'],
                                   '2.record_id': self.record_id,
                                  }
        elif self.reference_genome_id and self.reference_index_id:
            workflow_project_id = 'project-BpvKBv80ZgQJg4Y8ZQ0z3Z6f'  # 'WF_bcl2fastq_bwa_qc'
            workflow_name = 'WF_bcl2fastq_bwa_qc'

            self.workflow_input = {
                  '0.output_folder': '/stage0_bcl2fastq',
                  '0.lane_data_tar':{'$dnanexus_link': self.lane_tar_id}, 
                              '0.metadata_tar':{'$dnanexus_link': self.metadata_tar_id}, 
                              '0.record_id': self.record_id, 
                              '0.test_mode': self.test_mode,     # Where to get this info?
                              '0.mismatches': int(self.barcode_mismatches),
                              '1.output_folder': '/stage1_bwa',
                              '1.record_id': self.record_id,
                  '2.output_folder': '/stage2_qc',
                  '2.record_id': self.record_id,
                  '3.output_folder': '/stage3_qc_report',
                  '3.interop_tar': {'$dnanexus_link': self.interop_tar_id},
                  '3.paired_end': self.run_info.data['paired_end'],
                  '3.record_id': self.record_id
                             }
        elif not self.reference_genome_id and not self.reference_index_id:
            workflow_project_id = 'project-Bpv3PZQ0KY5P9vk59kg639jf'  # 'WF_bcl2fastq_qc'
            workflow_name = 'WF_bcl2fastq_qc'

            self.workflow_input = {
                  '0.output_folder': '/stage0_bcl2fastq',
                  '0.lane_data_tar':{'$dnanexus_link': self.lane_tar_id}, 
                              '0.metadata_tar':{'$dnanexus_link': self.metadata_tar_id}, 
                              '0.record_id': self.record_id, 
                              '0.test_mode': self.test_mode,
                  '0.mismatches': int(self.barcode_mismatches),
                  '1.output_folder': '/stage1_qc',
                              '1.record_id': self.record_id,
                  '2.output_folder': '/stage2_qc_report',
                              '2.interop_tar': {'$dnanexus_link': self.interop_tar_id},
                  '2.paired_end': self.run_info.data['paired_end'],
                  '2.record_id': self.record_id
                             }

        else:
            print 'Could not determine correct workflow'
            sys.exit()

        # Choose most recent version of workflow from project
        print 'Info: Configuring workflow: %s' % workflow_name
        self.workflow_id = dxpy.find_one_data_object(classname = 'workflow', 
                                             name = workflow_name,
                                             name_mode = 'exact',
                                             project = workflow_project_id,
                                             folder = '/',
                                             more_ok = True,
                                             zero_ok = False
                                            )['id']

    def get_reference_ids(self):
        reference_genome_project = 'project-BJJ0GQQ09Vv5Q7GKYGzQ0066'
        self.reference_genome_id = dxpy.find_one_data_object(classname='file',
                                                             name='genome.fa.gz',
                                                             name_mode='exact',
                                                             project = reference_genome_project,
                                                             folder = '/%s' % self.reference_genome,
                                                             zero_ok = False,
                                                             more_ok = False
                                                             )['id']
        self.reference_index_id = dxpy.find_one_data_object(classname='file',
                                                            name='bwa_index.tar.gz',
                                                            name_mode='exact',
                                                            project = reference_genome_project,
                                                            folder = '/%s' % self.reference_genome,
                                                            zero_ok = False,
                                                            more_ok = False
                                                            )['id']

    def get_lane_input_files(self):
        
        metadata_tar = '%s.metadata.tar*' % self.run_name
        self.metadata_tar_id = dxpy.find_one_data_object(classname = 'file',
                                                  name = metadata_tar,
                                                  name_mode = 'glob',
                                                  project = self.project_id,
                                                  folder = '/raw_data',
                                                  zero_ok = False,
                                                  more_ok = True
                                                 )['id']
        lane_tar = '%s_L%d.tar*' % (self.run_name, self.lane_index)
        self.lane_tar_id = dxpy.find_one_data_object(classname = 'file',
                                                  name = lane_tar,
                                                  name_mode = 'glob',
                                                  project = self.project_id,
                                                  folder = '/raw_data',
                                                  zero_ok = False,
                                                  more_ok = True
                                                 )['id']
        interop_tar = '%s.InterOp.tar*' % (self.run_name)
        self.interop_tar_id = dxpy.find_one_data_object(classname = 'file',
                                                  name = interop_tar,
                                                  name_mode = 'glob',
                                                  project = self.project_id,
                                                  folder = '/raw_data',
                                                  zero_ok = False,
                                                  more_ok = True
                                                 )['id']

    def run_analysis(self):
        self.record = dxpy.DXRecord(id=self.record_id, project=self.dashboard_project_id)
        properties = self.record.get_properties()
        if not 'analysis_started' in properties.keys():
            print 'Warning: Could not determine whether or not analysis had been started'
            dxpy.set_workspace_id(id=self.project_id)
            self.workflow_object = dxpy.DXWorkflow(id=self.workflow_id)
            print 'Launching workflow %s with input: %s' % (self.workflow_object.describe()['id'], 
                                                             self.analysis_input)
            self.workflow_object.run(workflow_input=self.analysis_input, 
                                     project=self.project_id, 
                                     folder='/')
            self.record.set_properties({'analysis_started': 'true'})
        elif properties['analysis_started'] == 'true':
            print 'Info: Analysis has already been started; skipping.'
            pass
        elif properties['analysis_started'] == 'false':
            dxpy.set_workspace_id(id=self.project_id)
            self.workflow_object = dxpy.DXWorkflow(id=self.workflow_id)
            print 'Launching workflow %s with input: %s' % (self.workflow_object.describe()['id'], 
                                                            self.analysis_input)
            self.workflow_object.run(workflow_input=self.workflow_input, 
                                     project=self.project_id, 
                                     folder='/')
            self.record.set_properties({'analysis_started': 'true'})

            # Create new pipeline run in LIMS
            if self.lane_index == 1:
                param_dict = {'started': True}
                json = self.connection.createpipelinerun(self.run_name, param_dict)
                self.record.set_properties({'pipeline_id': str(json['id'])})
                print 'Info: Created new LIMS pipeline run %s' % str(json['id'])

        def _set_record_details(self): 
        
        details = {
                   'email': str(self.lane_info['submitter_email']), 
                   'lane': str(self.lane_index), 
                   'laneProject': str(self.project_id),
                   'lane_id': str(self.lane_info['id']),
                   'library': str(self.lane_info['sample_name']),
                   'library_id': str(self.lane_info['dna_library_id']),
                   'mappingReference': str(self.reference_genome),
                   'run': str(self.run_name),
                   'uploadDate': str(int(round(time.time() * 1000))),
                   'user': str(self.lane_info['submitter'])
                  }
        return details

    def _set_record_properties(self):
        
        if self.run_info.data['paired_end'] == True:
            paired_end = 'true'
        else:
            paired_end = 'false'

            properties = {
                          'mapper': str(self.mapper),
                          'mismatches': str(self.map_mismatches),
                          'flowcell_id': str(self.run_info.data['flow_cell_id']),
                          'seq_instrument': str(self.run_info.data['sequencing_instrument']),
                          'lane_project_id': str(self.project_id),
                          'lab_name': str(self.lane_info['lab']),
                          'lims_token': str(self.lims_token),
                          'lims_url': str(self.lims_url),
                          'rta_version': str(self.rta_version),
                          'paired_end': paired_end,
                          'analysis_started': 'false',
                          'status': 'running_pipeline',
                          'library_id': str(self.lane_info['dna_library_id']),
                          'lane_id': str(self.lane_info['id']),
                          'billing_account': <REFERENCE>,
                          'experiment_type': <REFERENCE>,
                          'organism': <REFERENCE>,
                          'sample_volume': <REFERENCE>,
                          'average_molecule_size': <REFERENCE>,
                          'template_concentration_1': <REFERENCE>,
                          'template_concentration_2': <REFERENCE>,
                          'template_concentration_3': <REFERENCE> 
                         }

            if self.mapper:
                self.get_reference_ids()
                properties['reference_genome_id'] = self.reference_genome_id
                properties['reference_index_id'] = self.reference_index_id
            
            return properties

def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--run-name', dest='run_name', type=str, 
                        help='Name of sequencing run', required=True)
    parser.add_argument('-l', '--lane-index', dest='lane_index', type=str,
                        help='Indes of flowcell lane (1-8)', required=True)
    parser.add_argument('-p', '--project_id', dest='project_id', type=str,
                        help='Lane project id', required=True)
    parser.add_argument('-r', '--rta-version', dest='rta_version', type=str,
                        help='Version of illumina RTA software used', required=True)
    parser.add_argument('-e', '--release', dest='release', default=False, action='store_true', 
                        help='Automatically release DNAnexus projects to user', required=False)
    parser.add_argument('-t', '--test', dest='test_mode', type=str,
                        help='Only use one tile for analyses', required=True)
    args = parser.parse_args()
    return args

def main():

    args = parse_args()
    print 'Info: Initiating analysis for %s lane %d' % (args.run_name, int(args.lane_index))
    print args
    ## Dev: This needs to be changed. What is this.
    if args.test_mode == 'True': 
        test_mode = True
    else:
        test_mode = False

    # Load DNAnexus environment file
    help_dir = os.path.dirname(os.path.abspath(__file__))
    scripts_dir = os.path.split(help_dir)[0]
    home = os.path.split(scripts_dir)[0]
    
    environment_json = os.path.join(home, "dnanexus_environment.json")
    workflow_config_dir = os.path.join(home, "workflow_config_templates")

    lane_analysis = LaneAnalysis(run_name = args.run_name, 
                                 lane_index = int(args.lane_index), 
                                 project_id = args.project_id, 
                                 rta_version = args.rta_version, 
                                 lims_url = args.lims_url, 
                                 lims_token = args.lims_token,
                                 release = args.release, 
                                 test_mode = test_mode
                                 )
    #pdb.set_trace()
    print 'Info: Creating Dashboard record'
    lane_analysis.create_dxrecord()
    print 'Info: Choosing Workflow'
    lane_analysis.choose_workflow(environment_json)
    print 'Info: Configure Analysis'
    lane_analysis.configure_analysis(workflow_config_dir)
    print 'Info: Launching analysis'
    lane_analysis.run_analysis()

if __name__ == '__main__':
    main()

