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

    def __init__(self, run_name, lane_index, project_dxid, rta_version, lims_url, lims_token, test_mode=False):
        self.run_name = run_name
        self.project_dxid = project_dxid
        self.lane_index = lane_index
        self.rta_version = rta_version
        self.lims_url = lims_url
        self.lims_token = lims_token
        self.test_mode = test_mode

        self.workflow_input = None
        self.workflow_object = None
        self.record_dxid = None
        self.dashboard_project_dxid = 'project-BY82j6Q0jJxgg986V16FQzjx'
    
        self.metadata_tar_dxid = None
        self.interop_tar_dxid = None
        self.lane_tar_dxid = None

        connection = Connection(lims_url=lims_url, lims_token=lims_token)
        self.run_info = RunInfo(conn=connection, run=run_name)
        self.lane_info = self.run_info.get_lane(self.lane_index)

       # Bcl2fastq & demultiplexing variables
       self.barcode_mismatches = 1

        # Mapping variables
        try:
            self.mapper = self.lane_info['mapping_requests'][0]['mapping_program']
            self.map_mismatches = self.lane_info['mapping_requests'][0]['max_mismatches']
            self.reference_genome = self.lane_info['mapping_requests'][0]['reference_sequence_name']
        except:
            print 'Warning: No mapping information found for %s' % self.run_name
            self.mapper = None
            self.map_mismatches = None
            self.reference_genome = None
    
        self.reference_genome_dxid = None
        self.reference_index_dxid = None
        if self.reference_genome:
            self.get_reference_dxids()
        

    def create_dxrecord(self):
        details = self._set_record_details()
        properties = self._set_record_properties()
        
    record_generator = dxpy.find_data_objects(classname = 'record', 
                          name = '%s_L%d' % (self.run_name, self.lane_index),
                          name_mode = 'exact',
                          project = self.dashboard_project_dxid,
                          folder = '/'
                         )
    records = list(record_generator)
    if len(records) > 0:
        self.record_dxid = records[0]['id']
    else:
        self.record_dxid = dxpy.api.record_new(input_params={
                                                    "project": self.dashboard_project_dxid,
                                                        "name": '%s_L%d' % (self.run_name, self.lane_index),
                                                        "types": ["SCGPMRun"],
                                                        "properties": properties,
                                                        "details": details
                                                       }
                                          )['id']
            dxpy.api.record_close(self.record_dxid)
    
    def _set_record_details(self): 
        
        details = {
                   'email': str(self.lane_info['submitter_email']), 
                   'lane': str(self.lane_index), 
                   'laneProject': str(self.project_dxid),
                   'library': str(self.lane_info['sample_name']),
                   'mappingReference': str(self.reference_genome),
                   'run': str(self.run_name),
                   'uploadDate': str(int(round(time.time() * 1000))),
                   'user': str(self.lane_info['submitter'])
                  }
        return details

    def _set_record_properties(self):

        properties = {
                      'mapper': str(self.mapper),
                      'mismatches': str(self.map_mismatches),
                      'flowcell_id': str(self.run_info.data['flow_cell_id']),
              'lane_project_dxid': str(self.project_dxid),
                      'lab_name': str(self.lane_info['lab']),
                      'lims_token': str(self.lims_token),
                      'lims_url': str(self.lims_url),
                      'rta_version': str(self.rta_version),
              'analysis_started': 'false',
                      'status': 'running_pipeline' 
                 }

        if self.mapper:
            self.get_reference_dxids()

            properties['reference_genome_dxid'] = self.reference_genome_dxid
            properties['reference_index_dxid'] = self.reference_index_dxid
    return properties

    def configure_workflow(self):
        self.get_lane_input_files() 
        if self.reference_genome_dxid and self.reference_index_dxid:
            workflow_project_dxid = 'project-BpvKBv80ZgQJg4Y8ZQ0z3Z6f'  # 'WF_bcl2fastq_bwa_qc'
            workflow_name = 'WF_bcl2fastq_bwa_qc'

            self.workflow_input = {
                  '0.output_folder': '/stage0_bcl2fastq',
                  '0.lane_data_tar':{'$dnanexus_link': self.lane_tar_dxid}, 
                              '0.metadata_tar':{'$dnanexus_link': self.metadata_tar_dxid}, 
                              '0.record_dxid': self.record_dxid, 
                              '0.test_mode': self.test_mode,     # Where to get this info?
                              '0.mismatches': int(self.barcode_mismatches),
                              '1.output_folder': '/stage1_bwa',
                              '1.record_dxid': self.record_dxid,
                  '2.output_folder': '/stage2_qc',
                  '2.record_dxid': self.record_dxid,
                              '2.interop_tar': {'$dnanexus_link': self.interop_tar_dxid}
                             }


        elif not self.reference_genome_dxid and not self.reference_index_dxid:
            workflow_project_dxid = 'project-Bpv3PZQ0KY5P9vk59kg639jf'  # 'WF_bcl2fastq_qc'
            workflow_name = 'WF_bcl2fastq_qc'

            self.workflow_input = {
                  '0.output_folder': '/stage0_bcl2fastq',
                  '0.lane_data_tar':{'$dnanexus_link': self.lane_tar_dxid}, 
                              '0.metadata_tar':{'$dnanexus_link': self.metadata_tar_dxid}, 
                              '0.record_dxid': self.record_dxid, 
                              '0.test_mode': self.test_mode,
                  '0.mismatches': int(self.barcode_mismatches),
                  '1.output_folder': '/stage1_qc',
                              '1.record_dxid': self.record_dxid,
                              '1.interop_tar': {'$dnanexus_link': self.interop_tar_dxid}
                             }

        else:
            print 'Could not determine correct workflow'
            sys.exit()

        # Choose most recent version of workflow from project
        self.workflow_dxid = dxpy.find_one_data_object(classname = 'workflow', 
                                             name = workflow_name,
                                             name_mode = 'exact',
                                             project = workflow_project_dxid,
                                             folder = '/',
                                             more_ok = True,
                                             zero_ok = False
                                            )['id']

    def get_reference_dxids(self):
        reference_genome_project = 'project-BJJ0GQQ09Vv5Q7GKYGzQ0066'
        self.reference_genome_dxid = dxpy.find_one_data_object(classname='file',
                                                               name='genome.fa.gz',
                                                               name_mode='exact',
                                                               project = reference_genome_project,
                                                               folder = '/%s' % self.reference_genome,
                                                               zero_ok = False,
                                                               more_ok = False
                                                              )['id']
        self.reference_index_dxid = dxpy.find_one_data_object(classname='file',
                                                               name='bwa_index.tar.gz',
                                                               name_mode='exact',
                                                               project = reference_genome_project,
                                                               folder = '/%s' % self.reference_genome,
                                                               zero_ok = False,
                                                               more_ok = False
                                                              )['id']

    def get_lane_input_files(self):
        
        metadata_tar = '%s.metadata.tar.gz' % self.run_name
        self.metadata_tar_dxid = dxpy.find_one_data_object(classname = 'file',
                                                  name = metadata_tar,
                                                  name_mode = 'exact',
                                                  project = self.project_dxid,
                                                  folder = '/raw_data',
                                                  zero_ok = False,
                                                  more_ok = True
                                                 )['id']
        lane_tar = '%s_L%d.tar.gz' % (self.run_name, self.lane_index)
        self.lane_tar_dxid = dxpy.find_one_data_object(classname = 'file',
                                                  name = lane_tar,
                                                  name_mode = 'exact',
                                                  project = self.project_dxid,
                                                  folder = '/raw_data',
                                                  zero_ok = False,
                                                  more_ok = True
                                                 )['id']
        interop_tar = '%s.InterOp.tar.gz' % (self.run_name)
        self.interop_tar_dxid = dxpy.find_one_data_object(classname = 'file',
                                                  name = interop_tar,
                                                  name_mode = 'exact',
                                                  project = self.project_dxid,
                                                  folder = '/raw_data',
                                                  zero_ok = False,
                                                  more_ok = True
                                                 )['id']

    def run_analysis(self):
    self.record = dxpy.DXRecord(dxid=self.record_dxid, project=self.dashboard_project_dxid)
        properties = self.record.get_properties()
    if not 'analysis_started' in properties.keys():
        print 'Warning: Could not determine whether or not analysis had been started'
        dxpy.set_workspace_id(dxid=self.project_dxid)
        self.workflow_object = dxpy.DXWorkflow(dxid=self.workflow_dxid)
        print 'Launching workflow %s with input: %s' % (self.workflow_object.describe()['id'], 
                                self.workflow_input)
        self.workflow_object.run(workflow_input=self.workflow_input, 
                     project=self.project_dxid, 
                     folder='/')
        self.record.set_properties({'analysis_started': 'true'})
    elif properties['analysis_started'] == 'true':
        print 'Info: Analysis has already been started; skipping.'
        pass
    elif properties['analysis_started'] == 'false':
        dxpy.set_workspace_id(dxid=self.project_dxid)
        self.workflow_object = dxpy.DXWorkflow(dxid=self.workflow_dxid)
        print 'Launching workflow %s with input: %s' % (self.workflow_object.describe()['id'], 
                                self.workflow_input)
        self.workflow_object.run(workflow_input=self.workflow_input, 
                     project=self.project_dxid, 
                     folder='/')
        self.record.set_properties({'analysis_started': 'true'})

def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--run-name', dest='run_name', type=str, 
                        help='Name of sequencing run', required=True)
    parser.add_argument('-l', '--lane-index', dest='lane_index', type=str,
                        help='Indes of flowcell lane (1-8)', required=True)
    parser.add_argument('-p', '--project_dxid', dest='project_dxid', type=str,
                        help='Lane project dxid', required=True)
    parser.add_argument('-r', '--rta-version', dest='rta_version', type=str,
                        help='Version of illumina RTA software used', required=True)
    parser.add_argument('-u', '--lims-url', dest='lims_url', type=str,
                        help='LIMS URL', required=True)
    parser.add_argument('-o', '--lims-token', dest='lims_token', type=str,
                        help='LIMS token', required=True)
    parser.add_argument('-t', '--test', dest='test_mode', type=str,
                        help='Only use one tile for analyses', required=True)
    args = parser.parse_args()
    return args

def main():

    args = parse_args()
    print 'Initiating analysis for %s lane %d' % (args.run_name, int(args.lane_index))
    if args.test_mode == 'True': 
    test_mode = True
    else:
    test_mode = False

    lane_analysis = LaneAnalysis(run_name = args.run_name, 
                 lane_index = int(args.lane_index), 
                 project_dxid = args.project_dxid, 
                                 rta_version = args.rta_version, 
                 lims_url = args.lims_url, 
                 lims_token = args.lims_token, 
                                 test_mode = test_mode)
    print 'Creating Dashboard record'
    lane_analysis.create_dxrecord()
    print 'Configuring Workflow'
    lane_analysis.configure_workflow()
    print 'Launching analysis'
    lane_analysis.run_analysis()

if __name__ == '__main__':
    main()

