#!/usr/bin/python

''' Description: Workflow Manager
    1. Get run and lane info from LIMS using scgpm_lims
    5. Create dashboard record populated with information from LIMS
    6. Choose workflow based on mapping or not mapping
    7. Configure 'workflow_input'
    8. Call 'DXWorkflow.run(workflow_input={**input})
    8. Update record status to 'pipeline_running'
'''

import dxpy
import json
import time

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
        self.test_mode

        self.workflow_input = None
        self.workflow_object = None
        self.record_dxid = None

        self.metadata_tar_dxid = None
        self.interop_tar_dxid = None
        self.lane_tar_dxid = None

        connection = Connection(lims_url=lims_url, lims_token=lims_token)
        self.run_info = RunInfo(conn=connection, run=run_name)
        self.lane_info = run_info.get_lane(lane_index)

        # Mapping variables
        self.mapper = self.lane_info['mapping_requests'][0]['mapping_program']
        self.mismatches = self.lane_info['mapping_requests'][0]['max_mismatches']
        self.reference_genome = self.lane_info['mapping_requests'][0]['reference_sequence_name']
        self.reference_genome_dxid = None
        self.reference_index_dxid = None
        if self.reference_genome:
            self.get_reference_dxids()

    def create_dxrecord(self):
        dashboard_project_dxid = 'project-BY82j6Q0jJxgg986V16FQzjx'
        details = self._set_record_details()
        properties = self._set_record_properties()
        self.record_dxid = dxpy.api.record_new(input_params={
                                                        "project": dashboard_project_dxid,
                                                        "name": self.run_name,
                                                        "types": ["SCGPMRun"],
                                                        "properties": properties,
                                                        "details": details
                                                       }
                                          )['id']
        dxpy.api.record_close(record_dxid)
    
    def _set_record_details(self): 
        
        details = {
                   'email': self.lane_info['submitter_email'], 
                   'lane': self.lane_index, 
                   'lane_project': self.project_dxid,
                   'library': self.lane_info['sample_name'],
                   'mapping_reference': self.lane_info['mapping_requests'][0]['reference_sequence_name'],
                   'run': self.run_name,
                   'uploadDate': int(round(time.time() * 1000)) ,
                   'user': self.lane_info['submitter']
                  }
        return details

    def _set_record_properties(self):

        properties = {
                      'mapper': self.mapper,
                      'mismatches': self.mismatches
                      'flowcell_id': self.run_info['flow_cell_id'],
                      'lab_name': self.lane_info['lab'],
                      'lims_token': self.lims_token,
                      'lims_url': self.lims_url,
                      'rta_version': self.rta_version,
                      'status': 'running_pipeline' 
        }

        if mapping_reference:
            self.get_reference_dxids

            properties['reference_genome_dxid'] = self.reference_genome_dxid
            properties['reference_index_dxid'] = self.reference_index_dxid

    def configure_workflow(self):
        
        if self.reference_genome_dxid and self.reference_index_dxid:
            workflow_project_dxid = ''  # 'WF_bcl2fastq_bwa_qc'
            workflow_name = 'WF_bcl2fastq_bwa_qc'

            workflow_input = {'0.lane_data_tar':{'$dnanexus_link': self.lane_tar_dxid}, 
                              '0.metadata_tar':{'$dnanexus_link': self.metadata_tar_dxid}, 
                              '0.record_dxid': self.record_dxid, 
                              '0.test_mode': self.test_mode,     # Where to get this info?
                              '0.mismatches': 
                              '1.record_dxid': self.record_dxid
                             }


        elif not self.reference_genome_dxid and not self.reference_index_dxid:
            workflow_project_dxid = ''  # 'WF_bcl2fastq_qc'
            workflow_name = 'WF_bcl2fastq_qc'

            workflow_input = {'0.lane_data_tar':{'$dnanexus_link': self.lane_tar_dxid}, 
                              '0.metadata_tar':{'$dnanexus_link': self.metadata_tar_dxid}, 
                              '0.record_dxid': self.record_dxid, 
                              '0.test_mode': self.test_mode,
                              '1.record_dxid': self.record_dxid
                             }

        else:
            print 'Could not determine correct workflow'
            sys.exit()

        # Choose most recent version of workflow from project
        self.workflow_object = dxpy.find_one_data_object(classname='workflow', 
                                             name=workflow_name,
                                             project=workflow_project_dxid,
                                             folder='/',
                                             more_ok=True,
                                             zero_ok=False
                                            )

    def get_reference_dxids(self):
        reference_genome_project = 'project-BJJ0GQQ09Vv5Q7GKYGzQ0066'
        self.reference_genome_dxid = dxpy.find_one_data_object(classname='file',
                                                               name='genome.fa.gz',
                                                               name_mode='exact',
                                                               project = reference_genome_project,
                                                               folder = self.reference_genome,
                                                               zero_ok = False,
                                                               more_ok = False
                                                              )['id']
        self.reference_index_dxid = dxpy.find_one_data_object(classname='file',
                                                               name='bwa_index.tar.gz',
                                                               name_mode='exact',
                                                               project = reference_genome_project,
                                                               folder = self.reference_genome,
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

    def run(self):
        dxpy.set_workspace_id(dxid=self.project_dxid)
        self.workflow_object.run(self.workflow_input)


def main(run_name, lane_index, project_dxid, rta_version):

    lims_url = 'https://uhts.stanford.edu'
    lims_token = '9af4cc6d83fbfd793fe4'

    test_mode = False

    lane_analysis = LaneAnalysis(run_name, lane_index, project_dxid, rta_version, 
                                 lims_url, lims_token, test_mode)
    lane_analysis.create_dxrecord()
    lane_analysis.configure_workflow()
    lane_analysis.run_analysis()






