''' Workflow Manager
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

    def __init__(self, run_name, lane_index, project_dxid, rta_version, lims_url, lims_token):
        self.run_name = run_name
        self.project_dxid = project_dxid
        self.lane_index = lane_index
        self.rta_version = rta_version
        self.lims_url = lims_url
        self.lims_token = lims_token

        self.ref_info = ReferenceInfo()

        self.reference_genome_dxid = None
        self.reference_index_dxid = None

        self.workflow_input = None
        self.workflow_object = None

        connection = Connection(lims_url=lims_url, lims_token=lims_token)
        self.run_info = RunInfo(conn=connection, run=run_name)
        self.lane_info = run_info.get_lane(lane_index)

    def create_dxrecord(self):
        details = self.get_record_details()
        properties = self.get_record_properties()
        record_dxid = dxpy.api.record_new(input_params={
                                                        "project": "project-BY82j6Q0jJxgg986V16FQzjx",
                                                        "name": "151218_COOPER_0016_BH5HMTBBXX_L1",
                                                        "types": ["SCGPMRun"],
                                                        "properties": properties,
                                                        "details": details
                                                       }
                                          )['id']
        dxpy.api.record_close(record_dxid)
    
    def get_record_details(self): 
        
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

    def get_record_properties(self):

        properties = {
                      'mapper': self.lane_info['mapping_requests'][0]['mapping_program'],
                      'flowcell_id': self.run_info['flow_cell_id'],
                      'lab_name': self.lane_info['lab'],
                      'lims_token': self.lims_token,
                      'lims_url': self.lims_url,
                      'rta_version': self.rta_version,
                      'status': 'running_pipeline' 
        }

        if mapping_reference:
            self.reference_genome_dxid = self.ref_info.get_fasta(mapping_reference)
            self.reference_index_dxid = self.ref_info.get_index(mapping_reference)

            properties['reference_genome_dxid'] = self.reference_genome_dxid
            properties['reference_index_dxid'] = self.reference_index_dxid

    def configure_workflow(self):
        
        if self.reference_genome_dxid and self.reference_index_dxid:
            workflow_project_dxid = ''  # 'WF_bcl2fastq_bwa_qc'
            workflow_name = 'WF_bcl2fastq_bwa_qc'

            workflow_input = {'0.lane_data_tar':{'$dnanexus_link':'file-BpbpK4j0gkgVK9p9QZG9y7P6'}, 
                              '0.metadata_tar':{'$dnanexus_link':'file-Bpgp9280gkgVFQpZgj3bj4pv'}, 
                              '0.record_dxid':'record-BpbqyB00jJxQYF1g31bf13z3', 
                              '0.test_mode':True,
                              '1.record_id':'record-BpbqyB00jJxQYF1g31bf13z3'
                             }


        elif not self.reference_genome_dxid and not self.reference_index_dxid:
            workflow_project_dxid = ''  # 'WF_bcl2fastq_qc'
            workflow_name = 'WF_bcl2fastq_qc'

            workflow_input = {'0.lane_data_tar':{'$dnanexus_link':'file-BpbpK4j0gkgVK9p9QZG9y7P6'}, 
                              '0.metadata_tar':{'$dnanexus_link':'file-Bpgp9280gkgVFQpZgj3bj4pv'}, 
                              '0.record_dxid':'record-BpbqyB00jJxQYF1g31bf13z3', 
                              '0.test_mode':True,
                              '1.record_id':'record-BpbqyB00jJxQYF1g31bf13z3'
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

    def run(self):
        dxpy.set_workspace_id(dxid=self.project_dxid)
        self.workflow_object.run(self.workflow_input)


class ReferenceInfo:

    def __init__(self):

        self.ref_dict = {
                         'Human Male (Hg19)': {
                                               'fasta': <DXID>,
                                               'index': <DXID>
                                              }
                        }

    def get_fasta(self, reference_name):
        return(self.ref_dict[reference_name]['fasta'])

    def get_index(self, reference_name):
        return(self.ref_dict[reference_name]['index'])


def main(run_name, lane_index, project_dxid, rta_version):

    lims_url = 'https://uhts.stanford.edu'
    lims_token = '9af4cc6d83fbfd793fe4'

    lane_analysis = LaneAnalysis(run_name, lane_index, project_dxid, rta_version, 
                                 lims_url, lims_token)
    lane_analysis.create_dxrecord()
    lane_analysis.configure_workflow()
    lane_analysis.run_analysis()






