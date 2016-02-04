''' Workflow Manager
    1. Find all DXRecords with 'status': 'uploading'
    2. Check whether 'upload_status': 'complete'
    3. If upload is complete:
        4. Choose appropriate DXWorkflow object
        5. Copy Workflow object into lane project
        6. Specify input files
        7. Update DXRecord with Workflow name and dxid
        8. Update 'status': 'ready'
'''

''' Cron job
    1. Find all DXRecords with 'status': 'ready'
    2. Get 'workflow_dxid'
    3. Start analysis
'''

import json

class Workflow:

    def __init__(self, workflow_elements):

        workflow_name = '_'.join(workflow_elements)
        
        workflow_dict = {}
        workflow_dict['WF_bcl2fastq_qc'] = '<project_dxid>'
        workflow_dict['WF_bcl2fastq_bwa_qc'] = '<project_dxid>'

    def get_current_wf_object():
        # Project DXIDs will be static but specific objects will change
        # Get list of all directories within 'builds'
        
        # Each time I create a new workflow, update 'external_resources' to 
        # include workflows

class Manager:

    def __init__(rsc_config_filename, rsc_project_dxid, dashboard_project_dxid, dx_os):
        self.rsc_config_filename = rsc_config_filename
        self.rsc_project_dxid = rsc_project_dxid
        self.dashboard_project_dxid = dashboard_project_dxid
        self.dx_os = dx_os

        self.workflow_dict = None
 
        rsc_config_dxlink = dxpy.find_data_objects(name = self.rsc_config_filename, 
                                                      project = self.rsc_project_dxid, 
                                                      folder = self.dx_os,
                                                  )

        # Load resources json file

        # Get specifically workflow objects out of JSON file - populate workflow dict


    def find_ready_lanes():
        lane_projects_ready = {}
        search_properties = {'status': 'ready'}
        records_ready = dxpy.find_one_data_object(classname = 'record',
                                                  project = self.dashboard_record_dxid,
                                                  properties = search_properties,
                                                 )
        for record_dxlink in records_ready:
            record = dxpy.DXRecord(project=record_dxlink['project'], 
                                   dxid=record_dxlink['id']
                                  )
            record_properties = record.get_properties()
            lane_project_dxid = record_properties['lane_project_dxid']
            lane_projects_ready[record] = lane_project_dxid
        return lane_projects_ready


    def assign_workflow(dx_record):
        # find correct workflow based on record properties
        
        workflow_elements = ['WF', 'bcl2fastq', 'bwa', 'qc']
        workflow_elements = ['WF', 'bcl2fastq', 'qc']

        if not mapper and not reference_genome:
            # Invoke basic workflow with bcl2fastq and qc
        if mapper and not reference_genome:
            # throw error
        if mapper and reference_genome:
            # Invoke mapping workflow with bcl2fastq-bwa-qc

    def configure_inputs(lane_dxid, workflow_dxid):


@dxpy.entry_point("main")
def main():

    rsc_config_filename = 'external_resources.json'
    rsc_project_dxid = 'project-BkYXb580p6fv2VF8bjPYp2Qv'
    dashboard_project_dxid = 'project-BY82j6Q0jJxgg986V16FQzjx'
    dx_os = 'Ubuntu-12.04'
    
    workflow_manager = Manager(rsc_config_filename, rsc_project_dxid, dashboard_project_dxid, dx_os)
    ready_lane_projects = workflow_manager.find_ready_lanes()
    for lane_dxid in ready_lane_projects:
        workflow_dxid = workflow_manager.assign_workflow(lane_dxid)
        workflow_manager.configure_inputs(lane_dxid, workflow_dxid)




dxpy.run()