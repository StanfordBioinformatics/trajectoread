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

## Classes
FlowcellLane()
	dashboard_record : dxid
	rta_version : int
	barcodes : list
	run_project_id : dxid
	run_project : DXProject
	sequencing_run_name : string
	lane_index : int
	barcodes : list

	convert_bcl2fastq(rta_version)
	publish(files)
	update_dashboard_record(dashboard_record, properties)

LIMSHandler()
	lims_url : string
	lims_token : string

	get_barcodes(flowcell.sequencing_run_name, flowcell.lane_index)


## Modules in use:
# gbsc/limshostenv/prod
# gbsc/scgpm_lims/current
# bcl2fastq2/2.17.1.14
'''

import dxpy

# Fx test 1: Create Flowcell class that is able to populate metadata from 
# dashboard record.

class FlowcellLane()
	
	def __init__(self, dashboard_record_dxid, dashboard_project_dxid):
		
		self.dashboard_record_dxid = dashboard_record_id
		self.dashboard_project_dxid = dashboard_project_dxid
		self.dashboard_record = dxpy.DXRecord(dxid = self.dashboard_record_id,
			project = self.dashboard_project_dxid)
		# self.rta_version = dashboard_record.properties['rta_version']
		self.run_project_dxid = record.get_details()['runProject']
		self.run_project = dxpy.DXProject(dxid = self.run_project_dxid)
		self.run_name = record.get_details()['run']
		self.lane_index = record.get_details()['lane']
		self.barcodes = []

	def describe():
		print "Sequencing run: %s" % self.run_name
		print "Flowcell lane index: %s" % self.lane_index

@dxpy.entry_point('main')
def main(record_id, project_id):
	this_lane = FlowcellLane(dashboard_record_dxid = record_id,
		dashboard_project_dxid = project_id)
	this_lane.describe()


dxpy.run()
