#!/usr/bin/env python

"""
Creates a Dashboard record object inside of the Dashboard records project (conf.dashboardProject), and adds as many of the supported attributes as it can, where attributes are either 
properties or details. The new dashboard record is named according to the pipeline convention (see app_utils.createDashboardRecordName).  Any existing records by the same name are deleted from the Dashboard records project.
"""
import sys
from argparse import ArgumentParser
import datetime
import dxpy
from pipeline_utils import conf, app_utils

description=""
parser = ArgumentParser(description=description)
parser.add_argument('-l','--lane-project-id',required=True,help="The ID of the DNAnexus lane project to which the new dashboard record represents.")

args = parser.parse_args()
lane_project_id = args.lane_project_id

my_auth = app_utils.getSecurityAuth(conf.dashboardContributeToken) #use this for setting any properties/details on a dashboard record

laneProjProps = app_utils.get_project_properties(lane_project_id)

try:
	run_name = laneProjProps[conf.runNameAttrName]
	lane_number = laneProjProps[conf.laneAttrName]
	run_project_id = laneProjProps[conf.runProjectAttrName]
except KeyError as e:
	raise KeyError(e.message + " " + "Error in applet dashboard_run_record in populate.main(): The provided lane_project_id must have properties set for '{run_name}', '{lane}', and '{runProject}'! The existing properties on the lane project are {laneProjProps}.".format(run_name=conf.runNameAttrName,lane=conf.laneAttrName,runProject=conf.runProjectAttrName,laneProjProps=laneProjProps))
recordid = app_utils.createDashboardRunRecord(run_name=run_name,lane=lane_number,deleteExisting=True)

recordCreateTime =  datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0) #returns a datetime.datetime instance.
recordCreateTime = recordCreateTime.total_seconds() * 1000 #Returns fractional microsecond part. x1000 to get milliseconds
recordCreateTime = int(recordCreateTime) #drop remaining fractional part

submitter_name =  app_utils.get_submitter_name(runName=run_name,lane=lane_number)
submitter_email = app_utils.get_submitter_email(runName=run_name,lane=lane_number)
library_name = app_utils.get_sample_name(runName=run_name,lane=lane_number)
genome = app_utils.get_mapping_reference(runName=run_name,lane=lane_number)

attrs = { 
					conf.uploadDateAttrName  : recordCreateTime,
          conf.userAttrName        : submitter_name,
          conf.emailAttrName       : submitter_email,
          conf.libraryAttrName     : library_name,
					conf.runNameAttrName     : run_name,
					conf.laneAttrName    : lane_number,
					conf.laneProjectAttrName : lane_project_id,
					conf.runProjectAttrName  : run_project_id
        }   

if genome:
	attrs[conf.mappingReferenceAttrName] = genome

app_utils.set_dashboard_record_attributes(recordid=recordid,attrs=attrs)
#dashboard_proj,recid = recordid.split(":")
#rec = dxpy.DXRecord(project=dashboard_proj,dxid=recid)
#rec.close()

print(recordid)
