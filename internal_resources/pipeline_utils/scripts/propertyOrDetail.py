#!/usr/bin/env python

from argparse import ArgumentParser
from pipeline_utils import app_utils
from pipeline_utils import conf

PROPERTY = conf.dashboardRecordPropertyConstant #numeric constant
DETAIL = conf.dashboardRecordDetailConstant #numeric constant

description = "Returns {PROPERTY} if the attribute is a property used with a DNAnexus dashboard record, or {DETAIL} if it is a detail. Raises app_utils.InvalidPipelineAttrException if it is an unrecognized attribute.".format(PROPERTY=PROPERTY,DETAIL=DETAIL)
parser = ArgumentParser(description=description)
parser.add_argument('-a','--attribute',required=True,help="The name of the attribute.")

args = parser.parse_args()
attr = args.attribute

if app_utils.isProperty(attr):
	print(PROPERTY)
elif app_utils.isDetail(attr):
	print(DETAIL)
else:
	raise app_utils.InvalidPipelineAttrException("Unknown attribute {attr}".format(attr=attr))

