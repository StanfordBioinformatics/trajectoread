#!/usr/bin/env python

import sys
import os
from scgpm_lims import Connection
import json
import argparse

try:
	token = os.environ["UHTS_LIMS_TOKEN"]
except KeyError:
	token = None
try:
	url = os.environ["UHTS_LIMS_URL"]
except KeyError:
	url = None

description = """
Create a sample sheet for demultiplexing by communicating with UHTS. The sample sheet can be created for all lanes on the given run, or 
just the specified lane. Supports bcl2fastq 1x and 2x. For 2x, the second index (I5) is reverse-complemented
with respect to what's stored in UHTS. As stated in the Illumina docs: For Illumina sequencing systems running
RTA version 1.18.54 and later, use bcl2fastq2 Conversion Software v2.17.  For Illumina sequencing systems runnings
RTA versions earlier than 1.18.54, use bcl2fastq Conversion Software v1.8.4.

The version of RTA used in the sequencing run can be found in the runParameters.xml file with the run directory.
"""

parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,description=description)

parser.add_argument('-r', '--run_name', help='Name of the run.', required=True)
if token:
	parser.add_argument('-t', '--lims_token', help='LIMS access token. Default is set by UHTS_LIMS_TOKEN environment variable.', default = token)
else:
	parser.add_argument('-t', '--lims_token', help='LIMS access token.',required=True)
if url:
	parser.add_argument('-u', '--lims_url', help='LIMS url. Default is set by UHTS_LIMS_URL environment variable (%(default)s).', default = url)
else:
	parser.add_argument('-u', '--lims_url', help='LIMS url', required=True)
parser.add_argument('-b','--bcl2fastq-version',required=True,type=int,help="int. The major version number of the bcl2fastq demultiplexer that was used to demultiplex the run.")
parser.add_argument('-l','--lane',type=int,help="The number of the lane sequenced on the flowcell.")

args = parser.parse_args()
conn = Connection(lims_url=args.lims_url, lims_token=args.lims_token, verbose=False)

fn = args.run_name
if args.lane:
	fn += "_L" + str(args.lane)
fn += '_samplesheet.csv'
conn.getsamplesheet(run=args.run_name,lane=args.lane,bcl2fastq_version=args.bcl2fastq_version,filename=fn)
print fn
