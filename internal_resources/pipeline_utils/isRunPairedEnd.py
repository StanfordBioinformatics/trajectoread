from argparse import ArgumentParser
from gbsc_utils.illumina import runinfoxml
from pipeline_utils import conf

description = "Determines whether a sequencing run is paired-end."
parser = ArgumentParser(description=description)
parser.add_argument("-i","--infile",required=True,help="The RunInfo.xml file of the Illumina sequencing run.")

args = parser.parse_args()
runinfoFile = args.infile

ri = runinfoxml.RI(runinfoFile=runinfoFile)
pairedEnd = ri.isPairedEnd()

if pairedEnd:
	print(conf.pairedEndAttrValues["true"])
else:
	print(conf.pairedEndAttrValues["false"])

