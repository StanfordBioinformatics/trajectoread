#!usr/bin/python
'''
Description: This will build all the applets in the HiSeq4000_bcl2fastq workflow.
	For this pilot workflow, the only applet built will be bcl2fastq
Args: -t dxapp.template
Retuns: bcl2fastq applet, dxid of workflow object,
'''

import os
import shutil

# import applet

files_to_create = [
	"applets/bcl2fastq/src/bcl2fastq.py",
	"applets/bcl2fastq/dxapp.json",
	"applets/bcl2fastq/resources/..."]

## Functionalities
# Create applets/bcl2fastq directory
# Create src/ and resources/ subdirectories
# Copy bcl2fastq.py into src/
# Make dxapp.json file from dxapp.template.json and
# Copy the appropriate internal resources into resources/
# Build bcl2fastq
# (Future) build HiSeq4000_bcl2fastq workflow

class Applet:

	def init(name, code, configuration_template):
		self.name = name
		self.code_file = code
		self.configuration_template_file = configuration_template

		resouces = []
		bundled_depends = []

		# Make applet directory structure
	
	def make():
		# get timestamp
		# get git version for 'build_workflow.py' script
		# Create all applet directories
		# Copy all files and resources into directories
		# Write dxapp.json file

	def build():
		# get timestamp
		# get git version for 'build_workflow.py' script
		# build the applet on DNAnexus

	def add_resource(local_path, resources_path, dnanexus_path):
		'''
		Internal resources are locally stored and are added to an applet by
		copying them into the applet/resource directory. Resources are added to
		the root directory of the virtual machine instance at runtime.

		local_path : string ; local path of resource to be added to applet
		dnanexus_path : string ; path to where resource should be unpacked on DNAnexus
			(default is root directory)
		'''

		full_resource_path = resources_path + '/' + dnanexus_path
		
		resource_files = os.listdir(local_path)
		for file_name in resouce_files:
			full_file_path = os.path.join(local_path, file_name)
			if (os.path.isfile(full_file_path)):
				shutil.copy(full_file_path,)


	def add_bundledDepends(name, version, dxid):
		'''
		External resources are stored and compiled remotely on DNAnexus and
		are added to an applet by specifying their DNAnexus file information
		in the bundledDepends attribute of runSpec in the configuration file.
		'''


	def write_configuration_file(template):

class ExternalResourceList:

	def init(os="Ubuntu_12.04")

	def update(os):


def main():
	dnanexus_os = "Ubuntu_12.04"
	bcl2fastq_name = 'bcl2fastq'
	bcl2fastq_code = '../applets_source/bcl2fastq.py'
	configuration_template = './dxapp.template.json'

	bcl2fastq = Applet(name=name, code=applet_code_file,
		configuration_template=configuration_template)

	## Add bcl2fastq internal resources to resources/
	# module load gbsc/limshostenv/prod
	# module load gbsc/scgpm_lims/current
	# module load bcl2fastq2/2.17.1.14
	internal_resources = []
	bcl2fastq.add_resource()

	## Add bcl2fastq external resources to configuration file
	external_resources = []
	external_resources = ExternalResourceList()
	external_resources.update(os=dnanexus_os)


	bcl2fastq.add_bundledDepends




if __name__ == "__main__":
	main() 
