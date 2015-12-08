#!usr/bin/python
'''
Description: This will build all the applets in the HiSeq4000_bcl2fastq workflow.
	For this pilot workflow, the only applet built will be bcl2fastq
Args: -t dxapp.template
Retuns: bcl2fastq applet, dxid of workflow object,
'''

import os
import sys
import shutil
import datetime
import subprocess
import json

# import applet

#files_to_create = [
#	"applets/bcl2fastq/src/bcl2fastq.py",
#	"applets/bcl2fastq/dxapp.json",
#	"applets/bcl2fastq/resources/..."]

## Functionalities
# Create applets/bcl2fastq directory
# Create src/ and resources/ subdirectories
# Copy bcl2fastq.py into src/
# Make dxapp.json file from dxapp.template.json and
# Copy the appropriate internal resources into resources/
# Build bcl2fastq
# (Future) build HiSeq4000_bcl2fastq workflow

class Applet:

	def init(home, name, source_code, configuration_template):
		self.home = home
		self.name = name
		self.source_code = code
		self.configuration_template_file = configuration_template


		timestamp = str(datetime.datetime.now()).split()[0]	# yyyy-mm-dd
		current_commit = _get_git_commit()
		self.version_label = '%s_%s' % (timestamp, current_commit)
		
		self.bundled_depends = []
		# List of lists; [[<filename>, <dxid>],[<filename2>, <dxid2>]]

		# Make applet directory structure because it is necessary for adding internal resources
		# All directories are made in 'home' directory, which should usually be base of repo
		self.applet_path = '%s/applets/%s/%s' % (self.home, self.name, self.version_label)
		self.src_path = '%s/applets/%s/%s/src' % (self.home, self.name, self.version_label)
		self.resources_path = '%s/applets/%s/%s/resources' % (self.home, self.name, self.version_label) 

		_make_new_dir(self.src_path)
		_make_new_dir(self.resources_path)

		# Copy source code into applet directory
		shutil.copy(source_code, '%s/src' % self.src_path)

	def build(project_dxid, dry_run=False):
		'''
		Build the applet on DNAnexus
		'''

		else:
			if not project_dxid:
				# If no project specified, create a new one for this workflow
				# dxpy.createNewProjec
			dxpy.app_builder.upload_applet(src_dir=self.applet_path, project=project_dxid,
				override_folder='/builds/%s' % self.version_label, override_name=self.name,
				dry_run=dry_run)

	def add_resource(local_resource_path):
		'''
		Internal resources are locally stored and are added to an applet by
		copying them into the applet/resource directory. Resources are added to
		the root directory of the virtual machine instance at runtime.

		local_path : string ; local path of resource to be added to applet
		dnanexus_path : string ; path to where resource should be unpacked on DNAnexus
			(default is root directory)
		'''
		
		applet_path = self.resources_path

		#full_applet_path = resources_path + '/' + dnanexus_path
		
		# Copy all files and directories from local resource path into applet path
		resource_files = os.listdir(local_resource_path)
		for file_name in resouce_files:
			full_local_path = os.path.join(local_resource_path, file_name)
			if (os.path.isfile(full_local_path)):
				shutil.copy(full_local_path, applet_path)

	def add_bundledDepends(filename, dxid):
		'''
		External resources are stored and compiled remotely on DNAnexus and
		are added to an applet by specifying their DNAnexus file information
		in the bundledDepends attribute of runSpec in the configuration file.
		'''

		self.bundled_depends.append([filename, dxid])


	def write_configuration_file(template_file, out_file='dxapp.json'):
		'''
		<Blurb about static vs dynamic attributes etc.>
		'''
		## Load static configuration attributes from template file
		with open(template_file, 'r') as TEMPLATE:
			configuration_attributes = json.load(TEMPLATE)

		## Set new values for dynamic configuration attributes
		for external_resource in self.bundled_depends:
			filename = external_resource[0]
			dxid = external_resource[1]
			dependency_dict = {"name" : filename, "id" : dxid}
			configuration_attributes['runSpec']['bundledDepends'].append(dependency_dict)

		## Eventually also update applet source code version in configuration file
		# FUTURE CODE

		## Dump configuration attributes into new 'dxapp.json' file
		out_path = ('%s/%s' % (self.applet_path, out_file))
        with open(out_path, 'w') as OUT:
            json.dump(configuration_attributes, OUT, sort_keys=True, indent=4)


	## Private functions
	def _make_new_dir(directory):
		if not os.path.exists(directory):
			os.makedirs(directory)

	def _get_git_commit():
		# NOTE: not at all confident this is optimal solution
		commit = subprocess.check_output(['git', 'describe', '--always'])
		return commit 

class ExternalResourceList:

	def init(os="Ubuntu_12.04")

	def update(os):


def main():
	dry_run = True	# Test mode

	dnanexus_os = "Ubuntu_12.04"
	workflow_project_dxid =''
	bcl2fastq_name = 'bcl2fastq'
	home_dir = '../'	# Base directory of trajectoread repository
	bcl2fastq_code = '../applets_source/bcl2fastq.py'
	configuration_template = './dxapp.template.json'

	bcl2fastq_internal_resources = []
	# module load gbsc/limshostenv/prod
	# module load gbsc/scgpm_lims/current
	# module load bcl2fastq2/2.17.1.14
	bcl2fastq_external_resources = []
	# Lists of lists: [[<external_resource>,<version>],['fastqc',1.4],[bedtools]]

	## Create bcl2fastq applet object
	bcl2fastq = Applet(home=home_dir, name=name, source_code=bcl2fastq_code,
		configuration_template=configuration_template)

	## Add bcl2fastq internal resources to resources/
	internal_resource_list = InternalResourceList()
	for resource in bcl2fastq_internal_resources
		resource_path = internal_resource_list[resource]['path']
		bcl2fastq.add_resource(local_resource_path=resource_path)

	## Add bcl2fastq external resources to configuration file	
	external_resource_list = ExternalResourceList()
	# Get latest external_resources.json file from DNAnexus
	external_resource_list.update(os=dnanexus_os)
	for resource in external_resources:
		# Check that resource has only 1 or 2 elements
		if len(resource) < 1 or len(resource) > 2:
			print "Invalid number of elements in resource entry: %s" % resource
			sys.exit()
		elif len(resource) == 1:	# Use current version of resource if not specified
			name = resouce[0]
			filename = external_resource_list[name]['filename']
			dxid = external_resource_list[name]['dxid']
		elif len(resource) == 2:	
			name = resource[0]
			version = resource[1]
			filename = external_resource_list[name][version]['filename']
			dxid = exeternal_resource_list[name][version]['dxid']
		bcl2fastq.add_bundledDepends(filename=filename, dxid=dxid)

	## Write configuration file
	bcl2fastq.write_configuration_file(template_file=configuration_template)

	## Build bcl2fastq applet
	bcl2fastq.build(project_dxid=workflow_project_dxid, dry_run=dry_run)


if __name__ == "__main__":
	main() 
