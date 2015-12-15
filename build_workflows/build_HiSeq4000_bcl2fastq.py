#!usr/bin/python
'''
Description: This will build all the applets in the HiSeq4000_bcl2fastq workflow.
	For this pilot workflow, the only applet built will be bcl2fastq
Args: -t dxapp.template
Retuns: bcl2fastq applet, dxid of workflow object,
'''

import os
import sys
import dxpy
import json
import shutil
import datetime
import subprocess

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

	def __init__(self, home, name, code, config_template):
		self.home = home
		self.name = name
		self.source_code = code
		self.config_template_file = config_template


		timestamp = str(datetime.datetime.now()).split()[0]	# yyyy-mm-dd
		current_commit = self._get_git_commit()
		self.version_label = '%s_%s' % (timestamp, current_commit)
		
		self.bundled_depends = []
		# List of dictionaries: [{'filename':<filename>, 'dxid':<dxid>}, {...}, ...]

		# Make applet directory structure because it is necessary for adding internal resources
		# All directories are made in 'home' directory, which should usually be base of repo
		self.applet_path = '%s/applets/%s/%s' % (self.home, self.name, self.version_label)
		self.src_path = '%s/applets/%s/%s/src' % (self.home, self.name, self.version_label)
		self.resources_path = '%s/applets/%s/%s/resources' % (self.home, self.name, self.version_label) 

		self._make_new_dir(self.src_path)
		self._make_new_dir(self.resources_path)

		# Copy source code into applet directory
		shutil.copy(self.source_code, '%s/src' % self.src_path)

	def build(self, project_dxid, dry_run=False):
		'''
		Build the applet on DNAnexus
		'''

		dxpy.app_builder.upload_applet(src_dir=self.applet_path, project=project_dxid,
			override_folder='/builds/%s' % self.version_label, override_name=self.name,
			dry_run=dry_run)

	def add_resource(self, local_resource_path):
		'''
		Internal resources are locally stored and are added to an applet by
		copying them into the applet/resource directory. Resources are added to
		the root directory of the virtual machine instance at runtime.

		Arguments:
			local_resource_path : string ; local path of resource to be added to applet
			dnanexus_path : string ; path to where resource should be unpacked on DNAnexus
				(default is root directory)
		Returns:
		'''
		
		applet_path = self.resources_path

		#full_applet_path = resources_path + '/' + dnanexus_path
		
		# Copy all files and directories from local resource path into applet path
		resource_files = os.listdir(local_resource_path)
		for file_name in resouce_files:
			full_local_path = os.path.join(local_resource_path, file_name)
			if (os.path.isfile(full_local_path)):
				shutil.copy(full_local_path, applet_path)

	def add_bundledDepends(self, external_resouce_dict):
		'''
		External resources are stored and compiled remotely on DNAnexus and
		are added to an applet by specifying their DNAnexus file information
		in the bundledDepends attribute of runSpec in the configuration file.
		'''

		self.bundled_depends.append(external_resource_dict)

	def write_config_file(self, template_file, out_file='dxapp.json'):
		'''
		<Blurb about static vs dynamic attributes etc.>
		'''
		## Load static configuration attributes from template file
		with open(template_file, 'r') as TEMPLATE:
			config_attributes = json.load(TEMPLATE)

		## Set new values for dynamic configuration attributes
		for external_resource in self.bundled_depends:
			filename = external_resource[0]
			dxid = external_resource[1]
			dependency_dict = {"name" : filename, "id" : dxid}
			config_attributes['runSpec']['bundledDepends'].append(dependency_dict)

		## Eventually also update applet source code version in configuration file
		# FUTURE CODE

		## Dump configuration attributes into new 'dxapp.json' file
		out_path = '%s/%s' % (self.applet_path, out_file)
		with open(out_path, 'w') as OUT:
			json.dump(config_attributes, OUT, sort_keys=True, indent=4)

	## Private functions
	def _make_new_dir(self, directory):
		if not os.path.exists(directory):
			os.makedirs(directory)

	def _get_git_commit(self):
		# NOTE: not at all confident this is optimal solution
		commit = subprocess.check_output(['git', 'describe', '--always'])
		return commit 

class ExternalResourceConfig:

	def __init__(self, project_dxid, local_dir, name='external_resources.json', os="Ubuntu-12.04"):
		self.local_dir = local_dir
		self.project_dxid = project_dxid
		self.filename = name
		self.basename = self.filename.split('.')[0]
		self.filetype = self.filename.splti('.')[1]
		self.os = os
		
		self.config_data = None
	
	def update(self):
		# Open local 'external_resources.json', get 'date_created' value,
		# and rename to 'external_resources_<date_created>.json'
		existing_config_path = os.path.join(self.local_dir, os, self.filename)
		with open(existing_config_path, 'r') as EXIST:
			existing_json = json.load(EXIST)
		existing_date_created = existing_json['date_created']
		existing_config_archived = '%s_%s.%s' % (self.basename, 
			existing_date_created, self.filetype)
		existing_config_archived_path = os.path.join(self.local_dir, os, 
			existing_config_archived)
		os.rename(existing_config_path, existing_config_archived_path)
		
		# Get dxid of remote external resources configuration file
		updated_config_dxid = dxpy.find_one_data_object(zero_ok=False, more_ok=False,
			name=filename, project=project_dxid, folder=self.os)
		# Download updated version of external resources configuration file
		updated_config_path = os.path.join(self.local_dir, os, self.filename) 
		dxpy.download_dxfile(dxid=updated_config_dxid, filename=updated_config_path, 
			project=project_dxid)

	def load_config_data(self):
		config_path = os.path.join(self.local_dir, os, self.filename)

		if self.filetype == 'json':
			with open(config_path, 'r') as CONFIG:
				self.config_data = json.load(filepath)
		else:
			print 'Error: Unrecognized configuration file type: %s for configuration file: %s' % 
				(self.filetype, self.filename)
			sys.exit()

	def get_filename_dxid(self, name, version=None):
		'''
		Returns: Dictionary object of format {'filename':<filename>, 'dxid':<dxid>}
		'''
		# Automatically load configuration data if not already done
		if not self.config_data:
			self.load_config_data()
		# If no version specified, get current one
		if not version:
			resource_filename = self.config_data[name]['filename']
			resource_dxid = self.config.data[name]['dxid']
		elif version:
			try:
				resource_filename = self.config_data[name]['versions'][version]['filename']
				resource_dxid = self.config_data[name]['versions'][version]['dxid']
			except:
				print 'Error: Could not get external resource information for %s version %s' % 
					(name, version)
		resouce_dict = {'filename':resource_filename, 'dxid':resource_dxid}
		return(resource_dict)

class InternalResourceConfig:

	def __init__(self):
		!WRITE PLACEHOLDER CODE

def main():
	dry_run = True	# Test mode

	## Set full trajectoread repository paths
	build_workflows_dir = os.path.dirname(os.path.abspath(__file__))
	trajectoread_home = os.path.split(build_workflows_dir)[0]
	external_resources_dir = os.path.join(trajectoread_home, 'external_resources')
	applets_source_dir = os.path.join(trajectoread_home, 'applets_source')
	internal_resources_dir = os.path.join(trajectoread_home, 'interal_resources')
	applet_templates_dir = os.path.join(trajectoread_home, 'applet_templates')

	## Set DNAnexus variables
	dnanexus_os = "Ubuntu-12.04"
	workflow_project_dxid = ''
	workflow_object_dxid = ''
	workflow_name = 'WF_HiSeq4000_bcl2fastq'
	external_resources_dxid = 'project-BkYXb580p6fv2VF8bjPYp2Qv'
	project_properties = {
		'uhts_lims_url' : 'https://uhts.stanford.edu', 
		'uhts_lims' : '9af4cc6d83fbfd793fe4'
		}

	# Check to make sure user is logged into DNAnexus
	try:
		dxpy.api.system_whoami()
	except:
		print 'You must login to DNAnexus before proceeding ($ dx login)'
		sys.exit()

	## If no dxid specified for workflow, create new DNAnexus project
	if not workflow_project_dxid:
		workflow_project_dxid = dxpy.api.project_new(input_params={
			'name' : workflow_name,
			'summary' : 'Convert HiSeq 4000 bcl files to fastq',
			'description' : 'long summary',
			'properties' : project_properties
			})
	elif workflow_project_dxid:
		dxpy.api.project_set_properties(object_id=workflow_project_dxid, 
			input_params={"properties" : project_properties})	

	bcl2fastq_name = 'bcl2fastq'
	bcl2fastq_code = os.path.join(applets_source_dir, 'bcl2fastq.py')
	bcl2fastq_config_template = os.path.join(applet_templates_dir, 'bcl2fastq.template.json')

	bcl2fastq_internal_resources = [
		'scgpm_lims'
		]
	# module load gbsc/limshostenv/prod
	# module load gbsc/scgpm_lims/current
	# module load bcl2fastq2/2.17.1.14
	
	bcl2fastq_external_resources = [
		{'name':'bcl2fastq2', 'version':'2.17.1.14'}
		]

	## Create bcl2fastq applet object
	bcl2fastq = Applet(trajectoread_home, bcl2fastq_name, bcl2fastq_code, 
		bcl2fastq_config_template)

	## Add bcl2fastq internal resources to resources/
	internal_resource_list = InternalResourceList()
	for resource in bcl2fastq_internal_resources:
		resource_path = internal_resource_list[resource]['path']
		bcl2fastq.add_resource(local_resource_path=resource_path)

	## Add bcl2fastq external resources to configuration file	
	external_resources_config = ExternalResourceConfig(external_resources_dxid, 
		external_resources_dir)
	# Get latest external_resources.json file from DNAnexus
	external_resources_config.update()
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
	bcl2fastq.write_config_file()

	## Build bcl2fastq applet
	bcl2fastq.build(project_dxid=workflow_project_dxid, dry_run=dry_run)


if __name__ == "__main__":
	main() 
