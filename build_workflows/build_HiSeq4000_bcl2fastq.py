#!/usr/bin/python
'''
Description: This will build all the applets in the HiSeq4000_bcl2fastq workflow.
	For this pilot workflow, the only applet built will be bcl2fastq
Args: -t dxapp.template
Retuns: bcl2fastq applet, dxid of workflow object,
'''

import os
import sys
import pdb
import dxpy
import json
import stat
import shutil
import datetime
import subprocess
from dxpy import app_builder

# import applet

#files_to_create = [
#	"applets/bcl2fastq/src/bcl2fastq.py",
#	"applets/bcl2fastq/dxapp.json",
#	"applets/bcl2fastq/rscs/..."]

## Functionalities
# Create applets/bcl2fastq directory
# Create src/ and rscs/ subdirectories
# Copy bcl2fastq.py into src/
# Make dxapp.json file from dxapp.template.json and
# Copy the appropriate internal rscs into rscs/
# Build bcl2fastq
# build HiSeq4000_bcl2fastq workflow

class Applet:

	def __init__(self, home, name, code, config_template):
		self.home = home
		self.name = name
		self.source_code = code
		self.config_template_file = config_template


		timestamp = str(datetime.datetime.now()).split()[0]	# yyyy-mm-dd
		current_commit = self._get_git_commit().rstrip()
		self.version_label = '%s_%s' % (timestamp, current_commit)
		
		self.bundled_depends = []
		# List of dictionaries: [{'filename':<filename>, 'dxid':<dxid>}, {...}, ...]

		# Make applet directory structure because it is necessary for adding internal rscs
		# All directories are made in 'home' directory, which should usually be base of repo
		self.applet_path = '%s/applets/%s/%s' % (self.home, self.name, self.version_label)
		self.src_path = '%s/applets/%s/%s/src' % (self.home, self.name, self.version_label)
		self.rscs_path = '%s/applets/%s/%s/resources' % (self.home, self.name, self.version_label) 

		self._make_new_dir(self.src_path)
		self._make_new_dir(self.rscs_path)

		# Copy source code into applet directory
		src_name = os.path.basename(self.source_code)
		shutil.copy(self.source_code, '%s/%s' % (self.src_path, src_name))

	def build(self, project_dxid, dry_run=False):
		'''
		Build the applet on DNAnexus
		'''

		applet_folder = '/builds/%s' % self.version_label
		
		# Create new build folder if does not already exist
		dx_project = dxpy.DXProject(dxid=project_dxid)
		dx_project.new_folder(folder=applet_folder, parents=True)

		# Upload applet to DNAnexus
		dxpy.app_builder.upload_applet(src_dir=self.applet_path, uploaded_resources=None, 
			project=project_dxid, overwrite=True, override_folder=applet_folder, 
			override_name=self.name)

		# Get dxid of newly built applet
		applet_dict = dxpy.find_one_data_object(name=self.name, project=project_dxid, 
			folder=applet_folder, zero_ok=False, more_ok=False)
		return applet_dict['id']

	def add_rsc(self, local_path, dnanexus_path):
		'''
		Internal rscs are locally stored and are added to an applet by
		copying them into the applet/rsc directory. rscs are added to
		the root directory of the virtual machine instance at runtime.

		Arguments:
			local_rsc_path : string ; full local path of rsc file to be added to applet
			dnanexus_path : string ; relative path to where rsc file should be unpacked on DNAnexus
				(default is root directory)
		Returns:
		'''
		
		applet_path = self.rscs_path + dnanexus_path
		
		# Create parent directories within applet
		rsc_dirname = os.path.dirname(applet_path)
		if not os.path.exists(rsc_dirname):
			os.makedirs(rsc_dirname)

		if (os.path.isfile(local_path)):
			shutil.copyfile(local_path, applet_path)
		else:
			print 'Could not find internal applet rsc file: ' + local_path 

	def add_bundledDepends(self, filename, dxid):
		'''
		External rscs are stored and compiled remotely on DNAnexus and
		are added to an applet by specifying their DNAnexus file information
		in the bundledDepends attribute of runSpec in the configuration file.
		'''

		bundled_depends_dict = {'filename': filename, 'dxid': dxid}
		self.bundled_depends.append(bundled_depends_dict)

	def write_config_file(self, template_file, out_file='dxapp.json'):
		'''
		<Blurb about static vs dynamic attributes etc.>
		'''

		out_path = '%s/%s' % (self.applet_path, out_file)
		## Load static configuration attributes from template file
		with open(template_file, 'r') as TEMPLATE:
			config_attributes = json.load(TEMPLATE)
		## Create blank dxapp.json file to allow for 'upload_resources'
		with open(out_path, 'w') as DXAPP:
			DXAPP.write('temporary file')

		## Set new values for dynamic configuration attributes
		for external_rsc in self.bundled_depends:
			filename = external_rsc['filename']
			dxid = external_rsc['dxid']
			dependency_dict = {"name" : filename, "id" : {'$dnanexus_link':dxid}}
			config_attributes['runSpec']['bundledDepends'].append(dependency_dict)

		## ! NEED TO MAKE BETTER SOLUTION
		## Upload internal resources and add to self.bundled_depends (workflow project)
		#internal_resources = dxpy.app_builder.upload_resources(src_dir=self.applet_path, 
		#	project='project-BkZ4jqj02j8X0FgQJbY1Y183', folder='/')
		#pdb.set_trace()
		#config_attributes['runSpec']['bundledDepends'].append(internal_resources[0])

		## Eventually also update applet source code version in configuration file
		# FUTURE CODE

		## Dump configuration attributes into new 'dxapp.json' file
		with open(out_path, 'w') as OUT:
			json.dump(config_attributes, OUT, sort_keys=True, indent=4)
		internal_resources = dxpy.app_builder.upload_resources(src_dir=self.applet_path, 
			project='project-BkZ4jqj02j8X0FgQJbY1Y183', folder='/')
		config_attributes['runSpec']['bundledDepends'].append(internal_resources[0])
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

class ExternalResourceManager:

	def __init__(self, project_dxid, local_dir, name='external_resources.json', os="Ubuntu-12.04"):
		self.local_dir = local_dir
		self.project_dxid = project_dxid
		self.filename = name
		self.basename = self.filename.split('.')[0]
		self.file_type = self.filename.split('.')[1]
		self.os = os
		self.dx_os = '/' + self.os 	# All dnanexus paths must begin with '/'
		
		self.config_data = None
	
	def update(self):
		# Open local 'external_rscs.json', get 'date_created' value,
		# and rename to 'external_rscs_<date_created>.json'
		
		#pdb.set_trace()	# Debug line
		# Check if there is an existing configuration file and archive it
		existing_config_dir = os.path.join(self.local_dir, self.os)
		if not os.path.exists(existing_config_dir):
			os.makedirs(existing_config_dir)

		existing_config_path = os.path.join(self.local_dir, self.os, self.filename)
		if os.path.isfile(existing_config_path):
			with open(existing_config_path, 'r') as EXIST:
				existing_json = json.load(EXIST)
			existing_date_created = existing_json['date_created']
			existing_config_archived = '%s_%s.%s' % (self.basename, 
				existing_date_created, self.file_type)
			existing_config_archived_path = os.path.join(self.local_dir, self.os, existing_config_archived)
			os.rename(existing_config_path, existing_config_archived_path)
		
		# Get dxid of remote external rscs configuration file
		updated_config_dxlink = dxpy.find_one_data_object(zero_ok=False, more_ok=False,
			name=self.filename, project=self.project_dxid, folder=self.dx_os)
		updated_config_dxid = updated_config_dxlink['id']
		# Download updated version of external rscs configuration file
		updated_config_path = os.path.join(self.local_dir, self.os, self.filename) 
		#pdb.set_trace()
		dxpy.download_dxfile(dxid=updated_config_dxid, filename=updated_config_path, project=self.project_dxid)

	def load_config_data(self):
		config_path = os.path.join(self.local_dir, self.os, self.filename)

		if self.file_type == 'json':
			with open(config_path, 'r') as CONFIG:
				self.config_data = json.load(CONFIG)
		else:
			print 'Error: Unrecognized configuration file rsc_type: %s for configuration file: %s' % (self.file_type, self.filename)
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
			rsc_filename = self.config_data[name]['filename']
			rsc_dxid = self.config.data[name]['dxid']
		elif version:
			try:
				rsc_filename = self.config_data[name]['versions'][version]['filename']
				rsc_dxid = self.config_data[name]['versions'][version]['dxid']
			except:
				print 'Error: Could not get external rsc information for %s version %s' % (name, version)
		resouce_dict = {'filename':rsc_filename, 'dxid':rsc_dxid}
		return(rsc_dict)

	def add_rsc_to_applet(self, applet, name, version=None):
		# Check that configuration data has been loaded
		if not self.config_data:
			self.load_config_data()

		if version:
			filename = self.config_data[name]['versions'][version]['filename']
			dxid = self.config_data[name]['versions'][version]['dxid']
		elif not version:
			filename = self.config_data[name]['filename']
			dxid = self.config_data[name]['dxid']
		applet.add_bundledDepends(filename, dxid)

class InternalResourceManager:
	''' 
	Instead of having InternalResourceManager get the paths, have it handle
	all the aspects of adding rscs to the applet
		InternalrscManager.add_rsc_to_applet(applet, rsc_type, name, internal_rscs_path)
	'''

	def __init__(self, config_file):
		with open(config_file, 'r') as CONFIG:
			self.config = json.load(CONFIG)

	def add_rsc_to_applet(self, applet, rsc_type, name, internal_rscs_path):
		if rsc_type is 'python_package':
			self._add_python_package(applet, rsc_type, name, internal_rscs_path)
		else:
			local_path = self._get_local_path(rsc_type, name, internal_rscs_path)
			dnanexus_path = self._get_dnanexus_path(rsc_type, name)
			applet.add_rsc(local_path, dnanexus_path)

	def _add_python_package(self, applet, rsc_type, name, internal_rscs_path):
		#pdb.set_trace()	# Debug line
		package_files = self.config[rsc_type][name]["all_files"]
		for file in package_files:
			file_local_path = self._get_local_path(rsc_type, name, internal_rscs_path) + '/' + file
			file_dnanexus_path = self._get_dnanexus_path(rsc_type, name) + '/' + file
			applet.add_rsc(file_local_path, file_dnanexus_path)

	def _get_local_path(self, rsc_type, name, internal_rscs_path):
		relative_path = self.config[rsc_type][name]["local_path"]
		full_path = internal_rscs_path + '/' + relative_path
		if (os.path.exists(full_path)):
			return full_path
		else:
			print 'Could not find internal rsc path:' + full_path
			sys.exit()

	def _get_dnanexus_path(self, rsc_type, name):
		path_name = self.config[rsc_type][name]["dnanexus_location"]
		path = self.config["dnanexus_path"][path_name]["path"]
		full_path = path + '/' + name
		return full_path

def main():
	dry_run = True	# Test mode

	## Set full trajectoread repository paths
	build_workflows_dir = os.path.dirname(os.path.abspath(__file__))
	trajectoread_home = os.path.split(build_workflows_dir)[0]
	external_rscs_dir = os.path.join(trajectoread_home, 'external_resources')
	applets_source_dir = os.path.join(trajectoread_home, 'applets_source')
	internal_rscs_dir = os.path.join(trajectoread_home, 'internal_resources')
	applet_templates_dir = os.path.join(trajectoread_home, 'applet_config_templates')

	## Set DNAnexus variables
	dnanexus_os = "Ubuntu-12.04"
	workflow_project_dxid = 'project-BkZ4jqj02j8X0FgQJbY1Y183'
	workflow_object_dxid = ''
	bcl2fastq_stage_dxid = ''
	workflow_name = 'WF_HiSeq4000_bcl2fastq'
	external_rscs_dxid = 'project-BkYXb580p6fv2VF8bjPYp2Qv'
	project_properties = {
		'uhts_lims_url' : 'https://uhts.stanford.edu', 
		'uhts_lims' : '9af4cc6d83fbfd793fe4'
		}

	## bcl2fastq Applet resource information
	internal_rscs_json = internal_rscs_dir + '/' + 'internal_resources.json'
	bcl2fastq_internal_rscs = {
		'python_package': ['scgpm_lims'],
		'script': ['create_sample_sheet.py', 'calculate_use_bases_mask.py'],
		}
	
	bcl2fastq_external_rscs = [
		{'name':'bcl2fastq2', 'version':'2.17.1.14'}
		]

	# Check to make sure user is logged into DNAnexus
	try:
		dxpy.api.system_whoami()
	except:
		print 'You must login to DNAnexus before proceeding ($ dx login)'
		sys.exit()

	## Create new DNAnexus Workflow project if no dxid specified for workflow 
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

	## Create bcl2fastq applet object
	bcl2fastq_name = 'bcl2fastq'
	bcl2fastq_code = os.path.join(applets_source_dir, 'bcl2fastq.py')
	bcl2fastq_config_template = os.path.join(applet_templates_dir, 'bcl2fastq.template.json')
	bcl2fastq = Applet(trajectoread_home, bcl2fastq_name, bcl2fastq_code, bcl2fastq_config_template)

	## Add internal rscs to bcl2fastq applet rscs/
	internal_rsc_manager = InternalResourceManager(internal_rscs_json)
	for rsc_type in bcl2fastq_internal_rscs:
		for rsc_name in bcl2fastq_internal_rscs[rsc_type]:
			internal_rsc_manager.add_rsc_to_applet(bcl2fastq, rsc_type, rsc_name, internal_rscs_dir)

	## Add bcl2fastq external rscs to configuration file	
	external_rsc_manager = ExternalResourceManager(external_rscs_dxid, external_rscs_dir)
	#external_rsc_manager.update()	# Get latest external_rscs.json file from DNAnexus
	for rsc in bcl2fastq_external_rscs:
		#pdb.set_trace()
		if 'version' in rsc:
			name = rsc['name']
			version = rsc['version']
			external_rsc_manager.add_rsc_to_applet(bcl2fastq, name, version)
		elif not 'version' in rsc:
			name = rsc['name']
			external_rsc_manager.add_rsc_to_applet(bcl2fastq, name)
		else:
			pass	# Add error message here

	## Write configuration file
	bcl2fastq.write_config_file(bcl2fastq_config_template)

	## Build bcl2fastq applet
	bcl2fastq_dxid = bcl2fastq.build(project_dxid=workflow_project_dxid, dry_run=False)
	#pdb.set_trace()
	#dxapplet_bcl2fastq_dxid = dxapplet_bcl2fastq.get_id()

	## Build DXWorkflow object
	#if not workflow_object_dxid:
		#dxworkflow = dxpy.new_dxworkflow(title=workflow_name)
		#bcl2fastq_stage_id = dxworkflow.addStage(name='bcl2fastq', 
		#	executable=dxapplet_bcl2fastq_dxid)
	
	## ! Need to implement method to dynamically update workflow stages	
	#elif workflow_object_dxid:
	#	dxworkflow = dxpy.DXWorkflow(workflow_object_dxid, workflow_project_dxid)
	#	dxworkflow.update(, name='bcl2fastq')


if __name__ == "__main__":
	main() 
