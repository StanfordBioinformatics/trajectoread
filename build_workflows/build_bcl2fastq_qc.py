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

#class DirectoryListing:
#	def __init__(self, home):
#		# [dirname, basename] = os.path.split(abspath)

class WorkflowConfiguration:

	def __init__(self, config_file):
		self.config_file = config_file
		
		self.workflow_project_dxid = None
		self.workflow_name = None
		self.workflow_object_dxid = None
		self.edit_version = None
		self.dx_OS = None
		self.external_rscs_dxid = None

		self.stages = {}
		self.applets = {}

		self.dx_login_check
		self.get_workflow_attributes(self.config_file)

		if not self.workflow_project_dxid and not self.workflow_object_dxid:
			# self.workflow_project_dxid = Create Workflow project
			# self.workflow_object_dxid = Create empty Workflow object


		## Old trash ##
		if not self.workflow_project_dxid and not self.workflow_object_dxid:
			# Create workflow
			self.workflow = self.create_workflow()
			
			# Add stages
			for i in range(0, len(workflow_attributes['stages'])):
				stages[i] = self.workflow.add_stage(workflow_attributes['stages'][i])
			# Link inputs
			for i in range(0, len(workflow_attributes['stages'])):
				link_inputs = (workflow_attributes['stages'][i]['link_inputs'])
				self.workflow.update_stage(inputs=link_inputs)

		elif self.workflow_dxid and self.edit_version:

			# Update stages with new executables and links
			for i in range(0, len(workflow_attributes['stages'])):
				self.workflow.update_stage(everything)

	def dx_login_check(self):
		try:
			dxpy.api.system_whoami()
		except:
			print 'You must login to DNAnexus before proceeding ($ dx login)'
			sys.exit()

	def create_new_workflow_project(self):

		project_dxid = dxpy.api.project_new(input_params={
			'name' : self.name,
			'summary' : self.summary,
			'description' : self.description,
			'properties' : self.project_properties
			})
		return project_dxid

	def get_workflow_attributes(self, config_file):
		with open(config_file, 'r') as CONFIG:
			attributes = json.load(CONFIG)
			
			self.workflow_project_dxid = attributes['workflow_project_dxid']
			self.workflow_name = attributes['workflow_name']
			self.workflow_object_dxid = attributes['workflow_dxid']
			self.edit_version = workflow_attributes['edit_version']
			self.dx_OS = workflow_attributes['dx_OS']
			self.external_rscs_dxid = workflow_attributes['external_rscs_dxid']

			self.applets = workflow_attributes['applets']


class Workflow:

	def __init__(self, name, trajectoread_dirs, workflow_dxid, 
				 project_dxid=None, project_properties={}, object_dxid=None, 
				 summary='', description=''):
		''' A workflow could theoretically hold infinite stages and 
		dictionary should be designed to accomodate this. Current vision 
		is for the stage key to be a combination of the applet name and the
		index of the stage in the worflow. For example if 'bcl2fastq' was the
		first-stage applet and bwa_mem_controller was the second, their keys would be 
		'bcl2fastq-1' and 'bwa_mem_controller-2', respectively. Or, my original
		thought was to do indexing by applet, i.e.: 
		'bcl2fastq-1', bwa_mem_controller-1', bwa_mem_controller-2'. This was
		when I was thinking in terms of applets instead of stages, and I feel
		the stage based option makes more sense.
		'''
		
		self.name = name
		self.project_dxid = project_dxid
		self.project_properties = project_properties
		self.workflow_dxid = workflow_dxid
		self.trajectoread_dirs = trajectoread_dirs
		self.summary = summary
		self.description = description

		self.stages = {}

		## Check to make sure user is logged into DNAnexus
		try:
			dxpy.api.system_whoami()
		except:
			print 'You must login to DNAnexus before proceeding ($ dx login)'
			sys.exit()

		## Create new DNAnexus Workflow project if no dxid specified for workflow 
		## DEV: ... or search by workflow project by name
		if not self.project_dxid:
			self.project_dxid = self.create_new_workflow_project()

		elif self.project_dxid and self.project_properties:
			dxpy.api.project_set_properties(object_id=self.project_dxid, 
					input_params={"properties" : self.project_properties})

	def add_stage(self, applet_name):	# prepare applet, build, and add to workflow
		''' 
		Prepares applet by finding appropriate source code and configuration 
		template and using it to create Applet object. Then adds internal and 
		external resources. Finally, builds the applet on DNAneuxus in the workflow
		project.
		'''

		stage_index = len(self.stages)
		stage_name = applet_name + '-' + str(stage_index)

		stage = Stage(stage_name, applet_name, self.project_dxid, self.trajectoread_dirs)
		return(stage)

	def create_new_workflow_project(self):

		project_dxid = dxpy.api.project_new(input_params={
			'name' : self.name,
			'summary' : self.summary,
			'description' : self.description,
			'properties' : self.project_properties
			})
		return project_dxid

class Stage:

	def __init__(self, name, applet_name, project_dxid, trajectoread_dirs):

		self.name = name
		self.applet_name = applet_name
		self.project_dxid = project_dxid
		self.trajectoread_dirs = trajectoread_dirs
		self.applet_code_path = None
		self.applet_config_path = None
		self.applet = None
		self.applet_dxid = None

		self.internal_rscs = None
		self.external_rscs = None

		#### Initiate applet ####
		## Find applet code
		applet_code_basename = applet_name + '.py'
		self.applet_code_path = os.path.join(self.trajectoread_dirs['applets_source'], applet_code_basename)
		## Find applet configuration file
		applet_config_basename = applet_name + '.template.json'
		self.applet_config_path = os.path.join(self.trajectoread_dirs['applet_templates'], applet_config_basename)	# Config template will always be 
		## Create applet object
		self.applet = Applet(self.trajectoread_dirs['home'], applet_name, 
			self.applet_code_path, self.applet_config_path, self.project_dxid)

	def add_internal_applet_rscs(self, internal_rsc_manager, internal_rscs):
		## Add applet internal rscs to bcl2fastq applet rscs/
		# internal_rscs : 
		self.internal_rscs = internal_rscs
		for rsc_type in self.internal_rscs:
			for rsc_name in self.internal_rscs[rsc_type]:
				internal_rsc_manager.add_rsc_to_applet(self.applet, rsc_type, rsc_name, self.trajectoread_dirs['internal_rscs'])

	def add_external_applet_rscs(self, external_rsc_manager, external_rscs):
		## Add applet external rscs to configuration file
		self.external_rscs = external_rscs	
		for rsc in self.external_rscs:
			if 'version' in rsc:
				name = rsc['name']
				version = rsc['version']
				external_rsc_manager.add_rsc_to_applet(self.applet, name, version)
			elif not 'version' in rsc:
				name = rsc['name']
				external_rsc_manager.add_rsc_to_applet(self.applet, name)
			else:
				print 'How did you get here? What are you doing? You did something wrong with external resources. (Probably)'
				pass

	def commit(self):
		#pdb.set_trace()
		self.applet.write_config_file(self.applet_config_path)
		self.applet_dxid = self.applet.build(project_dxid=self.project_dxid)
		return(self.applet_dxid)

class Applet:

	def __init__(self, name, project_dxid, repo_dirs):
		self.name = name
		self.project_dxid = project_dxid

		timestamp = str(datetime.datetime.now()).split()[0]	# yyyy-mm-dd
		current_commit = self._get_git_commit().rstrip()
		self.version_label = '%s_%s' % (timestamp, current_commit)
		
		self.internal_resources = []
		self.bundled_depends = []	# External resources
		# List of dictionaries: [{'filename':<filename>, 'dxid':<dxid>}, {...}, ...]

		## Find applet code
		code_basename = self.name + '.py'
		self.code_path = os.path.join(repo_dirs['applets_source'], code_basename)
		## Find applet configuration file
		config_basename = self.name + '.template.json'
		self.config_path = os.path.join(repo_dirs['applet_templates'], config_basename)
		
		# Make applet directory structure because it is necessary for adding internal rscs
		# All directories are made in 'home' directory, which should usually be base of repo
		self.applet_path = '%s/applets/%s/%s' % (repo_dirs['home'], self.name, self.version_label)
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
		
		local_path = local_path
		dnanexus_path = dnanexus_path
		applet_path = self.rscs_path + dnanexus_path
		
		# Create parent directories within applet
		rsc_dirname = os.path.dirname(applet_path)
		if not os.path.exists(rsc_dirname):
			os.makedirs(rsc_dirname)

		if (os.path.isfile(local_path)):
			shutil.copyfile(local_path, applet_path)
			self.internal_resources.append(local_path)
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

		# If applet has internal resources, upload them and add to config file
		if len(self.internal_resources) > 0:
			internal_resources = dxpy.app_builder.upload_resources(
				src_dir=self.applet_path, project=self.project_dxid, folder='/')
			config_attributes['runSpec']['bundledDepends'].append(
				internal_resources[0])
			with open(out_path, 'w') as OUT:
				json.dump(config_attributes, OUT, sort_keys=True, indent=4)
		else:
			print 'Notice: No internal resources uploaded for applet %s' % self.name

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
	
	DEV: I think I should hardcode more of this stuff to make it fixed, rather
	than trying to weave it through Workflow -> Stage objects. trajectoread
	represents a mix of dynamic static architecture.
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

def add_applet(trajectoread_dirs, name):

	applet_name = name
	applet_code = os.path.join(trajectoread_dirs['applets_source'], 'bwa_mem_controller.py')

def main():
	dry_run = True	# Test mode

	## DEV: Create new object to handle all directory listings
	## DEV: dir_list = DirectoryListing(home_)
	trajectoread_dirs = {}
	trajectoread_dirs['build_workflows'] = os.path.dirname(os.path.abspath(__file__))
	trajectoread_dirs['home'] = os.path.split(trajectoread_dirs['build_workflows'])[0]
	trajectoread_dirs['external_rscs'] = os.path.join(trajectoread_dirs['home'], 'external_resources')
	trajectoread_dirs['applets_source'] = os.path.join(trajectoread_dirs['home'], 'applets_source')
	trajectoread_dirs['internal_rscs'] = os.path.join(trajectoread_dirs['home'], 'internal_resources')
	trajectoread_dirs['applet_templates'] = os.path.join(trajectoread_dirs['home'], 'applet_config_templates')
	trajectoread_dirs['workflow_config_templates'] = os.path.join(trajectoread_dirs['home'], 'workflow_config_templates')
	trajectoread_dirs['applets'] = os.path.join(trajectoread_dirs['home'], 'applets')

	#### Configure DNAnexus project ####
	workflow_config_basename = 'wf_bcl2fastq_bwa.json'
	workflow_config_path = os.path.join(trajectoread_dirs['workflow_config_templates'], 
										workflow_config_basename)
	workflow_config = WorkflowConfig(workflow_config_path)

	#### Create resource manager objects ####
	internal_rscs_json = trajectoread_dirs['internal_rscs'] + '/internal_resources.json'
	internal_rsc_manager = InternalResourceManager(internal_rscs_json)
	external_rsc_manager = ExternalResourceManager(external_rscs_dxid, trajectoread_dirs['external_rscs'])

	for applet_name in workflow_config.applets:
		print 'Building %s applet' % applet_name
		applet = Applet()
		applet.add_internal_rscs()
		applet.add_external_rscs()
		applet.configure() (????)
		applet_dxid/applet_dxlink = applet.build()
		workflow_config.applets[applet_name]['dxid'] = applet_dxid		













		#stage = workflow.add_stage(applet)
		## Add internal rscs to bwa_mem_controller applet rscs/
		#stage.add_internal_applet_rscs(internal_rsc_manager, internal_rscs[applet])
		## Add bwa_mem_controller external rscs to configuration file	
		#stage.add_external_applet_rscs(external_rsc_manager, external_rscs[applet])
		## Commit bwa_mem_controller stage
		#applet_dxid = stage.commit()
		#workflow_applets[i]['dxid'] = applet_dxid



	# Find or create DXWorkflow object
	# Workflow.__init__()
	if not workflow_object_dxid:
		workflow = dxpy.new_dxworkflow(title=workflow_name)
	elif workflow_object_dxid:
		workflow = dxpy.DXWorkflow(dxid = workflow_object_dxid, 
								   project = workflow_project_dxid)
		edit_version = workflow.describe()['editVersion']
	else:
		print 'What are you doing?'
		sys.exit()
	
	# Add or update stages in workflow
	# Workflow.add_stage()
	for i in range(0, len(workflow_stages))
		stage_output = dxworkflow.addStage(editVersion = edit_version,
										   executable = applet_dxid,
										   name = stage_name, 
										   folder = output_folder,
										   input = input_dict,
										  )
		edit_version = stage_output['editVersion']
		stage_dxid = stage_output['stage']


	# Building workflow from scratch
	workflow_config = JSON.read(workflow_template) #pseudo-code
	if not workflow_object_dxid:
		workflow = dxpy.new_dxworkflow(title=workflow_name)
		for i in range(0, len(workflow['stages'])):
			# Add stage to workflow
			stage_dxid = workflow.add_stage()
			workflow_config['stages'][i]['dxid'] = stage_dxid
		for i in range(0, len(workflow['stages'])):
			for link_input in stage['link_input']:
				stage = link_input['stage']
				field = link_input['field']
				name = link_input['name']
			edit_version = workflow.describe()['editVersion']
			workflow.update_stage(stage=stage_dxid, ) 


		

if __name__ == "__main__":
	main() 
