# This is a utility for building and interacting with the SCGPM
# sequencing center pipeline on DNAnexus.
#
# It is used by the Makefile, and can be used interactively as well.
#
# Run ./pu -h for usage information.

from argparse import ArgumentParser
import time
import subprocess
import datetime
import dxpy
import glob
import json
import os,sys
import re
import ast
from . import conf
import app_utils
from gbsc_utils import gbsc_utils

class VerifyEnvironmentException(Exception):
	pass
class ObjectNotFoundException(Exception):
	pass
class NewProjectException(Exception):
	pass
class DeleteProjectException(Exception):
	pass
class UploadException(Exception):
	pass
class NewFolderException(Exception):
	pass

devnull = open(os.devnull,'w')


class PipelineUtilities:
	#this class needs to be split up to separate the UHTS utils form the dnanexus pipeline utils.
	#I've already started the DxPipeline class above. I'll separate more out to that as time permis.

	
	def __init__(self,environment,lims_url=None,lims_token=None,skip_casava=False,verbose=False):
		self.verbose = verbose
		self.pu_dir = os.path.dirname(os.path.realpath(__file__))
		self.applets_dir = os.path.join(self.pu_dir, conf.appletsSrc)
		if self.verbose:
			sys.stderr.write("Applets directory in build_applets() is set to '{}'\n".format(self.applets_dir))
		self.environment = environment
		self.skip_casava = skip_casava
		self.lims_url = lims_url
		self.lims_token = lims_token
		#make sure environment is known environment (acceptable value)
		#If the value isn't one of the known values as specified in the conf.environments, an Exception of the type VerifyEnvironmentException
		self.isEnvironmentValid(self.environment)

		self.verify_dx_on_path()
		self.verify_logged_into_dx()	
		# ------- Low level commands that can be replaced by the DNAnexus python client if available ---------

	def get_property(self, property, path=None, project=None, folder=None):
		"""
		Arguments : path - The DNAnexus path to the project and project folder (if applicable)
		"""
		if path and (project or folder):
			raise Exception("Invalid settings. Don't specify the 'project' or 'folder' arguments when the 'path' argument is set.")
		if folder and not project:
			raise Exception("Invalid settings. The 'folder' argument must be specified when 'project' argument is set.")
		if not path:
			path = self.get_path(project=project, folder=folder)
		cmd = "dx describe '{path}' --json".format(path=path)
		p = subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdout,stderr = p.communicate()
		if p.returncode:
			raise ObjectNotFoundException("Failed to run command {cmd}.\nSTDOUT: {stdout}\nSTDERR: {stderr}".format(cmd=cmd,stdout=stdout,stderr=stderr))
		description = json.loads(stdout)
		properties = description['properties']
		return properties.get(property)


	def does_project_exist(self, project):
		cmd = 'dx select "{project}"'.format(project=project)
		if subprocess.call(cmd,shell=True,stdout=devnull, stderr=devnull) == 0:
			return True
		else:
			return False

	def does_file_exist(self, project=None, folder=None, filename=None):
		dest = self.get_path(project=project, folder=folder, filename=filename)
		cmd = 'dx ls "{dest}"'.format(dest=dest)
		if subprocess.call(cmd,shell=True,stdout=devnull, stderr=devnull) == 0:
			return True
		else:
			return False

	def new_project(self, project):
		# Don't do it if it already exists.
		if self.does_project_exist(project):
			return
		else:
			cmd = 'dx new project -s "{project}"'.format(project=project)
			p = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			stdout,stderr = p.communicate()
			if p.returncode:
				raise NewProjectException("Failed to create project {project} with command {cmd}.\n STDOUT: {stdout}\nSTDERR: {stderr}".format(project=project, cmd=cmd,stdout=stdout,stderr=stderr))

	def delete_project(self, project):
		cmd = 'dx rmproject -y "{project}"'.format(project=project)
		p = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdout,stderr = p.communicate()
		if p.returncode:
			raise DeleteProjectException("Failed to delete project {project} with command {cmd}. \n STDOUT: {stdout}\nSTDERR: {stderr}".format(project=project, cmd=cmd,stdout=stdout,stderr=stderr))

	def new_folder(self, folder, project):
		cmd = 'dx mkdir -p "{project}:{folder}"'.format(project=project, folder=folder)
		p = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdout,stderr = p.communicate()
		if p.returncode:
			raise NewFolderException("Failed to create folder {folder} in project {project} with command {cmd}. \n STDOUT: {stdout}\nSTDERR: {stderr}".format(folder=folder, project=project, cmd=cmd,stdout=stdout,stderr=stderr))

	def upload_file(self, infile, project, folder=None, destfilename=None):
		"""
		Function : Uses the upload agent (ua) from DNAnexus to upload the specified file to the specified project.
		Args     : infile - the file to upload.
							 project - the DNAnexus project (don't include the colon at the end).
							 folder - the sub-folder path within the project. May or may not include leading/trailing folder path separator '/'.
				destfilename - the remote name to use for infile. Default is same name as infile.
		"""
		folder = folder.strip("/")
		if destfilename:
			# destfilename lets you set a new filename after upload
			filename = destfilename
		else:
			filename = os.path.basename(infile)

		destination = self.get_path(project, folder, filename)
#      cmd = ['dx', 'upload',infile, '--path', destination]
		cmd = "ua --project '{project}'".format(project=project)
		if folder:
			cmd += " --folder /{folder}".format(folder=folder)
		if destfilename:
			cmd += " --name {destfilename}".format(destfilename=destfilename)
		cmd += " {infile}".format(infile=infile)
		if self.verbose:
			sys.stderr.write(cmd + "\n")
		popen = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdout,stderr = popen.communicate()
		if popen.returncode != 0: 
			raise UploadException("Failed to upload file '{infile}' to '{destination}'. The command was '{cmd}', and stderr was '{stderr}'.".format(infile=infile, destination=destination,cmd=cmd,stderr=stderr))

	def get_path(self, project=None, folder=None, filename=None):
		if not project:
			raise Exception('Cannot create path. Project is required. project=%s, folder=%s, filename=%s' % (project, folder, filename))
		dest = '%s:' % project
		if folder:
			folder = folder.lstrip("/")
			dest = os.path.join(dest,folder)
		if filename:
			dest = os.path.join(dest, filename)
		return dest

	def get_project_id(self, project_name):
		return dxpy.find_one_project(name=project_name)['id']

	def get_file_id(self, file_name, project_name, folder_name=None):
		project_id = self.get_project_id(project_name)
		if project_id is None:
			raise Exception("project not found %s" % project_name)
		file_iterator = dxpy.find_data_objects(project=project_id, name=file_name, folder="/%s" % folder_name, recurse=False)
		file_list = []
		for item in file_iterator:
			file_list.append(item)
		if len(file_list) == 0:
			raise Exception("Could not find %s in project %s" % (file_name, project_name))
		elif len(file_list) > 1:
			raise Exception("Found more than one file named %s in project %s" % (file_name, project_name))
		file_id = file_list[0]['id']
		return file_id

	def build_applet(self, applet, dest_project, dest_folder=None):
		localAppletPath = os.path.join(self.applets_dir,applet)
		if self.verbose:
			sys.stderr.write("Building applet '{}'.\n".format(localAppletPath))
		dest = self.get_path(project=dest_project, folder=dest_folder)+'/'
		cmd = 'dx build -y -f --destination "{dest}" {localAppletPath}'.format(dest=dest,localAppletPath=localAppletPath)
		print(cmd)
		stdout,stderr = gbsc_utils.createSubprocess(cmd=cmd,pipeStdout=True,checkRetcode=True)
		print(json.loads(stdout)['id']) #print the applet ID

	def remote_build_applet(self, applet, dest_project, dest_folder=None):
		#Archive applet if it exists. Needed because --archive is not supported for remote build.
		if self.does_file_exist(project=dest_project, folder=dest_folder, filename=applet):
			archive_filename = self._add_timestamp_to_filename(applet)
			self.move(src_project=dest_project, src_folder=dest_folder, src_filename=applet, dest_project=dest_project, dest_folder=conf.appletsArchiveFolder, dest_filename=archive_filename)
		dest = self.get_path(project=dest_project, folder=dest_folder)+'/'
		cmd = 'dx build -y --remote --destination "{dest}" {applet}'.format(dest=dest,applet=os.path.join(self.applets_dir, applet))
		print(cmd)
		stdout,stderr = gbsc_utils.createSubprocess(cmd=cmd,pipeStdout=True,checkRetcode=True)
		print(stdout)
		#print(json.loads(stdout)['id']) #print the applet ID

	def move(self, src_project, src_folder, src_filename, dest_project, dest_folder, dest_filename):
		src = self.get_path(project=src_project, folder=src_folder, filename=src_filename)
		dest = self.get_path(project=dest_project, folder=dest_folder, filename=dest_filename)
		cmd = 'dx mv "{src}" "{dest}"'.format(src=src,dest=dest)
		if self.verbose:
			print(cmd)
		subprocess.check_call(cmd,shell=True)

	def _add_timestamp_to_filename(self, filename):
		return '%s (%s)' % (filename, str(datetime.datetime.now()).replace(':', '\:'))

	# ----- Higher-level utilities that know something about our pipeline structure ------

	def verifyEnvAgainstAccount(self,environment):
		try:
			projectEnvironment = self.get_property(conf.environmentAttrName, project=self.get_path(conf.accountProject))
		except ObjectNotFoundException:
			raise VerifyEnvironmentException("Could not verify the environment because the DNAnexus Project '{projec}' was not found.".format(project=project))
		if projectEnvironment != environment:
			raise VerifyEnvironmentException("Environment '{environment}' does not match the environment property on the DNAnexus project '{project}', which is '{projectEnvironment}'.".format(environment=environment,project=project,projectEnvironment=projectEnvironment))

	def set_environment(self, environment):
		project = conf.accountProject
		self.set_property(conf.environmentAttrName, environment, path=self.get_path(project))

	def get_environment(self):
		"""
		Retrieves the environment property value from the DNAnexus project specified in the settings as the 'account_settings_project'
		"""
		project = conf.accountProject
		return self.get_property(conf.environmentAttrName, path=self.get_path(project))

	def set_lims_url(self, lims_url):
		"""
		Sets the lims_url property value on the DNAnexus project specified in the settings as the 'account_settings_project'.
		"""
		project = conf.accountProject
		path = self.get_path(project)
		self.set_property(property=conf.limsUrlAttrName, value=lims_url, path=path)

	def get_lims_url(self):
		"""
		Retrieves the lims_url property value from the DNAnexus project specified in the settings as the 'account_settings_project'.
		"""
		project = conf.accountProject
		return self.get_property(conf.limsUrlAttrName, path=self.get_path(project))      

	def set_lims_token(self, lims_token):
		"""
		Sets the lims_token property on the DNAnexus project specified in the settings as the 'account_settings_project'.
		"""
		project = conf.accountProject
		self.set_property(conf.limsTokenAttrName, lims_token, path=self.get_path(project))

	def get_lims_token(self):
		"""
		Retrieves the lims_token property from the DNANexux project specified in the settings as the 'account_settings_project'. 
		"""
		project = conf.accountProject
		return self.get_property(conf.limsTokenAttrName, path=self.get_path(project))      

	def are_lims_credentials_set(self):
		if self.get_lims_token() and self.get_lims_url():
			return True
		else:
			return False

	def verify_dx_on_path(self):
		retcode = subprocess.call("dx",shell=True,stdout=devnull,stderr=devnull) #shell returns an exit code of 127 if command not found
		if retcode == 127:
			raise Exception("dx toolkit is not on the path. Add it to the path before running. Return code for test comand \"dx\" was {retcode}".format(retcode=retcode))

	def verify_logged_into_dx(self):
		if subprocess.call("dx whoami",shell=True,stdout=devnull,stderr=devnull): #returns 3 if not logged in
			raise Exception("You must be logged into DNAnexus on the command-line using the \"dx toolkit\" in order to use this program. Try logging in with \"dx login\".")


	def get_reference_genomes_project_id(self):
		return self.get_project_id(conf.genomesProject)

	def get_resource_bundle_id(self):
		return self.get_file_id(
			file_name=conf.resourceBundleName,
			project_name=conf.accountProject,
			folder_name=conf.commonResourceBundleFolder
		)

	# ----- Methods called by the parser's subcommands, directly available to user -----

	def initialize_projects(self):
		"""
		Creates DNAnexus project folders if they don't already exist in the current DNAnexus account, as well as any subfolders, as specified in the settings object.
		If the project specified in the settings as the 'account_settings_project' didn't yet exist, creates it and tries to add the environment property to it, setting the value
		to that of self.environment which was specified when running this program. If that project already exists and the environment property isn't the same as that specified when running this program, then an Exception is raised.
     
		If either of the LIMS credentials (the properties lims_url and lims_token) aren't set on the account_settings_project (which is the case of this is the first time this project is being made), then the user
		will be prompted for each via standard input. Otherwise, if either of self.lims_url or self.lims_token are set, then the value of whichever is set (could be both) will be checked against the value of the corresponding properties     in the LIMS on the account settings object and an Exception will be raised if the values aren't the same.
		"""
		# Create projects and folders if they don't already exist
		# Add properties to the account_settings_project
		for project in conf.projects:
			project = conf.projects[project]
			projectName = project["name"]
			self.new_project(projectName)
			for folder in project["folders"].values():
				self.new_folder(folder, projectName)
				# Verify that commandline environment matches account environment
				# or set environment on account if it is not already set
		projectEnvironment = self.get_environment() #check that environment property is set on the account settings project in DNAnexus
		if not projectEnvironment:
			self.set_environment(self.environment)
		else:
			if projectEnvironment != self.environment:
				raise Exception("The environment property is already set to '{projectEnvironment}' on the DNAnexus project {project}, which is different from what was provided to this program ({environment}). This program isn't currently capable of resetting the environment property.".format(projectEnvironment=projectEnvironment,environment=self.environment,project=conf.accountProject)) 

		if not self.are_lims_credentials_set():
			if not self.lims_url:
				lims_url = raw_input("Enter the LIMS URL:\n")
			else:
				lims_url = self.lims_url

			if not self.lims_token:
				lims_token = raw_input("Enter the LIMS access token:\n")
			else:
				lims_token = self.lims_token

			self.set_lims_url(lims_url)
			self.set_lims_token(lims_token)
		else:
			if self.lims_url or self.lims_token:
				# If lims credentials are already set, but user provides settings, make sure they match
				lims_url = self.get_lims_url()
				lims_token = self.get_lims_token()
				if (lims_url != self.lims_url) or (lims_token != self.lims_token):
					raise Exception('Lims credentials are already set to lims_url=%s, lims_token=%s.' %(lims_url, lims_token) +
						'If you want to override those settings, use the set_lims_credentials subcommand.')

	def upload_resource_bundle(self, resource_bundle):
		self._archive_resource_bundle()
		self.upload_file(infile=resource_bundle, project=conf.accountProject, folder=conf.commonResourceBundleFolder)

	def _archive_resource_bundle(self):
		project = conf.accountProjectID #SCGPM Pipeline
		src_folder = conf.commonResourceBundleFolder #Applets
		src_filename = conf.resourceBundleName #scgpm_resources.tar.gz
		file_objects = list(dxpy.find_data_objects(project=project,name=src_filename,return_handler=True))
		for i in file_objects:
			app_utils.removeFile(projectid=project,fileid=i.id)

	def prepare_applets(self):
		# Look at all dxapp.json.template files in the applets directory,
		# look for bundledDepends, and insert the file id.
		# Write the new dxapp.json to disk

		dxapp_template_fns = glob.glob(os.path.join(self.applets_dir, '*/dxapp.json.template'))
		for dxapp_template_fn in dxapp_template_fns:
			print(dxapp_template_fn)
			output_filename = self._get_dxapp_filename(dxapp_template_fn)
			with open(dxapp_template_fn) as fh:
				dxapp = json.load(fh)

			self._update_bundle_id(dxapp)
			self._update_reference_genomes_project_id(dxapp)

			with open(output_filename, 'w') as fh:
				json.dump(dxapp, fh, sort_keys=True, indent=4)

	def _update_bundle_id(self, dxapp):
		"""
		Args : dxapp - a JSON dict. from a dxapp.json.template file.
		"""
		resource_bundle_id = self.get_resource_bundle_id()
		resource_bundle_name = conf.resourceBundleName

		runSpec = dxapp.get('runSpec')
		if not runSpec:
			return
		bundledDepends = runSpec.get('bundledDepends')
		if not bundledDepends:
			return
		for bd in bundledDepends:
			if bd['name'] == resource_bundle_name:
				bd['id']['$dnanexus_link'] = resource_bundle_id

	def _update_reference_genomes_project_id(self, dxapp):
		# Replace any inputSpec suggestions with name REFERENCE_GENOMES_PROJECT_NAME
		# with the correct project-id.

		reference_genomes_project_name = conf.genomesProject
		reference_genomes_project_id = self.get_project_id(reference_genomes_project_name)      

		inputSpecs = dxapp.get("inputSpec")
		if not inputSpecs:
			return
		for inputSpec in inputSpecs:
			suggestions = inputSpec.get('suggestions')
			if not suggestions:
				continue
			for suggestion in suggestions:
				if suggestion.get('name') == reference_genomes_project_name:
					suggestion['project'] = reference_genomes_project_id

	def _get_dxapp_filename(self, filename):
		# path/dxapp.json.template -> path/dxapp.json
		return re.sub('\.template$', '', filename)

	def build_applets(self):
		applets = self._get_applet_list()
		for applet in applets:
			time.sleep(3)
			self.build_applet(applet=applet, dest_project=conf.appletsProject, dest_folder=conf.appletsFolder)

	def _get_applet_list(self):
		dxapps = glob.glob(os.path.join(self.applets_dir, '*/dxapp.json'))
		applet_list = []
		for dxapp in dxapps:
			# Get the dirname that contains dxapp.json. This is the applet name.
			applet_list.append(os.path.dirname(dxapp).split('/')[-1])
		return applet_list

	def clean_applets(self):
		APPLETS_DIR = os.path.join(self.pu_dir, conf.appletsSrc)
		dxapps = glob.glob(os.path.join(APPLETS_DIR, '*/dxapp.json'))
		for dxapp in dxapps:
			try:
				os.remove(dxapp)
			except OSError:
				pass

	def set_lims_credentials(self,silent=False):
		self.set_lims_url(self.lims_url)
		self.set_lims_token(self.lims_token)
		if not silent:
			sys.stderr.write("LIMS credentials have been reset.\n")

	def get_lims_credentials(self):
		lims_url = self.get_lims_url()
		lims_token = self.get_lims_token()
		return lims_url,lims_token

	def isEnvironmentValid(self, environment):
		""" 
		Makes sure that expected_environment is one of the existing environments known to this script. If not, raises a ValueError.
		"""
		validEnvironments = conf.validEnvironments
		if not environment in validEnvironments:
			raise ValueError("Environment {environment} is not valid, must be one of {validEnvironments}.".format(environment=environment,validEnvironments=validEnvironments))
		return True 
