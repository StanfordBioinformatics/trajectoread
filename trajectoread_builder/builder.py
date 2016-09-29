#!/usr/bin/env python
'''
Description: This will build all the applets in the HiSeq4000_bcl2fastq workflow.
    For this pilot workflow, the only applet built will be bcl2fastq
Args: -t dxapp.template
Retuns: bcl2fastq applet, dxid of workflow object,
'''

import os
import re
import ast
import sys
import pdb
import dxpy
import json
import stat
import shutil
import fnmatch
import logging
import argparse
import datetime
import subprocess

from dxpy import app_builder

class WorkflowBuild:

    def __init__(self, workflow_config_path, project_dxid, dx_folder, 
                 path_list, dry_run):

        self.project_dxid = project_dxid
        self.dx_folder = dx_folder

        self.applet_dxids = {}
        
        self.name = None
        self.object = None
        self.object_dxid = None
        self.edit_version = None

        # Configure loggers for BuildWorkflow and Applet classes
        workflow_config_name = os.path.basename(workflow_config_path)
        self.workflow_logger = configure_logger(
                                                name = workflow_config_name, 
                                                source_type = 'BuildWorkflow',
                                                file_handle = True)
        '''
        applet_logger = configure_logger(
                                         name = workflow_config_name, 
                                         source_type = 'Applet',
                                         file_handle = True)
        '''

        # Logic for choosing applet path in DXProject; used by Applet:write_config_file()
        self.workflow_logger.info('Workflow path on DNAnexus will be: %s:%s' % (project_dxid, dx_folder))

        # Create workflow configuration object
        with open(workflow_config_path, 'r') as CONFIG:
            workflow_config = json.load(CONFIG)
        self.stages = workflow_config['stages']
        self.name = workflow_config['name']

        # Build all applets listed in workflow
        for stage_index in self.stages:
            applet_name = workflow_config['stages'][stage_index]['executable']
            #print applet
            
            applet = AppletBuild(
                                 applet_path = applet_name,
                                 project_dxid = project_dxid,
                                 dx_folder = dx_folder,
                                 dry_run = dry_run)

            self.applet_dxids[applet_name] = applet.dxid
            self.workflow_logger.info('Build complete: %s applet id: %s' % (applet, applet.dxid))
        
        # Create workflow 
        workflow_details = {
                            'name': workflow_config['name'],
                            'version': workflow_config['version'],
                            'date_created': str(datetime.datetime.now()).split()[0] # yyyy-mm-dd
                           }
        
        # Create DXWorkflow object on DNAnexus
        self.create_workflow_object(details=workflow_details)

        # Add executables to each workflow stage
        for stage_index in range(0, len(self.stages)):
            self.workflow_logger.info('Setting executable for stage %d' % stage_index)
            self.add_stage_executable(str(stage_index))

        # Add applet inputs to each workflow stage
        for stage_index in range(0, len(self.stages)):
            self.workflow_logger.info('Setting inputs for stage %d' % stage_index)
            self.set_stage_inputs(str(stage_index))
        
        dxpy.api.workflow_close(self.object_dxid)
        self.workflow_logger.info('Build complete: %s ,' % self.name +
                                  'workflow id: {%s, %s}' % (
                                                             self.project_dxid,
                                                             self.object_dxid))

    def create_workflow_object(self, environment=None, properties=None, details=None):
        ''' Description: In development environment, find and delete any old workflow
            object and create new one every time. If there is an existing workflow in
            the production environment, throw an error. Never delete an existing 
            production workflow or writing two production workflows to the same project 
            folder.
        '''

        '''
        # Find existing workflow(s) in project folder
        generator = dxpy.find_data_objects(classname = 'workflow',
                                           name = self.name,
                                           project = self.project_dxid,
                                           folder = self.dx_folder
                                           )
        existing_workflows = list(generator)
        
        # Remove old development workflow(s)
        if existing_workflows and environment in ['hotfix', 'develop']:
            for workflow in existing_workflows:
                self.logger.warning(
                                    'Removing existing development workflow: ' +
                                    '%s' % workflow)
                dxpy.remove(dxpy.dxlink(workflow))

        # Throw error if there is already workflow in production environment
        elif existing_workflows and environment == 'production':
            self.logger.error('Existing workflow(s) in production environment: '  +
                              'count: %s, ' % len(existing_workflows) +
                              'project: %s, ' % self.project_dxid + 
                              'path: %s, ' % path + 
                              'name: %s' % self.name)
            sys.exit()
        '''

        # Create new workflow
        self.object = dxpy.new_dxworkflow(title = self.name,
                                          name =  self.name,
                                          project = self.project_dxid,
                                          folder = self.dx_folder,
                                          properties = properties,
                                          details = details
                                          )
        self.object_dxid = self.object.describe()['id']

    def update_stage_executable(self, stage_index):
        ''' Description: Not in use since current strategy is to always create
            new workflow objects.
        '''

        self.edit_version = self.object.describe()['editVersion']
        
        output_folder = self.stages[stage_index]['folder']
        applet_name = self.stages[stage_index]['executable']
        applet_dxid = self.applet_dxids[applet_name]
        self.object.update_stage(
                                 stage = stage_index,
                                 edit_version = self.edit_version, 
                                 executable = applet_dxid, 
                                 folder = output_folder)

    def add_stage_executable(self, stage_index):

        self.edit_version = self.object.describe()['editVersion']
    
        output_folder = self.stages[stage_index]['folder']
        applet_name = self.stages[stage_index]['executable']
        #pdb.set_trace()
        applet_dxid = self.applet_dxids[applet_name]
        stage_dxid = self.object.add_stage(
                                           edit_version = self.edit_version,
                                           executable = applet_dxid,
                                           folder = output_folder)
        self.stages[stage_index]['dxid'] = stage_dxid

    def set_stage_inputs(self, stage_index):
        if not self.stages[stage_index]['dxid']:
            logger.error('Stage %s has not yet been created' % stage_index)
        stage_input = {}

        standard_inputs = self.stages[stage_index]['input']
        for name in standard_inputs:
            value = self.stages[stage_index]['input'][name]
            if value == '$dnanexus_link':
                print value
                project = value['project']
                dxid = value['id']
                dxlink = dxpy.dxlink(dxid, project)
                value = dxlink
            stage_input[name] = value

        linked_inputs = self.stages[stage_index]['linked_input']
        for field_name in linked_inputs:
            linked_input = linked_inputs[field_name]
            if type(linked_input) is dict:
                field_type = linked_input['field']
                input_stage_index = linked_input['stage']
                input_stage_dxid = self.stages[input_stage_index]['dxid']
                stage_input[field_name] = {'$dnanexus_link': {
                                                              'stage': input_stage_dxid,
                                                              field_type: field_name
                                                             }
                                          }
            elif type(linked_input) is list:
                stage_input[field_name] = []
                for list_input in linked_input:
                    #pdb.set_trace()
                    field_type = list_input['field']
                    input_stage_index = list_input['stage']
                    input_stage_dxid = self.stages[input_stage_index]['dxid']
                    stage_input[field_name].append({'$dnanexus_link': {
                                                                  'stage': input_stage_dxid,
                                                                  field_type: field_name
                                                                 }
                                                    })

        self.edit_version = self.object.describe()['editVersion']
        self.object.update_stage(stage = stage_index,
                                 edit_version = self.edit_version,
                                 stage_input = stage_input
                                )

class WorkflowConfig:

    def __init__(self, path_list, project_dxid, workflow_config, dx_folder,):
        ''' Dev: Eventually only rebuild applets/workflows if the applet source
                 has changed.
        '''

        self.logger = configure_logger(
                                       name = 'Workflow', 
                                       source_type = 'WorkflowConfig',
                                       path_list = path_list,
                                       file_handle = True)

        workflow_config
        
        # DEV: future project will be to just update exiting development workflows
        #self.new_workflow = True    # Always building new applets/workflows, now

        self.attributes = None
        self.object = None
        self.object_dxid = None
        self.edit_version = None

        self.stages = {}
        self.applets = {}

        ## Get workflow attributes - should be part of __init__ I think
        self.dx_login_check()
        self.read_workflow_template()

        if not self.project_dxid and not self.object_dxid:
            self.project_dxid = self.create_new_workflow_project()

    def dx_login_check(self):
        try:
            dxpy.api.system_whoami()
        except:
            self.logger.error('You must login to DNAnexus before proceeding ($ dx login)')
            sys.exit()

    def create_new_workflow_project(self):
        ''' Description: Only called if workflow project does not exist. Should only
            be used when chaning development framework.
        '''

        project_dxid = dxpy.api.project_new(input_params={'name' : self.name})['id']
        return project_dxid

    def read_workflow_template(self):
        
        with open(self.template_path, 'r') as CONFIG:
            self.attributes = json.load(CONFIG)

        self.applets = self.attributes['applets']
        self.stages = self.attributes['stages']

    def update_stage_executable(self, stage_index):
        ''' Description: Not in use since current strategy is to always create
            new workflow objects.
        '''

        self.edit_version = self.object.describe()['editVersion']
        
        output_folder = self.stages[stage_index]['folder']
        applet_name = self.stages[stage_index]['executable']
        applet_dxid = self.applets[applet_name]['dxid']
        self.object.update_stage(stage = stage_index,
                                 edit_version = self.edit_version, 
                                 executable = applet_dxid, 
                                 folder = output_folder
                                )

    def add_stage_executable(self, stage_index):

        self.edit_version = self.object.describe()['editVersion']
    
        output_folder = self.stages[stage_index]['folder']
        applet_name = self.stages[stage_index]['executable']
        applet_dxid = self.applets[applet_name]['dxid']
        stage_dxid = self.object.add_stage(edit_version = self.edit_version,
                                           executable = applet_dxid,
                                           folder = output_folder
                                          )
        self.stages[stage_index]['dxid'] = stage_dxid

    def set_stage_inputs(self, stage_index):
        if not self.stages[stage_index]['dxid']:
            logger.error('Stage %s has not yet been created' % stage_index)
        stage_input = {}

        '''
        standard_inputs = self.stages[stage_index]['input']
        for name in standard_inputs:
            if name == 'applet_build_version':
                version_label = get_version_label()
                self.stages[stage_index]['input']['applet_build_version'] = version_label
                stage_input[name] = version_label
            elif name == 'applet_project':
                self.stages[stage_index]['input']['applet_project'] = self.project_dxid
                stage_input[name] = self.project_dxid
        '''

        if self.stages[stage_index]['type'] == 'controller':
            worker_name = self.stages[stage_index]['worker_name']
            worker_id = self.applets[worker_name]['dxid']
            worker_project = self.project_dxid
            
            self.stages[stage_index]['input']['worker_id'] = worker_id
            self.stages[stage_index]['input']['worker_project'] = worker_project
            
            stage_input['worker_id'] = worker_id
            stage_input['worker_project'] = worker_project

        linked_inputs = self.stages[stage_index]['linked_input']
        ## DEV: Change linked input from dict to LIST of dicts. 
        ##      If length of linked_input == 1 stage_input = dict (as is)
        ##      Elif length of linked_input > 1 stage_input = list
        ##          append input of dicts
        for field_name in linked_inputs:
            linked_input = linked_inputs[field_name]
            if type(linked_input) is dict:
                field_type = linked_input['field']
                input_stage_index = linked_input['stage']
                input_stage_dxid = self.stages[input_stage_index]['dxid']
                stage_input[field_name] = {'$dnanexus_link': {
                                                              'stage': input_stage_dxid,
                                                              field_type: field_name
                                                             }
                                          }
            elif type(linked_input) is list:
                stage_input[field_name] = []
                for list_input in linked_input:
                    #pdb.set_trace()
                    field_type = list_input['field']
                    input_stage_index = list_input['stage']
                    input_stage_dxid = self.stages[input_stage_index]['dxid']
                    stage_input[field_name].append({'$dnanexus_link': {
                                                                  'stage': input_stage_dxid,
                                                                  field_type: field_name
                                                                 }
                                                    })

        self.edit_version = self.object.describe()['editVersion']
        self.object.update_stage(stage = stage_index,
                                 edit_version = self.edit_version,
                                 stage_input = stage_input
                                )

class AppletBuild:

    def __init__(self, applet_path, project_dxid, dx_folder, dry_run):

        self.logger = configure_logger(
                                       name = os.path.basename(applet_path), 
                                       source_type = 'AppletBuild',
                                       file_handle = True)
        self.dxid = None
        
        # GET VERSION INFO FROM dxapp.json FILE
        #print applet_path
        dxapp_path = os.path.join(applet_path, 'dxapp.json')
        with open(dxapp_path, 'r') as DXAPP:
            dxapp_json = json.load(DXAPP)  
            
        version = dxapp_json['details']['upstreamVersion']
        name = dxapp_json['name'] 
        dx_folder = os.path.join(dx_folder) 
        dx_applet_path = os.path.join(dx_folder, name)  

        # Ensure folder exists on DNAnexus
        dx_project = dxpy.DXProject(project_dxid)
        try:
            dx_project.new_folder(dx_folder)
        except:
            self.logger.info(
                             "Folder '%s'" % dx_folder +
                             " in project: '%s'" % project_dxid +
                             ' already exists.')

        build_args = [
                      'dx',
                      'build',
                      '%s' % applet_path, 
                      '-d=%s:%s' % (project_dxid, dx_applet_path),
                      '-f']
                      #'--remote']
        if dry_run:
            build_args.append('-n')
        self.logger.info(build_args)

        result = subprocess.check_output(build_args)
        result = ast.literal_eval(result)
        self.dxid = result['id']

        applet_name = os.path.basename(applet_path)
        if dry_run:
            self.logger.info(result)
        else:
            self.logger.info('Build complete: %s applet id: %s' % (name, result['id']))

class Applet:

    def __init__(self, name, version, path_list, branch, commit, logger=None):
        
        self.name = name
        self.version = version
        if logger:
            self.logger = logger
        else:
            self.logger = configure_logger(name = self.name, 
                                           source_type = 'Applet',
                                           path_list = path_list,
                                           file_handle = True
                                           )
        self.version_label = get_version_label()    # Used for Launchpad setup

        # Create applet details
        elements = self.version_label.split('_')
        date = elements[0]
        git_label = elements[1]
        self.details = {
                        'name': self.name,
                        'branch': branch,
                        'version': version,
                        'commit': commit,
                        'date_created': date,
                       }
        
        self.internal_rscs = []     # Filled by self.add_rsc()
        self.bundled_depends = []   # External resources
        # List of dictionaries: [{'filename':<filename>, 'dxid':<dxid>}, {...}, ...]

        ## Find applet code
        ## DEV: Change this to dynamically search for files with prefix matching name
        matching_files = []
        for source_file in os.listdir(path_list.applets_source):
            if fnmatch.fnmatch(source_file, '%s.*' % self.name):
                matching_files.append(source_file)
            else:
                pass

        if len(matching_files) == 1:
            code_basename = matching_files[0]
            self.logger.info('Found source file for %s: %s' % (self.name, code_basename))
        elif len(matching_files) == 0:
            self.logger.error('Could not find source file for %s' % self.name)
            sys.exit()
        elif len(matching_files) > 1: 
            self.logger.error('Found multiple source files for %s' % self.name)
            print matching_files
            sys.exit()

        self.code_path = os.path.join(path_list.applets_source, code_basename)
        # Find applet configuration file
        config_basename = self.name + '.template.json'
        self.config_path = os.path.join(path_list.applet_templates, config_basename)
        
        # Make applet directory structure because it is necessary for adding internal rscs
        # All directories are made in 'home' directory, which should usually be base of repo
        self.applet_path = '%s/%s/%s' % (path_list.launchpad, self.name, self.version_label)
        self.src_path = '%s/%s/%s/src' % (path_list.launchpad, self.name, self.version_label)
        self.rscs_path = '%s/%s/%s/resources' % (path_list.launchpad, self.name, self.version_label) 

        _make_new_dir(self.src_path)
        _make_new_dir(self.rscs_path)

        # Copy source code into applet directory
        shutil.copy(self.code_path, '%s/%s' % (self.src_path, code_basename))

class PathList:

    def __init__(self):
        self.home = os.path.dirname(os.path.abspath(__file__))
        #self.dnanexus_os = 'Ubuntu-12.04'
        
        # Specify relative directory paths. Depends on 'self.home'
        #self.applets_source = os.path.join(self.home, 'applets_source')
        #self.external_rscs = os.path.join(self.home, 'external_resources')
        #self.internal_rscs = os.path.join(self.home, 'internal_resources')
        #self.applet_templates = os.path.join(self.home, 'applet_config_templates')
        #self.workflow_config_templates = os.path.join(self.home, 'workflow_config_templates')
        self.launchpad = os.path.join(self.home, 'launchpad')
        #self.logs = os.path.join(self.builder, 'logs')
        
        # Specify relative file paths.
        self.build_json = os.path.join(self.home, 'builder.json')
        #self.applet_rscs = os.path.join(self.builder, 'applet_resources.json')
        #self.internal_rscs_json = os.path.join(self.internal_rscs, 'internal_resources.json')
        #self.external_rscs_json = os.path.join(self.external_rscs,
        #                                       self.dnanexus_os,
        #                                       'external_resources.json'
        #                                       )
    

    #def update_dnanexus_os(self, dnanexus_os):
    #    ''' Used by external_rscs_json '''
    #    self.dnanexus_os = dnanexus_os
    #    self.external_rscs_json = os.path.join(self.external_rscs, 
    #                                           self.dnanexus_os, 
    #                                           'external_resources.json'
    #                                          )

    def describe(self):
        self.__dict__

def parse_environment(path_list):
    ''' Description: First, reads in the possible build-environment 
    configurations currently supported, from the configuration 
    file: build_workflow.json. Then, determines the appropriate
    environment, based on the git branch. Returns dict with current
    build environment information.
    '''

    # Parse builer.json           
    with open(path_list.build_json, 'r') as CONFIG:
        build_config = json.load(CONFIG)

    # Get the current github branch, commit, and the latest version tag   
    try:     
        git_branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).rstrip()
        git_commit = subprocess.check_output(['git', 'describe', '--always']).rstrip()
        git_tag = subprocess.check_output(['git', 'describe', '--abbrev=0']).rstrip()
        version = git_tag
        git_branch_base = git_branch.split('-')[0]
    except:
        git_branch = None
        git_commit = None
        git_tag = None
        version = None
        git_branch_base = None

    if git_branch_base == 'master':
        project_dxid = build_config['workflow_projects']['production']['dxid']
        project_key = 'production'
    elif git_branch_base == 'develop':
        project_dxid = build_config['workflow_projects']['develop']['dxid']
        project_key = 'develop'
    else:
        project_dxid = build_config['workflow_projects']['develop']['dxid']
        project_key = 'develop'
        self.workflow_logger.warning(
                                     'Could not determine DXProject for branch: %s.' % git_branch,
                                     'Setting project to develop %s.' % project_dxid)

    environment_dict = {
                   'project_key': project_key,
                   'project_dxid': project_dxid,
                   'external_rscs_dxid': build_config['external_rscs_project']['dxid'],
                   'git_branch': git_branch,
                   'git_commit': git_commit,
                   'version': version,
                   'dx_OS': build_config['dnanexus_OS']
                  }
    return environment_dict

def configure_logger(source_type, name=None, file_handle=False):
    # Configure Logger object
    logger = logging.getLogger(source_type)    # Create logger object
    logger.setLevel(logging.DEBUG)

    timestamp = str(datetime.datetime.now()).split()[0]     # yyyy-mm-dd
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Add logging file handler
    if file_handle:
        file_handler_basename = 'builder_%s_%s_%s.log' % (name, source_type, timestamp)
        file_handler_path = os.path.join('logs', file_handler_basename)
        LOG = logging.FileHandler(file_handler_path)
        LOG.setLevel(logging.DEBUG)
        LOG.setFormatter(formatter)
        logger.addHandler(LOG)

    # Add logging stream handler
    STREAM = logging.StreamHandler(sys.stdout)
    STREAM.setLevel(logging.DEBUG)
    STREAM.setFormatter(formatter)
    logger.addHandler(STREAM)

    return logger

def get_version_label():
    timestamp = str(datetime.datetime.now()).split()[0] # yyyy-mm-dd
    git_commit = subprocess.check_output(['git', 'describe', '--always']).rstrip()
    version_label = '%s_%s' % (timestamp, git_commit)
    return version_label

def _make_new_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def parse_args(args):

    parser = argparse.ArgumentParser()
    parser.add_argument(
                        '-a', 
                        '--applet', 
                        dest='applet_path', 
                        type=str,
                        help='Name of applet folder.')
    parser.add_argument(
                        '-w', 
                        '--workflow', 
                        dest='workflow_path', 
                        type=str,
                        help='Path of workflow JSON file.')
    parser.add_argument(
                        '-e',
                        '--environment',
                        dest = 'env',
                        type = str,
                        help = 'Select DNAnexus environment [develop, production]')
    parser.add_argument(
                        '-d',
                        '--dry-run',
                        dest = 'dry_run',
                        action = 'store_true',
                        default = False,
                        help = 'Does not actually create applet or workflow')
    args = parser.parse_args(args)
    return(args)

def main():

    logger = configure_logger(source_type='Main')

    # Parse arguments
    args = parse_args(sys.argv[1:])
    logger.info('Args: %s' % args)
    
    if args.applet_path and args.workflow_path:
        logger.error(
                     'Applet and workflow arguments passed to builder. ' +
                     'Can only build one object at once')
        sys.exit()    
    elif not args.applet_path and not args.workflow_path:
        logger.error('No valid DNAnexus objects specified for building')
        sys.exit()

    if args.applet_path:
        type = 'applet'
    elif args.workflow_path:
        type = 'workflow'
    else:
        print 'No object type found to build'

    # Initiate path list and global resource manager objects
    path_list = PathList()
    #env_dict = parse_environment(path_list)

    # Read 'builder.json' configuration file for building workflows/applets
    with open('builder.json', 'r') as build_json:
        build_config = json.load(build_json)
        dnanexus_os = build_config['dnanexus_OS']
        project_dxid = build_config[type][args.env]['dxid']
        dx_folder = build_config[type][args.env]['folder']

    ## PARSE PROJECT & FOLDER INFO FROM build_config
    #sys.exit()

    # Create build object
    if args.applet_path:
        logger.info('Building applet: %s' % args.applet_path)
        builder = AppletBuild(
                              applet_path = args.applet_path,
                              project_dxid = project_dxid,
                              dx_folder = dx_folder,
                              dry_run = args.dry_run)
    elif args.workflow_path:
        logger.info('Building workflow: %s' % args.workflow_path)
        builder = WorkflowBuild(
                                workflow_config_path = args.workflow_path,
                                project_dxid = project_dxid,
                                dx_folder = dx_folder,
                                path_list = path_list,
                                dry_run = args.dry_run)
        
if __name__ == "__main__":
    main() 
