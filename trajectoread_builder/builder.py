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

class PathList:

    def __init__(self):
        self.home = os.path.dirname(os.path.abspath(__file__))
        #self.dnanexus_os = 'Ubuntu-12.04'
        
        # Specify relative directory paths. Depends on 'self.home'
        self.launchpad = os.path.join(self.home, 'launchpad')

        # Specify relative file paths.
        self.build_json = os.path.join(self.home, 'builder.json')

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

    # Read 'builder.json' configuration file for building workflows/applets
    with open('builder.json', 'r') as build_json:
        build_config = json.load(build_json)
        dnanexus_os = build_config['dnanexus_OS']
        project_dxid = build_config[type][args.env]['dxid']
        dx_folder = build_config[type][args.env]['folder']

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
