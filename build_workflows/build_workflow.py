#!/usr/bin/env python
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
import argparse
import datetime
import subprocess

from dxpy import app_builder

class WorkflowConfig:

    def __init__(self, config_path, workflow_config_dir):
        ''' Dev: Eventually only rebuild applets/workflows if the applet source
                 has changed.
        '''
        self.config_path = config_path
        self.workflow_config_dir = workflow_config_dir
        
        self.new_workflow = True    # Always building new applets/workflows, now

        self.attributes = None
        self.project_dxid = None
        self.title = None
        self.name = None            # Future: name == title + version
        self.object = None
        self.object_dxid = None
        self.edit_version = None
        self.dx_OS = None
        self.external_rscs_dxid = None

        self.stages = {}
        self.applets = {}

        self.dx_login_check()
        self.get_workflow_attributes()

        if not self.project_dxid and not self.object_dxid:
            self.project_dxid = self.create_new_workflow_project()

    def dx_login_check(self):
        try:
            dxpy.api.system_whoami()
        except:
            print 'You must login to DNAnexus before proceeding ($ dx login)'
            sys.exit()

    def create_new_workflow_project(self):

        project_dxid = dxpy.api.project_new(input_params={'name' : self.name})['id']
        return project_dxid

    def get_workflow_attributes(self):
        
        with open(self.config_path, 'r') as CONFIG:
            self.attributes = json.load(CONFIG)
            
        self.project_dxid = self.attributes['workflow_project_dxid']
        self.title = self.attributes['workflow_title']
        self.name = self.attributes['workflow_title']
        self.object_dxid = self.attributes['workflow_dxid']
        self.edit_version = self.attributes['edit_version']
        self.dx_OS = self.attributes['dx_OS']
        self.external_rscs_dxid = self.attributes['external_rscs_dxid']

        self.applets = self.attributes['applets']
        self.stages = self.attributes['stages']

    def initialize_workflow(self):
        #if self.object_dxid:
        if self.new_workflow == False:
            self.object = dxpy.DXWorkflow(self.object_dxid)
        #elif not self.object_dxid:
        elif self.new_workflow == True:
            self.object = dxpy.new_dxworkflow(title = self.title,
                                              name =  self.title,
                                              project = self.project_dxid,
                                              folder = '/'
                                              )
            self.object_dxid = self.object.describe()['id']

    def set_stage_executable(self, stage_index):
        #pdb.set_trace()
        self.edit_version = self.object.describe()['editVersion']
        
        #if self.stages[stage_index]['dxid']:
        if self.new_workflow == False:
            output_folder = self.stages[stage_index]['folder']
            applet_name = self.stages[stage_index]['executable']
            applet_dxid = self.applets[applet_name]['dxid']
            self.object.update_stage(stage = stage_index,
                                     edit_version = self.edit_version, 
                                     executable = applet_dxid, 
                                     folder = output_folder
                                    )
        
        #elif not self.stages[stage_index]['dxid']:
        elif self.new_workflow == True:
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
            print 'Error: Stage %s has not been created' % stage_index
        stage_input = {}

        standard_inputs = self.stages[stage_index]['input']
        for name in standard_inputs:
            if name == 'applet_build_version':
                version_label = self.get_version_label()
                self.stages[stage_index]['input']['applet_build_version'] = version_label
                stage_input[name] = version_label
            elif name == 'applet_project':
                self.stages[stage_index]['input']['applet_project'] = self.project_dxid
                stage_input[name] = self.project_dxid

        linked_inputs = self.stages[stage_index]['linked_input']
        for name in linked_inputs:
            linked_input = linked_inputs[name]
            field_type = linked_input['field']
            field_name = linked_input['name']
            input_stage_index = linked_input['stage']
            input_stage_dxid = self.stages[input_stage_index]['dxid']
            stage_input[field_name] = {'$dnanexus_link': {
                                                          'stage': input_stage_dxid,
                                                          field_type: field_name
                                                         }
                                      }
        self.edit_version = self.object.describe()['editVersion']
        self.object.update_stage(stage = stage_index,
                                 edit_version = self.edit_version,
                                 stage_input = stage_input
                                )

    def update_config_file(self):
        ''' Description: Open workflow config file, get 'date_created' value,
                         and rename to 'external_rscs_<date_created>.json'
        '''
        
        # Check if there is an existing configuration file and archive it
        existing_config_path = self.config_path
        basename = os.path.basename(existing_config_path)
        name_elements = basename.split('.')
        filename = name_elements[0]
        filetype = name_elements[1]
        if os.path.isfile(existing_config_path):
            with open(existing_config_path, 'r') as EXIST:
                existing_json = json.load(EXIST)
            existing_date_created = existing_json['date_created']
            existing_config_archived = '%s_%s.%s' % (filename, 
                                                     existing_date_created, 
                                                     filetype)
            existing_config_archived_path = os.path.join(self.workflow_config_dir, 'archive',
                                                         existing_config_archived)
            os.rename(existing_config_path, existing_config_archived_path)
        
        # Create new JSON file
        self.attributes['date_created'] = str(datetime.datetime.now()).split()[0] # yyyy-mm-dd
        self.attributes['edit_version'] = self.edit_version
        self.attributes['workflow_dxid'] = self.object_dxid
        self.attributes['workflow_project_dxid'] = self.project_dxid

        with open(self.config_path, 'w') as CONFIG:
            json.dump(self.attributes, CONFIG, sort_keys=True, indent=4)

    def get_version_label(self):
        timestamp = str(datetime.datetime.now()).split()[0] # yyyy-mm-dd
        current_commit = self._get_git_commit().rstrip()
        version_label = '%s_%s' % (timestamp, current_commit)
        return version_label

    def _get_git_commit(self):
        # NOTE: not at all confident this is optimal solution
        commit = subprocess.check_output(['git', 'describe', '--always'])
        return commit 

class Applet:

    def __init__(self, name, project_dxid, repo_dirs):
        self.name = name
        self.project_dxid = project_dxid

        self.version_label = self.get_version_label()
        
        self.internal_rscs = []     # Filled by self.add_rsc()
        self.bundled_depends = []   # External resources
        # List of dictionaries: [{'filename':<filename>, 'dxid':<dxid>}, {...}, ...]

        ## Find applet code
        code_basename = self.name + '.py'
        self.code_path = os.path.join(repo_dirs['applets_source'], code_basename)
        ## Find applet configuration file
        config_basename = self.name + '.template.json'
        self.config_path = os.path.join(repo_dirs['applet_templates'], config_basename)
        
        # Make applet directory structure because it is necessary for adding internal rscs
        # All directories are made in 'home' directory, which should usually be base of repo
        self.applet_path = '%s/%s/%s' % (repo_dirs['applets'], self.name, self.version_label)
        self.src_path = '%s/%s/%s/src' % (repo_dirs['applets'], self.name, self.version_label)
        self.rscs_path = '%s/%s/%s/resources' % (repo_dirs['applets'], self.name, self.version_label) 

        self._make_new_dir(self.src_path)
        self._make_new_dir(self.rscs_path)

        # Copy source code into applet directory
        shutil.copy(self.code_path, '%s/%s' % (self.src_path, code_basename))

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
            self.internal_rscs.append(local_path)
        else:
            print 'Could not find internal applet rsc file: ' + local_path
            sys.exit() 

    def add_bundledDepends(self, filename, dxid):
        '''
        External rscs are stored and compiled remotely on DNAnexus and
        are added to an applet by specifying their DNAnexus file information
        in the bundledDepends attribute of runSpec in the configuration file.
        '''

        bundled_depends_dict = {'filename': filename, 'dxid': dxid}
        self.bundled_depends.append(bundled_depends_dict)

    def write_config_file(self, out_file='dxapp.json'):
        '''
        <Blurb about static vs dynamic attributes etc.>
        '''

        out_path = '%s/%s' % (self.applet_path, out_file)
        ## Load static configuration attributes from template file
        with open(self.config_path, 'r') as TEMPLATE:
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
        #   project='project-BkZ4jqj02j8X0FgQJbY1Y183', folder='/')
        #pdb.set_trace()
        #config_attributes['runSpec']['bundledDepends'].append(internal_resources[0])

        ## Eventually also update applet source code version in configuration file
        # FUTURE CODE

        ## Dump configuration attributes into new 'dxapp.json' file
        with open(out_path, 'w') as OUT:
            json.dump(config_attributes, OUT, sort_keys=True, indent=4)

        # If applet has internal resources, upload them and add to config file
        if len(self.internal_rscs) > 0:
            #pdb.set_trace()
            rscs_links = dxpy.app_builder.upload_resources(src_dir=self.applet_path, 
                                                           project=self.project_dxid, 
                                                           folder='/')
            config_attributes['runSpec']['bundledDepends'].append(rscs_links[0])
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

    def get_version_label(self):
        timestamp = str(datetime.datetime.now()).split()[0] # yyyy-mm-dd
        current_commit = self._get_git_commit().rstrip()
        version_label = '%s_%s' % (timestamp, current_commit)
        return version_label

class ExternalRscsManager:

    def __init__(self, project_dxid, local_dir, name='external_resources.json', os="Ubuntu-12.04"):
        self.local_dir = local_dir
        self.project_dxid = project_dxid
        self.filename = name
        self.basename = self.filename.split('.')[0]
        self.file_type = self.filename.split('.')[1]
        self.os = os
        self.dx_os = '/' + self.os  # All dnanexus paths must begin with '/'
        
        self.config_data = None
    
    def update(self):
        # Open local 'external_rscs.json', get 'date_created' value,
        # and rename to 'external_rscs_<date_created>.json'
        
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
                                                     existing_date_created, 
                                                     self.file_type
                                                    )
            existing_config_archived_path = os.path.join(self.local_dir, self.os, existing_config_archived)
            os.rename(existing_config_path, existing_config_archived_path)
        
        # Get dxid of remote external rscs configuration file
        updated_config_dxlink = dxpy.find_one_data_object(zero_ok=False, more_ok=False,
            name=self.filename, project=self.project_dxid, folder=self.dx_os)
        updated_config_dxid = updated_config_dxlink['id']
        # Download updated version of external rscs configuration file
        updated_config_path = os.path.join(self.local_dir, self.os, self.filename) 
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

    def add_applet_external_rscs(self, applet, external_rscs):
        ## Add applet external rscs to configuration file   
        for rsc in external_rscs:
            if 'version' in rsc:
                name = rsc['name']
                version = rsc['version']
                self.add_rsc_to_applet(applet, name, version)
            elif not 'version' in rsc:
                name = rsc['name']
                self.add_rsc_to_applet(applet, name)
            else:
                print 'How did you get here?'
                sys.exit()

class InternalRscsManager:
    ''' 
    Instead of having InternalResourceManager get the paths, have it handle
    all the aspects of adding rscs to the applet
        InternalrscManager.add_rsc_to_applet(applet, rsc_type, name, internal_rscs_path)
    
    DEV: I think I should hardcode more of this stuff to make it fixed, rather
    than trying to weave it through Workflow -> Stage objects. trajectoread
    represents a mix of dynamic static architecture.
    '''

    def __init__(self, config_file, internal_rscs_path):
        self.internal_rscs_path = internal_rscs_path
        with open(config_file, 'r') as CONFIG:
            self.config = json.load(CONFIG)

    def add_applet_internal_rscs(self, applet, internal_rscs):
        for rsc_type in internal_rscs:
            for rsc_name in internal_rscs[rsc_type]:
                self.add_rsc_to_applet(applet, rsc_type, rsc_name)

    def add_rsc_to_applet(self, applet, rsc_type, name):
        if rsc_type == 'python_packages':
            self._add_python_package(applet, rsc_type, name)
        else:
            local_path = self._get_local_path(rsc_type, name)
            dnanexus_path = self._get_dnanexus_path(rsc_type, name)
            applet.add_rsc(local_path, dnanexus_path)

    def _add_python_package(self, applet, rsc_type, name):
        print 'Adding python package %s to applet' % name
        package_files = self.config[rsc_type][name]["all_files"]
        for file in package_files:
            file_local_path = self._get_local_path(rsc_type, name) + '/' + file
            file_dnanexus_path = self._get_dnanexus_path(rsc_type, name) + '/' + file
            applet.add_rsc(file_local_path, file_dnanexus_path)

    def _get_local_path(self, rsc_type, name):
        relative_path = self.config[rsc_type][name]["local_path"]
        full_path = self.internal_rscs_path + '/' + relative_path
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

def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_json', dest='config_json', type=str, 
                        help='Basename of workflow JSON configuration file')
    args = parser.parse_args()
    return args

def main():

    args = parse_args()
    config_json = args.config_json

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
    workflow_config_basename = config_json
    workflow_config_path = os.path.join(trajectoread_dirs['workflow_config_templates'], 
                                        workflow_config_basename)
    workflow_config = WorkflowConfig(workflow_config_path, 
                                     trajectoread_dirs['workflow_config_templates']
                                    )

    #### Create resource manager objects ####
    internal_rscs_json = trajectoread_dirs['internal_rscs'] + '/internal_resources.json'
    internal_rsc_manager = InternalRscsManager(internal_rscs_json, 
                                               trajectoread_dirs['internal_rscs'])
    external_rsc_manager = ExternalRscsManager(workflow_config.external_rscs_dxid, 
                                               trajectoread_dirs['external_rscs'])

    for applet_name in workflow_config.applets:
        print 'Building %s applet' % applet_name
        applet = Applet(name=applet_name, 
                        project_dxid=workflow_config.project_dxid,
                        repo_dirs=trajectoread_dirs
                       )
        internal_rscs = workflow_config.applets[applet_name]['internal_rscs']
        internal_rsc_manager.add_applet_internal_rscs(applet=applet, 
                                                      internal_rscs=internal_rscs, 
                                                     )
        external_rscs = workflow_config.applets[applet_name]['external_rscs']
        external_rsc_manager.add_applet_external_rscs(applet=applet, 
                                                      external_rscs=external_rscs)
        applet.write_config_file()
        applet_dxid = applet.build(project_dxid=workflow_config.project_dxid)
        workflow_config.applets[applet_name]['dxid'] = applet_dxid

    ''' DEV: Create workflow object and add stages.
        ! Issue when you create new applets and then try to initialize
        existing workflow with missing applets.
        - Current solution is just to rebuild everything everytime
    '''
    workflow_config.initialize_workflow()
    for stage_index in range(0, len(workflow_config.stages)):
        print 'Setting executable for stage %d' % stage_index
        workflow_config.set_stage_executable(str(stage_index))

    for stage_index in range(0, len(workflow_config.stages)):
        print 'Setting inputs for stage %d' % stage_index
        workflow_config.set_stage_inputs(str(stage_index))

    workflow_config.update_config_file()
        
if __name__ == "__main__":
    main() 
