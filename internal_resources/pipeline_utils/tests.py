#!/usr/bin/env python

import imp
import os
import tempfile
import unittest

# Fancy import needed because pu doesn't have .py extension
pu = imp.load_source('pu', 'pu')

class PipelineUtilsTest(unittest.TestCase):

    def setUp(self):

        settings = {}
        settings['valid_environments'] = [
            'production',
            'staging',
            ]
        settings['projects'] = {
            # All the projects that need to exist
            'PIPELINE_PROJECT': "TestProject1"
            }


        settings['folders'] = {
            # This project needs to contain the folders in this list
            settings['projects']['PIPELINE_PROJECT']: {
                'APPLETS_FOLDER': 'Applets',
                'APPLETS_ARCHIVE': 'Applets/.Applet_archive',
                },
            }
        # environment, lims_url, and lims_token are stored as properties on this object
        settings['account_settings_object'] = settings['projects']['PIPELINE_PROJECT']

        # applets and resource bundle live here
        settings['applets_project'] = settings['projects']['PIPELINE_PROJECT']
        settings['applets_folder'] = settings['folders'][settings['applets_project']]['APPLETS_FOLDER']

        # For testing simple operations that don't need the
        # pipeline structure
        self.testproject = "TestProject2"

        self.pu = pu.PipelineUtilities(settings=settings)

    def test_verify_environment_no_project(self):
        with self.assertRaises(pu.VerifyEnvironmentException):
            self.pu.verify_environment('staging')

    def test_verify_environment_mismatch_setting(self):
        project = self.pu.settings['account_settings_object']
        self.pu.new_project(project)
        self.pu.set_property('environment', 'production', project=project)
        with self.assertRaises(pu.VerifyEnvironmentException):
            self.pu.verify_environment('staging')
        self.pu.delete_project(project)

    def test_verify_environment_success(self):
        project = self.pu.settings['account_settings_object']
        self.pu.new_project(project)
        self.pu.set_property('environment', 'staging', project=project)
        self.pu.verify_environment('staging')
        self.pu.delete_project(project)

    def test_get_set_cycle(self):
        project = self.pu.settings['account_settings_object']
        property = 'marco'
        value = 'polo'
        self.pu.new_project(project)
        self.pu.set_property(property, value, project=project)
        self.assertEquals(self.pu.get_property(property, project=project), value)
        self.pu.delete_project(project)

    def test_initialize_from_scratch(self):
        environment = 'staging'
        lims_url = 'http://www.th3in7erne7.gov'
        lims_token = '42'

        args = self.pu.parse_arglist(['init', '--environment', environment, '--lims_url', lims_url, '--lims_token', lims_token])
        args.func(args)

        self.assertEqual(self.pu.get_environment(), environment)
        self.assertEqual(self.pu.get_lims_url(), lims_url)
        self.assertEqual(self.pu.get_lims_token(), lims_token)

        for project in self.pu.settings['projects'].values():
            self.assertTrue(self.pu.does_project_exist(project))
            self.pu.delete_project(project)

    def test_set_lims_credentials(self):
        environment = 'staging'
        lims_url = 'http://www.th3in7erne7.gov'
        lims_token = '42'

        args = self.pu.parse_arglist(['init', '--environment', environment, '--lims_url', lims_url, '--lims_token', lims_token])
        args.func(args)

        # Now overwrite the original LIMS credentials
        new_lims_url = 'http://www.th30u7ergr055.gov'
        new_lims_token = '43'
        args = self.pu.parse_arglist(['set_lims_credentials', '--environment', environment, '--lims_url', new_lims_url, '--lims_token', new_lims_token])
        args.func(args, silent=True)

        self.assertEqual(self.pu.get_lims_url(), new_lims_url)
        self.assertEqual(self.pu.get_lims_token(), new_lims_token)

        for project in self.pu.settings['projects'].values():
            self.assertTrue(self.pu.does_project_exist(project))
            self.pu.delete_project(project)
        
    def test_upload_file(self):
        project = self.testproject
        file = tempfile.NamedTemporaryFile()
        self.pu.new_project(project)
        self.pu.upload_file(file=file.name, project=project)
        self.assertTrue(self.pu.does_file_exist(project=project, filename=os.path.basename(file.name)))
#        self.pu.delete_project(project)

if __name__ == '__main__':
    unittest.main()
