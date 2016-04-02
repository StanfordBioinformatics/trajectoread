#!/usr/bin/env python
'''
Description: Upload multiple files within a source directory to DNAnexus.
Author: Paul Billing-Ross
Creation date: April 2, 2016
'''

import os
import sys
import fnmatch
import argparse
import subprocess

def parse_arguments():

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--name', required=True, dest='name',
                        help='Name of file(s) to upload')
    parser.add_argument('-d', '--directory', required=True, dest='src_dir',
                        help='Path of directory to with file(s) to upload')
    parser.add_argument('-p', '--project_dxid', required=True, dest='dxid',
                        help='Project dxid to upload files to')
    parser.add_argument('-m', '--name_mode', required=False, dest='name_mode', 
                        default='exact', 
                        help='"exact": exact match, ' + 
                             '"glob": use "*" and "?" as wildcards, ' +
                             '"regexp": interpret as regular expression')
    parser.add_argument('-f', '--folder', required=False, dest='folder', default='',
                        help='Destination DXProject folder to upload files into')
    parser.add_argument('-a', '--auth_token', required=True, dest='auth_token',
                        help='DNAnexus authorization token')
    parser.add_argument('-u', '--upload_agent_path', required=False, dest='ua_path',
                        default='../../upload_agent/1.5.11/ua', 
                        help='Manually specify path of upload agent ("ua")')
    args = parser.parse_args()
    return args

def call_subprocess(command):
    '''
    try:
        subprocess.check_call(command, shell=True, stderr=sys.stderr, 
                              stdout=sys.stdout)
    except subprocess.CalledProcessError as e:
        print 'Error: subprocess command "%s" died: %s' % (e.cmd, e)
        raise
    '''
    #subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
    try:
        subprocess.check_call(command, 
                              shell=True, 
                              stderr=sys.stderr, 
                              stdout=sys.stdout)
    except:
        print '\nError: Command failed: "%s"\n' % command

def main():

    args = parse_arguments()

    name = args.name
    src_dir = args.src_dir
    mode = args.name_mode
    project_dxid = args.dxid
    project_folder = args.folder
    auth_token = args.auth_token
    ua_path = args.ua_path

    if mode == 'exact':
        file_path = os.path.join(src_dir, name)
        ua_command = '%s ' % ua_path
        ua_command += '-a %s ' % auth_token
        ua_command += '-p %s ' % project_dxid
        ua_command += '-f /%s ' % project_folder
        ua_command += '%s' % file_path
        call_subprocess(ua_command)

    elif mode == 'glob':
        for file in os.listdir(src_dir):
            if fnmatch.fnmatch(file, name):
                file_path = os.path.join(src_dir, file)
                ua_command = '%s ' % ua_path
                ua_command += '-a %s ' % auth_token
                ua_command += '-p %s ' % project_dxid
                ua_command += '-f /%s ' % project_folder
                ua_command += '%s' % file_path
                call_subprocess(ua_command)

    elif mode == 'regexp':
        print 'Warning: "regexp" mode is UNTESTED.'
        print 'Implementation: https://docs.python.org/2/library/fnmatch.html'
        regex = fnmatch.translate(name)
        reobj = re.compile(regex)
        for file in os.listdir(src_dir):
            if reobj.match(name):
                file_path = os.path.join(src_dir, file)
                ua_command = '%s ' % ua_path
                ua_command += '-a %s ' % auth_token
                ua_command += '-p %s ' % project_dxid
                ua_command += '-f %s ' % project_folder
                ua_command += '%s' % file_path
                call_subprocess(ua_command)

if __name__ == '__main__':
    main()
