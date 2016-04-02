#!/usr/bin/env python
'''
Description: Upload multiple files within a source directory to DNAnexus.
Author: Paul Billing-Ross
Creation date: April 2, 2016
'''

import os
import argparse

def parse_arguments():

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--name', required=False, dest='name',
                        help='Name of file(s) to upload')
    parser.add_argument('-d', '--directory', required=True, dest='src_dir',
                        help='Path of directory to with file(s) to upload')
    parser.add_argument('-m', '--name_mode', required=False, dest='mode',
                        help='"exact": exact match, ' + 
                             '"glob": use "*" and "?" as wildcards, ' +
                             '"regexp": interpret as regular expression')
    parser.add_argument('-p', '--project_dxid', required=True, dest='dxid',
                        help='Project dxid to upload files to')
    parser.add_argument('-f', '--folder', required=False, dest='folder',
                        help='Destination DXProject folder to upload files into')
    args = parser.parse_args()
    return args

def main():

    auth_token = 'rk03sdQHj9EqSWOaW0v7vh3b78zQPbYW'

    args = parse_arguments()
    #print args

    name = args.filename
    src_dir = args.src_dir
    mode = args.name_mode
    project_dxid = args.dxid
    project_folder = args.folder

    if mode == 'exact':
        file_path = os.path.join(src_dir, name)
        ua_command = 'ua -a %s ' % auth_token
        ua_command += '-p %s ' % project_dxid
        ua_command += '-f %s ' % project_folder
        ua_command += '%s' % file_path

    elif mode == 'glob':
        for file in os.listdir(src_dir):
            if fnmatch.fnmatch(file, name):
                file_path = os.path.join(src_dir, file)
                ua_command = 'ua -a %s ' % auth_token
                ua_command += '-p %s ' % project_dxid
                ua_command += '-f %s ' % project_folder
                ua_command += '%s' % file_path

    elif mode == 'regexp':
        print 'Warning: "regexp" mode is UNTESTED.'
        print 'Implementation: https://docs.python.org/2/library/fnmatch.html'
        regex = fnmatch.translate(name)
        reobj = re.compile(regex)
        for file in os.listdir(src_dir):
            if reobj.match(name):
                file_path = os.path.join(src_dir, file)
                ua_command = 'ua -a %s ' % auth_token
                ua_command += '-p %s ' % project_dxid
                ua_command += '-f %s ' % project_folder
                ua_command += '%s' % file_path


if __name__ == '__main__':
    main()