#!/usr/bin/env python

import sys
import dxpy
import argparse
import datetime

def leave_delivered_project(project_info, dry_run=False):
    created_by_me = False
    owned_by_scgpm = True
    owned_by_me = True
    
    description = project_info['describe']

    if description['billTo'] != 'org-scgpm':
        owned_by_scgpm = False
    if description['billTo'] != 'user-pbilling':
        owned_by_me = False
    if description['createdBy']['user'] == 'user-pbilling':
        created_by_me = True
    if created_by_me and not owned_by_me and not owned_by_scgpm:
        if not dry_run:
            dxpy.api.project_leave(description['id'])
        return('Leaving project %s ' % description['name'] + 
               'owned by %s' % description['billTo'])

def destroy_pending_transfer_project(project_info, dry_run=False):
    created_by_me = False
    owned_by_scgpm = False
    pending_transfer = False
    
    description = project_info['describe']
    
    if description['createdBy']['user'] == 'user-pbilling':
        created_by_me = True
    if description['billTo'] == 'org-scgpm':
        owned_by_scgpm = True
    if description['pendingTransfer']:
        pending_transfer = True

    if created_by_me and owned_by_scgpm and pending_transfer:
        if not dry_run:
            dxpy.api.project_leave(description['id'])
        return('Destroying project %s ' % description['name'] + 
               'pending transfer to %s' % description['pendingTransfer'])

def remove_file(file_info, dry_run=False):
    project_id = file_info['project']
    file_id = file_info['id']
    size = file_info['describe']['size']
    name = file_info['describe']['name']
    project_info = dxpy.describe(project_id)
    if project_info['billTo'] == 'org-scgpm':
        file_link = {'$dnanexus_link': {'project': project_id, 'id': file_id}}
        if not dry_run:
            dxpy.remove(file_link)
        return(file_id, project_id, size, name)

def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument(
                        "--created-before",
                        "-b",
                        type=str,
                        help="Find objects created before date. i.e. 2016-04-02")
    parser.add_argument(
                        "--created-after",
                        "-a",
                        type=str,
                        help="Find objects created after date. i.e. 2016-04-03")
    parser.add_argument(
                        "--cron",
                        "-c",
                        action="store_true",
                        help="Flag indicates that script is being run periodically "
                             "by cron job. Overwrites 'created-before' arg.")
    parser.add_argument(
                        "--dry-run",
                        "-d",
                        action="store_true",
                        help="Flag indicates not to change any DNAnexus objects")
    if len(args) < 1:
        parser.print_help()
        sys.exit()
    else:
        return parser.parse_args(args)

def main():

    args = parse_args(sys.argv[1:])
    print args

    # Specify --created-before DNAnexus parameter
    if args.cron:
        # Set parameters for automatic removal of data from DNAnexus
        before = datetime.datetime.now() - datetime.timedelta(days=45)
        before_date_str = '%d-%d-%d' % (before.year, before.month, before.day)

        source_before = datetime.datetime.now() - datetime.timedelta(days=90)
        source_before_date_str = '%d-%d-%d' % (source_before.year, 
                                               source_before.month,
                                               source_before.day)
    elif args.created_before:
        before_date_str = args.created_before
        source_before_date_str = args.created_before
    else:
        # Default is to remove anything created over 2 months ago
        before = datetime.datetime.now() - datetime.timedelta(days=60)
        before_date_str = '%d-%d-%d' % (before.year, before.month, before.day)

    # Specify --created-after DNAnexus parameter
    if args.created_after:
        after_date_str = args.created_after
    else:
        after_date_str = '2016-04-04' # Launch date

    # Dry run will log would-be changes, but not make them
    if args.dry_run:
        dry_run = True
    else:
        dry_run = False

    print "-------"
    print "Removing DNAnexus data with following parameters..."
    print "-------"
    print "Data created before: %s" % before_date_str
    print "Source data created before: %s" % source_before_date_str
    print "Data created after: %s" % after_date_str
    print "Dry run: %s" % str(dry_run)

    # Check to make sure before data is not within 31 days
    date_elements = before_date_str.split('-')
    before_date = datetime.datetime(
                                    year = int(date_elements[0]),
                                    month = int(date_elements[1]),
                                    day = int(date_elements[2]))
    min_before_date = datetime.datetime.now() - datetime.timedelta(days=31)
    if before_date > min_before_date:
        print "Error: '--created-before' date cannot be within 31 days of current date."
        sys.exit()

    # Leave delivered projects and remove pending transfers
    print 'Info: Leaving delivered projects and removing pending transfers'
    project_counter = 0
    project_generator = dxpy.find_projects(
                                           name = '16*', 
                                           name_mode = 'glob', 
                                           level = 'ADMINISTER', 
                                           describe = True, 
                                           created_after = after_date_str, 
                                           created_before = before_date_str)
    for project in project_generator:
        project_counter += 1
        #print project['describe']['name']
        #leave_result = leave_delivered_project(project, dry_run)
        #if leave_result:
        #    print leave_result
        #destroy_result = destroy_pending_transfer_project(project, dry_run)
        #if destroy_result:
        #   print destroy_result
        leave_result = None
        destroy_result = None
        if not leave_result and not destroy_result:
            project_counter +=1
        if leave_result and destroy_result:
            print 'Error: left project and then destroyed it.'
            sys.exit()
    print 'Total count of projects: %d' % project_counter

    total_size_removed_gb = 0
    
    # Remove old BAM files
    print 'Info: Removing old bam files'
    bams_size_removed = 0
    bams_file_count = 0
    bam_generator = dxpy.find_data_objects(
                                  classname = 'file', 
                                  name = "SCGPM*.bam", 
                                  name_mode = "glob", 
                                  created_after = after_date_str,
                                  created_before = before_date_str, 
                                  describe=True)
    for bam in bam_generator:
        result = remove_file(bam, dry_run)
        if result:
            bams_file_count += 1
            bams_size_removed += int(result[2])
    bams_size_removed_gb = bams_size_removed / 1000000000
    print 'Count of bams removed: %d' % bams_file_count
    print 'Total size of bams removed: %d GB' % bams_size_removed_gb
    total_size_removed_gb += bams_size_removed_gb

    # Remove old BAI files
    print 'Info: Removing old bai files'
    bais_size_removed = 0
    bais_file_count = 0
    bai_generator = dxpy.find_data_objects(
                                  classname = 'file', 
                                  name = "SCGPM*.bai", 
                                  name_mode = "glob", 
                                  created_after = after_date_str,
                                  created_before = before_date_str, 
                                  describe=True)
    for bai in bai_generator:
        result = remove_file(bai, dry_run)
        if result:
            bais_file_count += 1
            bais_size_removed += int(result[2])
    bais_size_removed_gb = bais_size_removed / 1000000000
    print 'Count of bais removed: %d' % bais_file_count
    print 'Total size of bais removed: %d GB' % bais_size_removed_gb
    total_size_removed_gb += bais_size_removed_gb

    # Remove old source files
    print 'Info: Removing source files'
    # Add an extra 30 days for source files. There is no coming back from this.
    source_size_removed = 0
    source_file_count = 0
    source_generator = dxpy.find_data_objects(
                                              classname = 'file',
                                              name = '.+_L\d.tar',
                                              name_mode = "regexp",
                                              created_after = after_date_str,
                                              created_before = source_before_date_str,
                                              describe = True)
    for source in source_generator:
        result = remove_file(source, dry_run)
        if result:
            #print result[3]    # Sanity checking
            source_file_count += 1
            source_size_removed += int(result[2])
    source_size_removed_gb = source_size_removed / 1000000000
    print 'Count of source files removed: %d' % source_file_count
    print 'Total size of source files removed: %d GB' % source_size_removed_gb
    total_size_removed_gb += source_size_removed_gb


    # Remove old FastQC files
    print 'Info: Removing old fastqc files'
    fastqc_size_removed = 0
    fastqc_file_count = 0
    fastqc_generator = dxpy.find_data_objects(
                                              classname = 'file', 
                                              name = ".+_fastqc_.+\.zip", 
                                              name_mode = "regexp",
                                              created_after = after_date_str, 
                                              created_before = before_date_str, 
                                              describe = True)
    for fastqc in fastqc_generator:
        result = remove_file(fastqc, dry_run)
        if result:
            fastqc_file_count += 1
            fastqc_size_removed += int(result[2])
    fastqc_size_removed_gb = fastqc_size_removed / 1000000000
    print 'Count of FastQC files removed: %d' % fastqc_file_count
    print 'Total size of FastQC files removed: %d GB' % fastqc_size_removed_gb
    total_size_removed_gb += fastqc_size_removed_gb

    # Sum approximate total size removed
    print 'Total size of files removed: %d GB' % total_size_removed_gb

if __name__ == '__main__':
    main()

