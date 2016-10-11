#!/usr/bin/env python

import re 
import sys
import dxpy

from itertools import chain

sys.path.append('/Users/pbilling/Documents/GitHub/trajectoread/internal_resources/python_packages')
from scgpm_lims import Connection
from scgpm_lims import RunInfo

def get_experiment(experiment_id):

    experiment_dict = {
                   1: "Whole-Genome DNA Fragments",
                   2: "Whole-Genome DNA Mate Pairs",
                   3: "Targeted Genomic DNA",
                   4: "ChIP-Seq Experiment",
                   5: "ChIP-Seq Control",
                   6: "Methyl-Seq",
                   7: "Whole-Transcript mRNA",
                   8: "3'-End-Biased mRNA",
                   9: "5'-End-Biased mRNA",
                   10: "Small RNA",
                   11: "Micro RNA",
                   12: "Total RNA",
                   13: "Other",
                   14: "Unknown",
                   15: "Whole-Transcript mRNA, Non-Directional",
                   16: "Whole-Transcript mRNA, Strand Specific",
                   17: "ATAC-Seq",
                   18: "Single-Cell",
                   19: "qPCR"
                  }
    return experiment_dict[experiment_id]

def get_organism(organism_id):
    
    organisms = {
                 1: 'Human'
                }
    return organisms[organism_id]

def add_project_tags(project_name, project_dxid, conn):

    print project_name
    elements = project_name.split('_')
    run_name = '_'.join(elements[0:4])
    lane = elements[4]
    match = re.search(r'L(\d)', lane)
    lane_index = match.group(1)

    # Lookup LIMS lane info
    run_info = RunInfo(conn=conn, run=run_name)
    lane_info = run_info.get_lane(lane_index)
    
    # Get experiment & organism id
    dna_library_id = lane_info['dna_library_id']
    dna_library_info = conn.getdnalibraryinfo(dna_library_id)
    experiment_type_id = dna_library_info['experiment_type_id']
    organism_id = dna_library_info['organism_id']

    # Add these functions
    experiment_type = get_experiment(experiment_type_id)
    organism_name = get_organism(organism_id)

    ## GET PROJECT NAMES

    # Determine whether project is production or release
    if len(elements) == 5:
        tags = ['ENCODE', experiment_type, organism_name, 'Production']
    elif len(elements) > 5:
        tags = ['ENCODE', experiment_type, organism_name]
    print tags
    dxid = dxpy.api.project_add_tags(
                                     object_id = project_dxid,
                                     input_params = {'tags':tags})
    return dxid

def main():
    '''Add tags to ENCODE projects.
    '''

    conn = Connection(
                      lims_url = 'https://uhts.stanford.edu',
                      lims_token = '9af4cc6d83fbfd793fe4')

    # Find projects name that match patterns:
    # SReq-
    # HEK-ZNF
    sreq_gen = dxpy.find_projects(
                                  name='*SReq-*', 
                                  name_mode='glob', 
                                  describe=True)
    znf_gen = dxpy.find_projects(
                                 name='*_HEK-ZNF*', 
                                 name_mode='glob',
                                 describe=True)

    project_gen = chain(sreq_gen, znf_gen)
    for project in project_gen:
        release_name = project['describe']['name']
        release_id = project['id']
        # Add tags to release project
        add_project_tags(release_name, release_id, conn)

        # Add tags to production project
        elements = release_name.split('_')
        production_name = '_'.join(elements[0:5])
        production = dxpy.find_one_project(
                                           name = production_name,
                                           more_ok = False,
                                           describe = True)
        add_project_tags(production_name, production['id'], conn)

if __name__ == "__main__":
    main()