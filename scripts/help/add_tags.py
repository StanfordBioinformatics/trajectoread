#!/usr/bin/env python
 
import sys
import dxpy

from itertools import chain

#sys.path.append('/Users/pbilling/Documents/GitHub/trajectoread/internal_resources/python_packages')
from scgpm_lims import Connection
from scgpm_lims import RunInfo

def get_experiment(id):

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
    return experiment_dict(id)

def get_organism(id):
    
    organisms = {
                 1: 'Human'
                }
    return organisms[id]

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
        name = project['describe']['name']
        print name
        elements = name.split('_')
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
        elif len(elements) == 6:
            tags = ['ENCODE', experiment_type, organism_name, project_name]
        '''
        dxpy.api.project_add_tags(
                                  object_id = project['id'],
                                  input_params = {tags: tags})
        '''

if __name__ == "__main__":
    main()