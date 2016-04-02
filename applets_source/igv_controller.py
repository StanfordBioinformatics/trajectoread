#!/usr/bin/env python

class FlowcellLane:

    def __init__(self, record_dxid, dashboard_project_dxid='project-BY82j6Q0jJxgg986V16FQzjx'):

        self.dashboard_record_dxid = record_dxid
        self.dashboard_project_dxid = dashboard_project_dxid

        # Get reference genome information and dx references
        self.dashboard_record = dxpy.DXRecord(dxid = self.dashboard_record_dxid, 
                                              project = self.dashboard_project_dxid)

        self.details = self.dashboard_record.get_details()
        self.mapping_reference = self.details['mappingReference']
        self.project = self.details['laneProject']

class Genome:

    def __init__(self, name, project_dxid='project-BJJ0GQQ09Vv5Q7GKYGzQ0066'):

        self.name = name
        self.project_dxid = project_dxid

        self.fasta = self.find_ref_genome_rsc(rsc_name="genome.fa.gz")
        self.fai = self.find_ref_genome_rsc(rsc_name="genome.fai")
        self.bwa_index = self.find_ref_genome_rsc(rsc_name="bwa_index.tar.gz")
        self.object = self.find_ref_genome_rsc(rsc_name="genome", classname="record")

    def find_ref_genome_rsc(self, rsc_name, classname="file"):
        """
        Looks up a resource file (e.g., a BWA index file) or object for
        a given genome.
        """

        found = dxpy.find_one_data_object(classname = classname, 
                                          name = rsc_name,
                                          project = self.project_dxid,
                                          folder = "/" + self.name,
                                          zero_ok = False, 
                                          more_ok = False, 
                                          return_handler = True
                                         )
        print "Found %s object %s for genome %s" % (rsc_name, found.get_id(), self.name)
        return dxpy.dxlink(found)

def group_files_by_barcode(barcoded_files):
    """
    Group FASTQ files by sample according to their SampleID and Index
    properties. Returns a dict mapping (SampleID, Index) tuples to lists of
    files.
    Note - since I have casava outputting each barcode read in a single file, the value of each group should be a single file for single-end sequencing,
     or two files for PE sequencing.
    """
    
    print("Grouping files by barcode")
    dxfiles = [dxpy.DXFile(item) for item in barcoded_files]
    sample_dict = {}

    for dxfile in dxfiles:
        props = dxfile.get_properties()
        barcode =  props["barcode"] #will be NoIndex if non-multiplex (see bcl2fatq UG sectino "FASTQ Files")
        if barcode not in sample_dict:
            sample_dict[barcode] = []
        dxlink = dxpy.dxlink(dxfile)
        sample_dict[barcode].append(dxlink)
    print("Grouped barcoded files as follows:")
    print(sample_dict)
    return sample_dict

@dxpy.entry_point("main")
def main(bams, record_dxid, applet_build_version, applet_project, output_folder, properties=None):

    lane = FlowcellLane(record_dxid=record_dxid)
    ref_genome = Genome(name=lane.mapping_reference)
    applet_folder = '/builds/%s' % applet_build_version

    output = {
              'coverage_files' : [],
              'coverage_tracks': [],
              'mappings_tables': [],
              'tools_used': []
             }

    sample_dict = group_files_by_barcode(bams)
    for sample in sample_dict:

        bam_file = sample_dict[sample][0]
        properties = {'barcode': sample}

        ## Compute a coverage track
        cov_input = {
                     'bam_file': bam_file,
                     'genome_file': ref_genome.fai,
                     'sample_name': sample,
                     'output_project': lane.project,
                     'output_folder': output_folder,
                     'properties': properties 
                    }
        applet_id = dxpy.find_one_data_object(classname = 'applet',
                                                 name = 'compute_sample_coverage',
                                                 name_mode = 'exact',
                                                 project = applet_project,
                                                 folder = applet_folder,
                                                 zero_ok = False,
                                                 more_ok = False
                                                )
        cov_applet = dxpy.DXApplet(dxid=applet_id['id'], project=applet_id['project'])
        cov_job = cov_applet.run(cov_input)
        output['tools_used'].append(cov_job.get_output_ref('tools_used'))
        #output["coverage_file"] = cov_job.get_output_ref("coverage_file")
        output['coverage_files'].append(cov_job.get_output_ref('coverage_file'))

        '''
        ## DEV: WIG importer DOES NOT WORK. Trying "Mappings Coverage Track Generator"
        ##      as alternative
        ## Import the coverage track as a Wiggle/TrackSpec object
        cov_import_input = {
                            'file': cov_job.get_output_ref('coverage_file'),
                            'reference_genome': ref_genome.object,
                            'output_name': sample + ' coverage',
                            'output_project': lane.project,
                            'output_folder': output_folder,
                            'properties': properties 
                           }
        cov_import_job = dxpy.DXApp(name="wig_importer").run(cov_import_input)
        #output["coverage_track"] = cov_import_job.get_output_ref("wiggle")
        output['coverage_tracks'].append(cov_import_job.get_output_ref('wiggle')) 
        '''

               

        ## Import mappings for visualization
        bam_import_input = {
                            'bam': bam_file,
                            'reference_file': ref_genome.fasta,
                            'genome': ref_genome.object 
                           }
        applet_id = dxpy.find_one_data_object(classname = 'applet',
                                                 name = 'compressed_mappings_importer',
                                                 name_mode = 'exact',
                                                 project = applet_project,
                                                 folder = applet_folder,
                                                 zero_ok = False,
                                                 more_ok = False
                                                )
        mappings_applet = dxpy.DXApplet(dxid=applet_id['id'], project=applet_id['project'])
        bam_import_job = mappings_applet.run(bam_import_input)
        #output["mappings_table"] = bam_import_job.get_output_ref("reference_compressed_mappings")
        output['mappings_tables'].append(bam_import_job.get_output_ref('reference_compressed_mappings'))
    return output


dxpy.run()