#!/usr/bin/env python

"""
Compute QC metrics on a sample.
"""

import os
import subprocess
import csv
import json
from multiprocessing import cpu_count
import dxpy

ALIGNERS = {'bwa_aln': 0, 'bwa_mem': 1, 'Bowtie2': 2}

def run_cmd(cmd, logger, shell=True):
    if shell:
        save_cmd = cmd
    else:
        save_cmd = subprocess.list2cmdline(cmd)
    logger.append(save_cmd)
    print save_cmd
    subprocess.check_call(cmd, shell=shell)

def get_app_title():
    cmd = "dx describe `dx describe ${DX_JOB_ID} --json | jq -r '.applet'` --json | jq -r '.title'"
    title = subprocess.check_output(cmd, shell=True).strip()
    return title

def extract_json_from_asm(fname):
    """Parse the output of CollectAlignmentSummaryMetrics and return
    it in a dict."""

    temp_fn = 'temp.asm'
    output = {'Read 1': {'Total Reads': 0, 'Post-Filter Reads': 0, 'Mapped PF Reads': 0},
              'Read 2': {'Total Reads': 0, 'Post-Filter Reads': 0, 'Mapped PF Reads': 0},
              'Pair': {'Total Reads': 0, 'Post-Filter Reads': 0, 'Mapped PF Reads': 0}}

    # First strip off all comment lines.
    with open(fname) as in_file:
        with open(temp_fn, 'w') as out_file:
            for line in in_file:
                if (len(line.strip()) > 0) and (line.strip()[0] != '#'):
                    out_file.write(line)

    with open(temp_fn) as in_file:
        reader = csv.reader(in_file, delimiter='\t')
        col_names = reader.next()
        col_names = dict([(name, i) for i, name in enumerate(col_names)])
        for line in reader:
            if line[col_names['CATEGORY']] == 'FIRST_OF_PAIR':
                output['Read 1']['Total Reads'] += int(line[col_names['TOTAL_READS']])
                output['Read 1']['Post-Filter Reads'] += int(line[col_names['PF_READS']])
                output['Read 1']['Mapped PF Reads'] += int(line[col_names['PF_READS_ALIGNED']])
            elif line[col_names['CATEGORY']] == 'SECOND_OF_PAIR':
                output['Read 2']['Total Reads'] += int(line[col_names['TOTAL_READS']])
                output['Read 2']['Post-Filter Reads'] += int(line[col_names['PF_READS']])
                output['Read 2']['Mapped PF Reads'] += int(line[col_names['PF_READS_ALIGNED']])
            elif line[col_names['CATEGORY']] == 'PAIR':
                output['Pair']['Total Reads'] += int(line[col_names['TOTAL_READS']])
                output['Pair']['Post-Filter Reads'] += int(line[col_names['PF_READS']])
                output['Pair']['Mapped PF Reads'] += int(line[col_names['PF_READS_ALIGNED']])
            elif line[col_names['CATEGORY']] == 'UNPAIRED':
                # For unpaired reads we store counts as "read 1"
                output['Read 1']['Total Reads'] += int(line[col_names['TOTAL_READS']])
                output['Read 1']['Post-Filter Reads'] += int(line[col_names['PF_READS']])
                output['Read 1']['Mapped PF Reads'] += int(line[col_names['PF_READS_ALIGNED']])

    # If we are analyzing paired-end reads, do a sanity check that the
    # paired-end counts are the sum of the read 1 and read 2 counts.
    if output['Pair']['Total Reads'] != 0:
        assert (output['Pair']['Total Reads'] ==
                (output['Read 1']['Total Reads'] + output['Read 2']['Total Reads'])), \
            'Total Reads failed PE sanity check.'
        assert (output['Pair']['Post-Filter Reads'] ==
                (output['Read 1']['Post-Filter Reads'] + output['Read 2']['Post-Filter Reads'])), \
            'Post-Filter Reads failed PE sanity check.'
        assert (output['Pair']['Mapped PF Reads'] ==
                (output['Read 1']['Mapped PF Reads'] + output['Read 2']['Mapped PF Reads'])), \
            'Mapped PF Reads failled PE sanity check.'

    output['Pair']['Failed Reads'] = output['Pair']['Total Reads'] - output['Pair']['Post-Filter Reads']

    return output

def extract_json_from_ism(fname):
    """Parse the output of CollectInsertSizeMetrics and return it in a
    dict."""

    temp_fn = 'temp.ism'
    output = {}

    # First strip off all comment lines.
    with open(fname) as in_file:
        with open(temp_fn, 'w') as out_file:
            for line in in_file:
                if (len(line.strip()) > 0) and (line.strip()[0] != '#'):
                    out_file.write(line)

    with open(temp_fn) as in_file:
        reader = csv.reader(in_file, delimiter='\t')
        col_names = reader.next()
        col_names = dict([(name, i) for i, name in enumerate(col_names)])
        data = reader.next()
        output['Median Insert Size'] = int(data[col_names['MEDIAN_INSERT_SIZE']])
        output['Mean Insert Size'] = float(data[col_names['MEAN_INSERT_SIZE']])
        output['Median Absolute Deviation'] = int(data[col_names['MEDIAN_ABSOLUTE_DEVIATION']])
        output['Standard Deviation'] = float(data[col_names['STANDARD_DEVIATION']])
        output['Minimum Insert Size'] = int(data[col_names['MIN_INSERT_SIZE']])
        output['Maximum Insert Size'] = int(data[col_names['MAX_INSERT_SIZE']])

        # Now sum each observation in the insert size histogram. This will
        # be used later to calculate the average overall insert size across
        # multiple bams.
        output['Total Insert Reads'] = 0
        seen_header = False
        for data in reader:
            if len(data) >= 2:
                if (not seen_header) and (data[0].strip() == 'insert_size'):
                    seen_header = True
                elif seen_header:
                    output['Total Insert Reads'] += int(data[1])

    return output

@dxpy.entry_point('collect_alignment_summary_metrics')
def collect_alignment_summary_metrics(bam_file, genome_fasta_file, output_project, output_folder, sample_name=None, properties=None):
    """Run Picard CollectAlignmentSummaryMetrics"""
    logger = []

    bam_file = dxpy.DXFile(bam_file)
    dxpy.download_dxfile(bam_file.get_id(), "sample.bam")

    genome_fasta_file = dxpy.DXFile(genome_fasta_file)
    dxpy.download_dxfile(genome_fasta_file.get_id(), "genome.fa.gz")

    cmd = ("java -jar /CollectAlignmentSummaryMetrics.jar " +
           "VALIDATION_STRINGENCY=LENIENT " +
           "INPUT=sample.bam " +
           "OUTPUT=sample.alignment_summary_metrics " +
           "REFERENCE_SEQUENCE=genome.fa.gz")
    run_cmd(cmd, logger)

    asm_file = dxpy.upload_local_file("sample.alignment_summary_metrics",
                                      name = sample_name + ".alignment_summary_metrics", 
                                      properties = properties,
                                      project = output_project,
                                      folder = output_folder,
                                      parents = True
                                     )
    json_info = extract_json_from_asm("sample.alignment_summary_metrics")

    return {"alignment_summary_metrics": dxpy.dxlink(asm_file),
            "json_alignment_summary_metrics": json_info,
            "tools_used": logger}

@dxpy.entry_point('collect_insert_size_metrics')
def collect_insert_size_metrics(bam_file, genome_fasta_file, output_project, output_folder, 
    sample_name=None, properties=None):
    """Run Picard CollectInsertSizeMetrics"""
    logger = []

    bam_file = dxpy.DXFile(bam_file)
    dxpy.download_dxfile(bam_file.get_id(), "sample.bam")

    genome_fasta_file = dxpy.DXFile(genome_fasta_file)
    dxpy.download_dxfile(genome_fasta_file.get_id(), "genome.fa.gz")

    cmd = ("java -jar /CollectInsertSizeMetrics.jar " +
           "VALIDATION_STRINGENCY=LENIENT " +
           "INPUT=sample.bam REFERENCE_SEQUENCE=genome.fa.gz " +
           "OUTPUT=sample.insert_size_metrics " +
           "HISTOGRAM_FILE=sample.insert_size_histogram" +
           "MINIMUM_PCT=0.1")
    run_cmd(cmd, logger)

    json_info = extract_json_from_ism("sample.insert_size_metrics")

    cmd = ("tar czvf sample_insert_size_metrics.tar.gz " +
           "sample.insert_size_metrics sample.insert_size_histogram")
    subprocess.check_call(cmd, shell=True)

    ism_file = dxpy.upload_local_file("sample_insert_size_metrics.tar.gz",
                                      name = sample_name + "_insert_size_metrics.tar.gz", 
                                      properties = properties,
                                      project = output_project,
                                      folder = output_folder,
                                      parents = True
                                     )

    print MAKE_DIE_VARIABLE
    return {
            "insert_size_metrics": dxpy.dxlink(ism_file),
            "json_insert_size_metrics": json_info,
            "tools_used": logger
           }

@dxpy.entry_point('collect_uniqueness_metrics')
def collect_uniqueness_metrics(bam_file, aligner):
    """Run the script to collect statistics on uniqueness of mappings
    in the BAM file."""
    logger = []

    bam_file = dxpy.DXFile(bam_file)
    bam_filename = bam_file.describe()['name']
    dxpy.download_dxfile(bam_file.get_id(), bam_filename)

    output_json_file = 'uniqueness.json'
    cmd = 'chmod +x /collect_uniqueness_metrics'
    run_cmd(cmd, logger)
    cmd = '/collect_uniqueness_metrics {0} {1} > {2}'
    cmd = cmd.format(bam_filename, ALIGNERS[aligner], output_json_file)
    run_cmd(cmd, logger)

    uniqueness_json = json.loads(open(output_json_file).read())
    output = {"json_uniqueness_metrics": uniqueness_json,
              "tools_used": logger}

    return output

@dxpy.entry_point('run_fastqc')
def run_fastqc(fastq_files, output_name, output_project, output_folder, properties=None):
    """Run FastQC"""
    logger = []

    fastq_files = [dxpy.DXFile(item) for item in fastq_files]
    fastq_filenames = []

    if len(fastq_files) == 1:
        fastq_filename = "sample.fastq.gz"
        dxpy.download_dxfile(fastq_files[0].get_id(), fastq_filename)
        fastq_filenames.append(fastq_filename)
    else:
        for i, fastq_file in enumerate(fastq_files):
            fastq_filename = "sample_%03d.fastq.gz" % i
            dxpy.download_dxfile(fastq_file.get_id(), fastq_filename)
            fastq_filenames.append(fastq_filename)

    num_cores = str(cpu_count())

    print "num_cores: %s" % num_cores

    cmd = ("java -Xmx6000m -cp /fastqc:/fastqc/fastqc-0.10.1.jar:/fastqc/jbzip2-0.9.jar:/fastqc/sam-1.32.jar" +
           " -Djava.awt.headless=true -Dfastqc.threads=" + num_cores +
           " -Dfastqc.nogroup=true -Dfastqc.unzip=false -Dfastqc.casava=true" +
           " uk.ac.babraham.FastQC.FastQCApplication " + (" ".join(fastq_filenames)))
    run_cmd(cmd, logger)

    fastqc_report = dxpy.upload_local_file("sample_fastqc.zip", 
                                           name = output_name, 
                                           properties = properties,
                                           project = output_project,
                                           folder = output_folder,
                                           parents = True
                                          )

    return {"fastqc_report": dxpy.dxlink(fastqc_report),
            "tools_used": logger}

@dxpy.entry_point('produce_qc_report')
def produce_qc_report(individual_json_outputs, sample_name, output_project, output_folder):
    """Combine the various statistics collected into a single dict for
    output."""

    output = {'Sample name': sample_name}

    for j in individual_json_outputs:
        for k in j:
            if k in output:
                output[k].update(j[k])
            else:
                output[k] = j[k]

    ofn = sample_name + '_stats.json'
    with open(ofn, 'w') as output_fh:
        output_fh.write(json.dumps(output))

    output_json_file = dxpy.upload_local_file(ofn,
                                              project = output_project,
                                              folder = output_folder,
                                              parents = True
                                             )

    return {'combined_json_file': dxpy.dxlink(output_json_file)}

@dxpy.entry_point('calc_mismatch_per_cycle_stats')
def calc_mismatch_per_cycle_stats(bam_file, aligner, output_project, output_folder):
    logger = []
    bam_file = dxpy.DXFile(bam_file)
    bam_filename = bam_file.describe()['name']
    dxpy.download_dxfile(bam_file.get_id(), bam_filename)
    ofn = os.path.splitext(bam_filename)[0] + '.mm_stats'

    # Change permissions
    cmd = 'chmod +x /bwa_mismatches'
    run_cmd(cmd, logger)
    cmd = '/bwa_mismatches -o {0} -m {1} {2}'.format(ofn, ALIGNERS[aligner], bam_filename)
    run_cmd(cmd, logger)

    mismatch_per_cycle_stats = dxpy.upload_local_file(ofn,
                                                      project = output_project,
                                                      folder = output_folder,
                                                      parents = True
                                                     )

    return {'mismatch_per_cycle_stats': mismatch_per_cycle_stats,
            "tools_used": logger}

@dxpy.entry_point('create_tools_used_json_file')
def create_tools_used_json_file(tools_used, output_project, output_folder):
    tools_used_dict = {}
    tools_used_dict['name'] = get_app_title()
    tools_used_dict['commands'] = []

    for tools in tools_used:
        tools_used_dict['commands'] += tools

    fn = tools_used_dict['name'] + '_tools_used.json'
    with open(fn, 'w') as fh:
        fh.write(json.dumps(tools_used_dict))

    tools_used_json_file = dxpy.upload_local_file(fn,
                                                  project = output_project,
                                                  folder = output_folder,
                                                  parents = True
                                                 )

    return {'tools_used_json_file': tools_used_json_file}

@dxpy.entry_point('main')
def main(fastq_files, sample_name, output_project, output_folder, properties=None, aligner=None, genome_fasta_file = None,
         fastq_files2=None, bam_file=None):
    """Run the various QC programs and output the report files that they
    produce."""

    output = {}
    json_outputs = []
    tools_used = []

    # Run fastqc
    fastqc_jobs = []
    fastqc_input = {
                    "fastq_files": fastq_files, 
                    "properties": properties,
                    "output_project": output_project,
                    "output_folder": output_folder
                   }
    if fastq_files2 == None:
        fastqc_input["output_name"] = sample_name + "_fastqc.zip"
    else:
        fastqc_input["output_name"] = sample_name + "_fastqc_left.zip"
    fastqc_jobs.append(dxpy.new_dxjob(fastqc_input, "run_fastqc"))

    if fastq_files2 != None:
        fastqc_input2 = {"fastq_files": fastq_files2, 
                         "output_name": sample_name + "_fastqc_right.zip",
                         "properties": properties,
                         "output_project": output_project,
                         "output_folder": output_folder
                        }
        fastqc_jobs.append(dxpy.new_dxjob(fastqc_input2, "run_fastqc"))

    output["fastqc_reports"] = [job.get_output_ref("fastqc_report") for job in fastqc_jobs]
    tools_used += [job.get_output_ref("tools_used") for job in fastqc_jobs]

    # These tools require a bam file.
    if (bam_file is not None) and (genome_fasta_file is not None):
        # Run CollectAlignmentSummaryMetrics
        casm_input = {
                      "bam_file": bam_file, 
                      "genome_fasta_file": genome_fasta_file,
                      "sample_name": sample_name, 
                      "properties": properties,
                      "output_project": output_project,
                      "output_folder": output_folder
                     }
        casm_job = dxpy.new_dxjob(casm_input, "collect_alignment_summary_metrics")
        output["alignment_summary_metrics"] = casm_job.get_output_ref("alignment_summary_metrics")
        json_outputs += [casm_job.get_output_ref("json_alignment_summary_metrics")]
        tools_used += [casm_job.get_output_ref("tools_used")]

    if (bam_file is not None) and (aligner is not None):
        # Run Collect Uniqueness Metrics
        uniqueness_input = {
                            "bam_file": bam_file, 
                            "aligner": aligner
                            #"output_project": output_project,
                            #"output_folder": output_folder
                           }
        uniqueness_job = dxpy.new_dxjob(uniqueness_input, "collect_uniqueness_metrics")
        json_outputs += [uniqueness_job.get_output_ref("json_uniqueness_metrics")]
        tools_used += [uniqueness_job.get_output_ref("tools_used")]

        # Run Calc Mismatch Per Cycle Stats
        mismatch_per_cycle_input = {
                                    "bam_file": bam_file, 
                                    "aligner": aligner,
                                    "output_project": output_project,
                                    "output_folder": output_folder
                                   }
        mismatch_per_cycle_job = dxpy.new_dxjob(mismatch_per_cycle_input, 'calc_mismatch_per_cycle_stats')
        output['mismatch_metrics'] = mismatch_per_cycle_job.get_output_ref('mismatch_per_cycle_stats')
        tools_used += [mismatch_per_cycle_job.get_output_ref('tools_used')]

    # If paired-end reads, run CollectInsertSizeMetrics
    if (bam_file is not None) and (fastq_files2 is not None) and (genome_fasta_file is not None):
        cism_input = {
                      "bam_file": bam_file, 
                      "genome_fasta_file": genome_fasta_file,
                      "sample_name": sample_name, 
                      "properties": properties,
                      "output_project": output_project,
                      "output_folder": output_folder
                     }
        cism_job = dxpy.new_dxjob(cism_input, "collect_insert_size_metrics")
        output["insert_size_metrics"] = cism_job.get_output_ref("insert_size_metrics")
        json_outputs += [cism_job.get_output_ref("json_insert_size_metrics")]
        tools_used += [cism_job.get_output_ref("tools_used")]

    produce_qc_report_input = {
                               "individual_json_outputs": json_outputs,
                               "sample_name": sample_name,
                               "output_project": output_project,
                               "output_folder": output_folder
                              }
    produce_qc_report_job = dxpy.new_dxjob(produce_qc_report_input, "produce_qc_report")
    output['json_output_file'] = produce_qc_report_job.get_output_ref("combined_json_file")

    tools_used_input = {
                        "tools_used": tools_used,
                        "output_project": output_project,
                        "output_folder": output_folder
                       }
    tools_used_job = dxpy.new_dxjob(tools_used_input, "create_tools_used_json_file")
    output['tools_used'] = tools_used_job.get_output_ref('tools_used_json_file')

    return output

dxpy.run()
