#!/usr/bin/env python

"""
Maps FASTQ files to a genome using BWA.
"""

import os.path
import subprocess
import json
import dxpy
from multiprocessing import cpu_count
from  pipeline_utils import conf

SUPPORTED_MAPPERS = conf.supportedMappers #["bwa", "bwa_aln", "bwa_mem"]

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

@dxpy.entry_point("postprocess")
def postprocess(bam_files, sample_name=None, properties=None):
    """Downloads the BAM files produced by mapping each of the input FASTQ
    files. Merge them into a single BAM file."""

    logger = []

    bam_files = [dxpy.DXFile(f) for f in bam_files]
    for i, bam_file in enumerate(bam_files):
        dxpy.download_dxfile(bam_file.get_id(), "bam_files-" + str(i))

    cmd_array = ["samtools", "merge", "sample.bam"]
    for i in xrange(len(bam_files)):
        cmd_array.append("bam_files-" + str(i))

    run_cmd(cmd_array, logger, shell=False)

    merged_bam_file = dxpy.upload_local_file("sample.bam", name=sample_name+".bam", properties=properties)

    return { "bam_file": dxpy.dxlink(merged_bam_file),
             "tools_used": logger }

def remove_ext(filename):
    """Removes all extensions from a filename. E.g., if filename is
    'foo.fastq.gz', returns 'foo'."""

    split = os.path.splitext(filename)
    if split[1] == '':
        return split[0]
    else:
        return remove_ext(split[0])


def run_bwa_mem_single(fastq_file, genome_fasta_file, genome_index_file, mark_duplicates, logger):
    """Runs BWA-MEM on a single FASTQ file."""

    fastq_file = dxpy.DXFile(fastq_file)
    genome_fasta_file = dxpy.DXFile(genome_fasta_file)
    genome_index_file = dxpy.DXFile(genome_index_file)

    dxpy.download_dxfile(fastq_file.get_id(), "sample.fastq.gz")
    dxpy.download_dxfile(genome_fasta_file.get_id(), "genome.fa.gz")
    dxpy.download_dxfile(genome_index_file.get_id(), "genome.tar.gz")

    subprocess.check_call("tar xzvf genome.tar.gz", shell=True)
    num_cores = str(cpu_count())

    run_cmd("bwa mem -t " + num_cores + " genome.fa.gz sample.fastq.gz > sample0.sam", logger)
    run_cmd("java -jar /CleanSam.jar INPUT=sample0.sam OUTPUT=sample1.bam", logger)
    run_cmd("samtools sort -@ " + num_cores + " sample1.bam sample", logger)

    if mark_duplicates:
        run_cmd("java -jar /MarkDuplicates.jar " +
                "INPUT=sample.bam OUTPUT=sample_deduped.bam METRICS_FILE=/dev/null", logger)
        subprocess.check_call("mv sample_deduped.bam sample.bam", shell=True)

def run_bwa_mem_paired(fastq_file, fastq_file2, genome_fasta_file, genome_index_file, mark_duplicates, logger):
    """Runs BWA-MEM on a pair of FASTQ files."""

    fastq_file = dxpy.DXFile(fastq_file)
    fastq_file2 = dxpy.DXFile(fastq_file2)
    genome_fasta_file = dxpy.DXFile(genome_fasta_file)
    genome_index_file = dxpy.DXFile(genome_index_file)

    dxpy.download_dxfile(fastq_file.get_id(), "sample.fastq.gz")
    dxpy.download_dxfile(fastq_file2.get_id(), "sample_2.fastq.gz")
    dxpy.download_dxfile(genome_fasta_file.get_id(), "genome.fa.gz")
    dxpy.download_dxfile(genome_index_file.get_id(), "genome.tar.gz")

    subprocess.check_call("tar xzvf genome.tar.gz", shell=True)
    num_cores = str(cpu_count())

    run_cmd("bwa mem -t " + num_cores + " genome.fa.gz sample.fastq.gz sample_2.fastq.gz > sample0.sam", logger)
    run_cmd("java -jar /CleanSam.jar INPUT=sample0.sam OUTPUT=sample1.bam", logger)
    run_cmd("samtools sort -@ " + num_cores + " sample1.bam sample", logger)

    if mark_duplicates:
        run_cmd("java -jar /MarkDuplicates.jar " +
                "INPUT=sample.bam OUTPUT=sample_deduped.bam METRICS_FILE=/dev/null", logger)
        subprocess.check_call("mv sample_deduped.bam sample.bam", shell=True)

def run_bwa_backtrack_single(fastq_file, genome_fasta_file, genome_index_file, mark_duplicates, logger):
    """Runs BWA-backtrack on a single FASTQ file."""

    fastq_file = dxpy.DXFile(fastq_file)
    genome_fasta_file = dxpy.DXFile(genome_fasta_file)
    genome_index_file = dxpy.DXFile(genome_index_file)

    dxpy.download_dxfile(fastq_file.get_id(), "sample.fastq.gz")
    dxpy.download_dxfile(genome_fasta_file.get_id(), "genome.fa.gz")
    dxpy.download_dxfile(genome_index_file.get_id(), "genome.tar.gz")

    subprocess.check_call("tar xzvf genome.tar.gz", shell=True)
    num_cores = str(cpu_count())

    run_cmd("bwa-0.6.2 aln -t " + num_cores + " genome.fa.gz sample.fastq.gz > sample.sai", logger)
    run_cmd("bwa-0.6.2 samse genome.fa.gz sample.sai sample.fastq.gz > sample0.sam", logger)
    run_cmd("java -jar /CleanSam.jar INPUT=sample0.sam OUTPUT=sample1.bam", logger)
    run_cmd("samtools sort -@ " + num_cores + " sample1.bam sample", logger)

    if mark_duplicates:
        run_cmd("java -jar /MarkDuplicates.jar " +
                "INPUT=sample.bam OUTPUT=sample_deduped.bam METRICS_FILE=/dev/null", logger)
        subprocess.check_call("mv sample_deduped.bam sample.bam", shell=True)

def run_bwa_backtrack_paired(fastq_file, fastq_file2, genome_fasta_file, genome_index_file, mark_duplicates, logger):
    """Runs BWA-backtrack on a pair of FASTQ files."""

    fastq_file = dxpy.DXFile(fastq_file)
    fastq_file2 = dxpy.DXFile(fastq_file2)
    genome_fasta_file = dxpy.DXFile(genome_fasta_file)
    genome_index_file = dxpy.DXFile(genome_index_file)

    dxpy.download_dxfile(fastq_file.get_id(), "sample.fastq.gz")
    dxpy.download_dxfile(fastq_file2.get_id(), "sample_2.fastq.gz")
    dxpy.download_dxfile(genome_fasta_file.get_id(), "genome.fa.gz")
    dxpy.download_dxfile(genome_index_file.get_id(), "genome.tar.gz")

    subprocess.check_call("tar xzvf genome.tar.gz", shell=True)
    num_cores = str(cpu_count())

    run_cmd("bwa-0.6.2 aln -t " + num_cores + " genome.fa.gz sample.fastq.gz > sample.sai", logger)
    run_cmd("bwa-0.6.2 aln -t " + num_cores + " genome.fa.gz sample_2.fastq.gz > sample_2.sai", logger)
    run_cmd("bwa-0.6.2 sampe -P genome.fa.gz sample.sai sample_2.sai sample.fastq.gz sample_2.fastq.gz" +
            " > sample0.sam", logger)
    run_cmd("java -jar /CleanSam.jar INPUT=sample0.sam OUTPUT=sample1.bam", logger)
    run_cmd("samtools sort -@ " + num_cores + " sample1.bam sample", logger)

    if mark_duplicates:
        run_cmd("java -jar /MarkDuplicates.jar " +
                "INPUT=sample.bam OUTPUT=sample_deduped.bam METRICS_FILE=/dev/null", logger)
        subprocess.check_call("mv sample_deduped.bam sample.bam", shell=True)

def run_samtools_calmd(logger):
    """Runs samtools calmd on the sorted BAM file."""

    subprocess.check_call("gunzip -c genome.fa.gz > genome.fa", shell=True)
    run_cmd("samtools calmd -b sample.bam genome.fa > sample.calmd.bam", logger)
    subprocess.check_call("mv -v sample.calmd.bam sample.bam", shell=True)

@dxpy.entry_point("process")
def process(fastq_file, genome_fasta_file, genome_index_file, mapper, mark_duplicates, fastq_file2=None,
            sample_name=None, properties=None):
    """Download a single FASTQ file, map it, and output a coordinate-sorted
    BAM file."""

    logger = []
    if mapper not in SUPPORTED_MAPPERS:
        raise dxpy.AppError("Unsupported mapper: " + mapper)

    if mapper == "bwa_mem":
        if fastq_file2 == None:
            run_bwa_mem_single(fastq_file, genome_fasta_file, genome_index_file, mark_duplicates, logger)
        else:
            run_bwa_mem_paired(fastq_file, fastq_file2, genome_fasta_file, genome_index_file, mark_duplicates, logger)
    elif mapper == "bwa" or mapper == "bwa_aln":
        if fastq_file2 == None:
            run_bwa_backtrack_single(fastq_file, genome_fasta_file, genome_index_file, mark_duplicates, logger)
        else:
            run_bwa_backtrack_paired(fastq_file, fastq_file2, genome_fasta_file, genome_index_file, mark_duplicates, logger)
    else:
        raise dxpy.AppError("Unsupported mapper: " + mapper)

    run_samtools_calmd(logger)

    bam_file = dxpy.upload_local_file("sample.bam", name=sample_name+".bam", properties=properties)

    return { "bam_file": dxpy.dxlink(bam_file),
             "tools_used": logger }

@dxpy.entry_point('create_tools_used_json_file')
def create_tools_used_json_file(tools_used):
    tools_used_dict = {}
    tools_used_dict['name'] = get_app_title()
    tools_used_dict['commands'] = []

    for tools in tools_used:
        tools_used_dict['commands'] += tools
        
    fn = tools_used_dict['name'] + '_tools_used.json'
    with open(fn, 'w') as fh:
        fh.write(json.dumps(tools_used_dict))
    
    tools_used_json_file = dxpy.upload_local_file(fn)

    return {'tools_used_json_file': tools_used_json_file}

@dxpy.entry_point("main")
def main(fastq_files, genome_fasta_file, genome_index_file, mapper, mark_duplicates=False, fastq_files2=None,
         sample_name=None, properties=None):
    """Spawn subjobs to map each of the FASTQ files (and their pairs,
    if provided) and merge the BAM files into a single BAM file, which
    is output."""

    if fastq_files2 != None:
        assert len(fastq_files2) == len(fastq_files), \
            "fastq_files2 contains %s elements; expected %s" % (len(fastq_files2), len(fastq_files))

    subjobs = []
    for i in xrange(len(fastq_files)):
        subjob_input = { "fastq_file": fastq_files[i],
                         "genome_fasta_file": genome_fasta_file,
                         "genome_index_file": genome_index_file,
                         "mapper": mapper,
                         "sample_name": sample_name,
                         "mark_duplicates": mark_duplicates,
                         "properties": properties }
        if fastq_files2 != None:
            subjob_input["fastq_file2"] = fastq_files2[i]
        subjobs.append(dxpy.new_dxjob(subjob_input, "process"))

    if len(fastq_files) > 1:
        postprocess_input = { "bam_files": [subjob.get_output_ref("bam_file") for subjob in subjobs],
                              "sample_name": sample_name, "properties": properties }
        postprocess_job = dxpy.new_dxjob(fn_input=postprocess_input, fn_name="postprocess", depends_on=subjobs)

        tools_used_input = {'tools_used': [job.get_output_ref('tools_used') for job in (subjobs + [postprocess_job])]}
        tools_used_job = dxpy.new_dxjob(tools_used_input, "create_tools_used_json_file")

        # TODO Fix output to correctly grab logger info.
        return { "bam_file": postprocess_job.get_output_ref("bam_file"),
                 "tools_used": tools_used_job.get_output_ref("tools_used_json_file")}
    else:
        tools_used_input = {'tools_used': [job.get_output_ref('tools_used') for job in subjobs]}
        tools_used_job = dxpy.new_dxjob(tools_used_input, "create_tools_used_json_file")
        return { "bam_file": subjobs[0].get_output_ref("bam_file"),
                 "tools_used": tools_used_job.get_output_ref("tools_used_json_file") }

dxpy.run()
