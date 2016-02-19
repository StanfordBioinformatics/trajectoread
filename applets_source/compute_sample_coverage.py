#!/usr/bin/env python

"""
Compute a coverage track for a sample using bedtools.
"""

import subprocess
import dxpy
#from pipeline_utils import app_utils

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

@dxpy.entry_point('main')
def main(bam_file, genome_file, sample_name=None, properties=None):
    """Download the BAM file, run bedtools, upload the coverage file."""
    logger = []

    bam_file = dxpy.DXFile(bam_file)
    genome_file = dxpy.DXFile(genome_file)

    dxpy.download_dxfile(bam_file.get_id(), "sample.bam")
    dxpy.download_dxfile(genome_file.get_id(), "genome.fai")

    cmd = "./bedtools genomecov -ibam sample.bam -g genome.fai -bg -trackline > sample.bedGraph"
    run_cmd(cmd, logger)

    coverage_file = dxpy.upload_local_file("sample.bedGraph", name=sample_name+".bedGraph", properties=properties)

    tools_used = {'name': get_app_title(),
                  'commands': logger}
    fn = tools_used['name'] + '_tools_used.json'
    with open(fn, 'w') as fh:
        fh.write(json.dumps(tools_used))
    tools_used_json_file = dxpy.upload_local_file(fn)

    return { "coverage_file": dxpy.dxlink(coverage_file),
             "tools_used": tools_used_json_file}

dxpy.run()
