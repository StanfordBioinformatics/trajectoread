@dxpy.entry_point('main')
def main(bam_file, aligner):
    logger = []
    bam_file = dxpy.DXFile(bam_file)
    bam_filename = bam_file.describe()['name']
    dxpy.download_dxfile(bam_file.get_id(), bam_filename)
    ofn = os.path.splitext(bam_filename)[0] + '.mm_stats'

    # Change permissions
    cmd = 'chmod +x /home/dnanexus/bwa_mismatches'
    run_cmd(cmd, logger)
    cmd = '/home/dnanexus/bwa_mismatches -o {0} -m {1} {2}'.format(ofn, ALIGNERS[aligner], bam_filename)
    run_cmd(cmd, logger)

    mismatch_per_cycle_stats = dxpy.upload_local_file(ofn)

    return {'mismatch_per_cycle_stats': mismatch_per_cycle_stats,
            "tools_used": logger}

    output = {}
    output['mismatch_per_cycle_stats'] = dxpy.dxlink(mismatch_per_cycle_stats)

dxpy.run()