import os
import json
import argparse
import datetime
import pytz
import subprocess

scriptDir = os.path.dirname(__file__)
qcToolsDir = os.path.join(scriptDir,"qc_tools")

homeDir = '/home/dnanexus'
interOpDir = os.path.join(homeDir,"InterOp")

COVER_SHEET_FDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Cover_Sheet.fdf.template')
LANE_RESULTS_SE_FDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Lane_Results_SE.fdf.template')
LANE_RESULTS_PE_FDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Lane_Results_PE.fdf.template')
SEQUENCING_RESULTS_BASIC_FDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Run_Info.fdf.template')
SEQUENCING_RESULTS_SE_FDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Sequencing_Results_SE.fdf.template')
SEQUENCING_RESULTS_PE_FDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Sequencing_Results_PE.fdf.template')
LETTERHEAD_FDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Letterhead.fdf.template')
TOOLS_USED_FDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Tools_Used.fdf.template')

COVER_SHEET_PDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Cover_Sheet_Template.pdf')
LANE_RESULTS_SE_PDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Lane_Results_SE_Template.pdf')
LANE_RESULTS_PE_PDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Lane_Results_PE_Template.pdf')
SEQUENCING_RESULTS_BASIC_PDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Run_Info_Template.pdf')
SEQUENCING_RESULTS_SE_PDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Sequencing_Results_SE_Template.pdf')
SEQUENCING_RESULTS_PE_PDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Sequencing_Results_PE_Template.pdf')
BASE_CALL_VS_READ_CYCLE_PDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Base_Call_Composition_vs_Read_Cycle_Template.pdf')
IMAGE_INTENSITY_VS_READ_CYCLE_PDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Image_Intensity_vs_Read_Cycle_Template.pdf')
MISMATCHES_VS_READ_CYCLE_PDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Mismatches_vs_Read_Cycle_Template.pdf')
QUALITY_SCORE_VS_READ_CYCLE_PDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Quality_Score_vs_Read_Cycle_Template.pdf')
TOOL_VERSIONS_PDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Tool_Version_Template.pdf')
TOOLS_USED_PDF_TEMPLATE = os.path.join(scriptDir,'QC_templates/Tools_Used_Template.pdf')

# Output files (?)
DEFAULT_REPORT_FN = os.path.join(homeDir,'combined_report.pdf')
MISMATCH_PER_CYCLE_STATS_FN = os.path.join(homeDir,'combined_mismatch.stats')
MISMATCHES_PER_READ_CYCLE_PLOT_FN = os.path.join(homeDir,'mm_v_rc.pdf')
QUALITY_SCORE_PER_READ_CYCLE_PLOT_FN = os.path.join(homeDir,'qs_v_rc.pdf')
BASE_CALL_PER_READ_CYCLE_PLOT_FN = os.path.join(homeDir,'bc_v_rc.pdf')
IMAGE_INTENSITY_PER_READ_CYCLE_PLOT_FN = os.path.join(homeDir,'ii_v_rc.pdf')

TOOLS_USED_LINES_PER_PAGE = 40

PAGE_NUMBER = 1
def incr_page_number(i):
    def wrap(f):
        def wrapped_f(*args, **kwargs):
            global PAGE_NUMBER
            val = f(page_number=PAGE_NUMBER, *args, **kwargs)
            PAGE_NUMBER += i
            return val
        return wrapped_f
    return wrap

def chunks(l, n):
    '''Yields successive n-sized chunks from l.  Found here:
    http://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks-in-python
    '''
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def get_args():
    parser = argparse.ArgumentParser(description='Gather statistics on unique reads from SAM/BAM files.')
    parser.add_argument('--mismatch_files', required=False, help='Files with mismatch metrics.', nargs='+')
    parser.add_argument('--basic', required=False, action='store_true', help='Create a bare-bones, basic report. (No mapping info etc.)')
    parser.add_argument('--interop', required=True, help='The interop file for the given run.')
    parser.add_argument('--run_details', required=True, help='JSON file with run details information.')
    parser.add_argument('--sample_stats', required=True, help='JSON file with stats on the individual samples.')
    parser.add_argument('--barcodes', required=True, help='JSON file with a list of the barcodes. Order should match order in mismatch_files and sample_stats.')
    parser.add_argument('--paired_end', required=False, action='store_true', help='Reads are paired-end.')
    parser.add_argument('--out_file', required=False, help='The output name of the QC report PDF.')
    parser.add_argument('--tools_used', required=False, help='A list of the tools and command lines used..')
    parser.add_argument('--collect_duplicate_info', required=False, action='store_true',
                        help=('Collect info on duplicates ' +
                              '(note: input bam file must have duplicate information already marked).'))

    return parser.parse_args()

def run_cmd(cmd):
    print cmd
    output = subprocess.check_output(cmd, shell=True)

    return output

def commafy(number):
    txt = '{0:,}'.format(number)

    return txt

def percentage_of(part, total):
    """Returns a float between 0 and 100 describing 'part' as a
    percentage of 'total'."""
    val = 0.0

    if total == 0:
        assert part == 0, "part must be zero when total is zero; part was %s" % part
        val = 0.0
    else:
        val = 100.0 * float(part) / float(total)

    return '%0.1f%%' % val


def create_qc_summary_data(json_data, collect_duplicate_info):
    ret_val = {'Total Reads': 0,
               'Post-Filter Reads': 0,
               'Failed Reads': 0,
               'Post-Filter Reads (Read 1)': 0,
               'Post-Filter Reads (Read 2)': 0,
               'Mapped PF Reads (Read 1)': 0,
               'Mapped PF Reads (Read 2)': 0,
               'Uniquely-Mapped PF Reads (Read 1)': 0,
               'Uniquely-Mapped PF Reads (Read 2)': 0,
               'Consistent Pairs': 0,
               'Mean Insert Size': 0}
    if collect_duplicate_info:
        ret_val['Mapped PF Reads (Read 1) Duplicates'] = 0
        ret_val['Mapped PF Reads (Read 2) Duplicates'] = 0

    total_insert_reads = 0
    for data in json_data:
        total_reads = data['Read 1']['Total Reads'] + data['Read 2']['Total Reads']
        pf_reads = data['Read 1']['Post-Filter Reads'] + data['Read 2']['Post-Filter Reads']
        ret_val['Total Reads'] += total_reads
        ret_val['Post-Filter Reads'] += pf_reads
        ret_val['Failed Reads'] += total_reads - pf_reads
        ret_val['Post-Filter Reads (Read 1)'] += data['Read 1']['Post-Filter Reads']
        ret_val['Post-Filter Reads (Read 2)'] += data['Read 2']['Post-Filter Reads']
        ret_val['Mapped PF Reads (Read 1)'] += data['Read 1']['Mapped PF Reads']
        ret_val['Mapped PF Reads (Read 2)'] += data['Read 2']['Mapped PF Reads']

        if collect_duplicate_info:
            ret_val['Mapped PF Reads (Read 1) Duplicates'] += (sum(data['Read 1 Duplicates']['Unique'].values()) +
                                                               sum(data['Read 1 Duplicates']['Non-Unique'].values()))
            ret_val['Mapped PF Reads (Read 2) Duplicates'] += (sum(data['Read 2 Duplicates']['Unique'].values()) +
                                                               sum(data['Read 2 Duplicates']['Non-Unique'].values()))
        ret_val['Uniquely-Mapped PF Reads (Read 1)'] += sum(data['Read 1']['Unique'].values())
        ret_val['Uniquely-Mapped PF Reads (Read 2)'] += sum(data['Read 2']['Unique'].values())
        total_insert_reads += data.get('Total Insert Reads', 0)
        ret_val['Mean Insert Size'] += data.get('Mean Insert Size', 0) * data.get('Total Insert Reads', 0)

    if total_insert_reads > 0:
        ret_val['Mean Insert Size'] /= total_insert_reads

    return ret_val

def create_plot_pdf(letterhead_template_pdf_fn, plot_pdf, current_date, current_time, page_number):
    # First, create the letterhead with the proper page number.
    template_fn = os.path.splitext(LETTERHEAD_FDF_TEMPLATE)[0]
    with open(template_fn, 'w') as ofh, open(LETTERHEAD_FDF_TEMPLATE) as ifh:
        text = ifh.read()
        text = text.format(page_number=page_number,
                           date=current_date,
                           time=current_time)
        ofh.write(text)

    letterhead_pdf_fn = letterhead_template_pdf_fn.replace('_Template.pdf', '.pdf')
    cmd = 'pdftk {0} fill_form {1} output {2} flatten'.format(letterhead_template_pdf_fn, template_fn, letterhead_pdf_fn)
    run_cmd(cmd)

    # Now place the plot on a letter size canvas
    letter_plot_pdf_fn = os.path.splitext(plot_pdf)[0] + '_letter.pdf'
    cmd = 'convert {0} -page letter {1}'.format(plot_pdf, letter_plot_pdf_fn)
    run_cmd(cmd)

    shifted_letter_plot_pdf_fn = os.path.splitext(letter_plot_pdf_fn)[0] + '_shifted.pdf'
    shiftScript = os.path.join(scriptDir,"shift_pdf.py")
    cmd = "python " + shiftScript + ' {0} {1} u290'.format(letter_plot_pdf_fn, shifted_letter_plot_pdf_fn)
    run_cmd(cmd)

    ofn = os.path.splitext(plot_pdf)[0] + '_combined.pdf'
    cmd = 'pdftk {0} background {1} output {2}'.format(shifted_letter_plot_pdf_fn, letterhead_pdf_fn, ofn)
    run_cmd(cmd)

    return ofn

###############################################################################
# Code for generating mismatch metrics.
def get_mismatch_metrics(fn):
    metrics = []
    total_counts = -1
    for l in open(fn):
        # First line is simply the total number of counts
        # Followed by a line per cycle where the last value is the mismatches
        # for that cycle.
        if total_counts < 0:
            total_counts = int(l.strip())
        else:
            metrics += [int(l.strip().split()[-1])]

    return {'total_counts': total_counts, 'metrics': metrics}

def add_metrics(src, dst):
    if dst['total_counts'] == 0:
        dst['total_counts'] = src['total_counts']
        dst['metrics'].extend(src['metrics'])
    else:
        dst['total_counts'] += src['total_counts']
        for i, v in enumerate(src['metrics']):
            dst['metrics'][i] += v

def merge_mismatch_metrics(mismatch_metrics_files):
    metrics = {'total_counts': 0, 'metrics': []}
    for fn in mismatch_metrics_files:
        curr_metrics = get_mismatch_metrics(fn)
        add_metrics(curr_metrics, metrics)

    with open(MISMATCH_PER_CYCLE_STATS_FN, 'w') as fh:
        fh.write('mmfraction mmcount\n')
        for v in metrics['metrics']:
            fh.write('{0} {1}\n'.format(float(v)/metrics['total_counts'], v))

    return MISMATCH_PER_CYCLE_STATS_FN

###############################################################################
# Code to create individual pdfs which will make up full QC report.
@incr_page_number(1)
def create_cover_sheet(ordering_lab, current_date, current_time, run_name, page_number):
    template_fn = os.path.splitext(COVER_SHEET_FDF_TEMPLATE)[0]
    with open(template_fn, 'w') as ofh, open(COVER_SHEET_FDF_TEMPLATE) as ifh:
        text = ifh.read()
        text = text.format(ordering_lab=ordering_lab, date_generated=current_date + ' ' + current_time, run_name=run_name, page_number=page_number)
        ofh.write(text)

    ofn = COVER_SHEET_PDF_TEMPLATE.replace('_Template.pdf', '.pdf')
    cmd = 'pdftk {0} fill_form {1} output {2} flatten'.format(COVER_SHEET_PDF_TEMPLATE, template_fn, ofn)
    run_cmd(cmd)

    return ofn

@incr_page_number(1)
def create_run_info_pdf(run_details, qc_summary, current_date, current_time, paired_end, page_number):
    if paired_end:
        template_fn = os.path.splitext(SEQUENCING_RESULTS_PE_FDF_TEMPLATE)[0]
        pdf_template_fn = SEQUENCING_RESULTS_PE_PDF_TEMPLATE
        with open(template_fn, 'w') as ofh, open(SEQUENCING_RESULTS_PE_FDF_TEMPLATE) as ifh:
            text = ifh.read()
            text = text.format(date=current_date,
                               time=current_time,
                               mean_insert_size=commafy(qc_summary['Mean Insert Size']),
                               unique_mapped_pf_reads_2=commafy(qc_summary['Uniquely-Mapped PF Reads (Read 2)']),
                               unique_mapped_pf_reads_2_percent=percentage_of(qc_summary['Uniquely-Mapped PF Reads (Read 2)'],
                                                                              qc_summary['Post-Filter Reads (Read 2)']),
                               unique_mapped_pf_reads_1=commafy(qc_summary['Uniquely-Mapped PF Reads (Read 1)']),
                               unique_mapped_pf_reads_1_percent=percentage_of(qc_summary['Uniquely-Mapped PF Reads (Read 1)'],
                                                                              qc_summary['Post-Filter Reads (Read 1)']),
                               mapped_pf_reads_2=commafy(qc_summary['Mapped PF Reads (Read 2)']),
                               mapped_pf_reads_2_percent=percentage_of(qc_summary['Mapped PF Reads (Read 2)'],
                                                                       qc_summary['Post-Filter Reads (Read 2)']),
                               mapped_pf_reads_1=commafy(qc_summary['Mapped PF Reads (Read 1)']),
                               mapped_pf_reads_1_percent=percentage_of(qc_summary['Mapped PF Reads (Read 1)'],
                                                                       qc_summary['Post-Filter Reads (Read 1)']),
                               pf_reads=commafy(qc_summary['Post-Filter Reads']),
                               genome_name=run_details['genome_name'],
                               mapper_name=run_details['mapper'],
                               operator_name=run_details['operator'],
                               sample_name=run_details['library'],
                               lane_number=run_details['lane'],
                               flow_cell=run_details['flow_cell'],
                               run_name=run_details['run_name'],
                               page_number=page_number)
            ofh.write(text)
    else:
        template_fn = os.path.splitext(SEQUENCING_RESULTS_SE_FDF_TEMPLATE)[0]
        pdf_template_fn = SEQUENCING_RESULTS_SE_PDF_TEMPLATE
        with open(template_fn, 'w') as ofh, open(SEQUENCING_RESULTS_SE_FDF_TEMPLATE) as ifh:
            text = ifh.read()
            text = text.format(date=current_date,
                               time=current_time,
                               mean_insert_size=commafy(qc_summary['Mean Insert Size']),
                               unique_mapped_pf_reads=commafy(qc_summary['Uniquely-Mapped PF Reads (Read 1)']),
                               unique_mapped_pf_reads_percent=percentage_of(qc_summary['Uniquely-Mapped PF Reads (Read 1)'],
                                                                            qc_summary['Post-Filter Reads (Read 1)']),
                               mapped_pf_reads=commafy(qc_summary['Mapped PF Reads (Read 1)']),
                               mapped_pf_reads_percent=percentage_of(qc_summary['Mapped PF Reads (Read 1)'],
                                                                     qc_summary['Post-Filter Reads (Read 1)']),
                               pf_reads=commafy(qc_summary['Post-Filter Reads']),
                               genome_name=run_details['genome_name'],
                               mapper_name=run_details['mapper'],
                               operator_name=run_details['operator'],
                               sample_name=run_details['library'],
                               lane_number=run_details['lane'],
                               flow_cell=run_details['flow_cell'],
                               run_name=run_details['run_name'],
                               page_number=page_number)
            ofh.write(text)

    ofn = pdf_template_fn.replace('_Template.pdf', '.pdf')
    cmd = 'pdftk {0} fill_form {1} output {2} flatten'.format(pdf_template_fn, template_fn, ofn)
    run_cmd(cmd)

    return ofn

@incr_page_number(1)
def create_basic_run_info_pdf(run_details, current_date, current_time, page_number):
    template_fn = os.path.splitext(SEQUENCING_RESULTS_BASIC_FDF_TEMPLATE)[0]
    pdf_template_fn = SEQUENCING_RESULTS_BASIC_PDF_TEMPLATE
    with open(template_fn, 'w') as ofh, open(SEQUENCING_RESULTS_BASIC_FDF_TEMPLATE) as ifh:
        text = ifh.read()
        text = text.format(date=current_date,
                           time=current_time,
                           genome_name=run_details['genome_name'],
                           mapper_name=run_details['mapper'],
                           operator_name=run_details['operator'],
                           sample_name=run_details['library'],
                           lane_number=run_details['lane'],
                           flow_cell=run_details['flow_cell'],
                           run_name=run_details['run_name'],
                           page_number=page_number)
        ofh.write(text)

    ofn = pdf_template_fn.replace('_Template.pdf', '.pdf')
    cmd = 'pdftk {0} fill_form {1} output {2} flatten'.format(pdf_template_fn, template_fn, ofn)
    run_cmd(cmd)

    return ofn

@incr_page_number(1)
def create_sample_stats_pdf(stats, collect_duplicate_info, barcode, current_date, current_time, paired_end, page_number):
    title = 'Detailed Alignment Results for Barcode {0}'.format(barcode)
    if paired_end:
        template_fn = os.path.splitext(LANE_RESULTS_PE_FDF_TEMPLATE)[0]
        pdf_template_fn = LANE_RESULTS_PE_PDF_TEMPLATE
        with open(template_fn, 'w') as ofh, open(LANE_RESULTS_PE_FDF_TEMPLATE) as ifh:
            text = ifh.read()
            text = text.format(date=current_date,
                               time=current_time,
                               title=title,
                               no_match_1=commafy(stats['Read 1']['Total Reads'] - stats['Read 1']['Mapped PF Reads']),
                               no_match_1_percent=percentage_of(stats['Read 1']['Total Reads'] - stats['Read 1']['Mapped PF Reads'],
                                                                stats['Read 1']['Total Reads']),
                               no_match_2=stats['Read 2']['Total Reads'] - stats['Read 2']['Mapped PF Reads'],
                               no_match_2_percent=percentage_of(stats['Read 2']['Total Reads'] - stats['Read 2']['Mapped PF Reads'],
                                                                stats['Read 2']['Total Reads']),
                               reads_1=commafy(stats['Read 1']['Total Reads']),
                               reads_1_percent='',
                               reads_2=commafy(stats['Read 2']['Total Reads']),
                               reads_2_percent='',
                               mapped_1=commafy(stats['Read 1']['Mapped PF Reads']),
                               mapped_1_percent=percentage_of(stats['Read 1']['Mapped PF Reads'],
                                                              stats['Read 1']['Total Reads']),
                               mapped_2=commafy(stats['Read 2']['Mapped PF Reads']),
                               mapped_2_percent=percentage_of(stats['Read 2']['Mapped PF Reads'],
                                                              stats['Read 2']['Total Reads']),
                               unique_1=commafy(sum(stats['Read 1']['Unique'].values())),
                               unique_1_percent=percentage_of(sum(stats['Read 1']['Unique'].values()),
                                                              stats['Read 1']['Total Reads']),
                               unique_2=commafy(sum(stats['Read 2']['Unique'].values())),
                               unique_2_percent=percentage_of(sum(stats['Read 2']['Unique'].values()),
                                                              stats['Read 2']['Total Reads']),
                               unique_1_0mm=commafy(stats['Read 1']['Unique']['0mm']),
                               unique_1_0mm_percent=percentage_of(stats['Read 1']['Unique']['0mm'],
                                                                  stats['Read 1']['Total Reads']),
                               unique_2_0mm=commafy(stats['Read 2']['Unique']['0mm']),
                               unique_2_0mm_percent=percentage_of(stats['Read 2']['Unique']['0mm'],
                                                                  stats['Read 2']['Total Reads']),
                               unique_1_1mm=commafy(stats['Read 1']['Unique']['1mm']),
                               unique_1_1mm_percent=percentage_of(stats['Read 1']['Unique']['1mm'],
                                                                  stats['Read 1']['Total Reads']),
                               unique_2_1mm=commafy(stats['Read 2']['Unique']['1mm']),
                               unique_2_1mm_percent=percentage_of(stats['Read 2']['Unique']['1mm'],
                                                                  stats['Read 2']['Total Reads']),
                               unique_1_2mm=commafy(stats['Read 1']['Unique']['2mm']),
                               unique_1_2mm_percent=percentage_of(stats['Read 1']['Unique']['2mm'],
                                                                  stats['Read 1']['Total Reads']),
                               unique_2_2mm=commafy(stats['Read 2']['Unique']['2mm']),
                               unique_2_2mm_percent=percentage_of(stats['Read 2']['Unique']['2mm'],
                                                                  stats['Read 2']['Total Reads']),
                               unique_1_3mm=commafy(stats['Read 1']['Unique']['3+mm']),
                               unique_1_3mm_percent=percentage_of(stats['Read 1']['Unique']['3+mm'],
                                                                  stats['Read 1']['Total Reads']),
                               unique_2_3mm=commafy(stats['Read 2']['Unique']['3+mm']),
                               unique_2_3mm_percent=percentage_of(stats['Read 2']['Unique']['3+mm'],
                                                                  stats['Read 2']['Total Reads']),
                               unique_1_indel=commafy(stats['Read 1']['Unique']['w/Indel']),
                               unique_1_indel_percent=percentage_of(stats['Read 1']['Unique']['w/Indel'],
                                                                    stats['Read 1']['Total Reads']),
                               unique_2_indel=commafy(stats['Read 2']['Unique']['w/Indel']),
                               unique_2_indel_percent=percentage_of(stats['Read 2']['Unique']['w/Indel'],
                                                                    stats['Read 2']['Total Reads']),
                               non_unique_1=commafy(sum(stats['Read 1']['Non-Unique'].values())),
                               non_unique_1_percent=percentage_of(sum(stats['Read 1']['Non-Unique'].values()),
                                                                  stats['Read 1']['Total Reads']),
                               non_unique_2=commafy(sum(stats['Read 2']['Non-Unique'].values())),
                               non_unique_2_percent=percentage_of(sum(stats['Read 2']['Non-Unique'].values()),
                                                                  stats['Read 2']['Total Reads']),
                               non_unique_1_0mm=commafy(stats['Read 1']['Non-Unique']['0mm']),
                               non_unique_1_0mm_percent=percentage_of(stats['Read 1']['Non-Unique']['0mm'],
                                                                      stats['Read 1']['Total Reads']),
                               non_unique_2_0mm=commafy(stats['Read 2']['Non-Unique']['0mm']),
                               non_unique_2_0mm_percent=percentage_of(stats['Read 2']['Non-Unique']['0mm'],
                                                                      stats['Read 2']['Total Reads']),
                               non_unique_1_1mm=commafy(stats['Read 1']['Non-Unique']['1mm']),
                               non_unique_1_1mm_percent=percentage_of(stats['Read 1']['Non-Unique']['1mm'],
                                                                      stats['Read 1']['Total Reads']),
                               non_unique_2_1mm=commafy(stats['Read 2']['Non-Unique']['1mm']),
                               non_unique_2_1mm_percent=percentage_of(stats['Read 2']['Non-Unique']['1mm'],
                                                                      stats['Read 2']['Total Reads']),
                               non_unique_1_2mm=commafy(stats['Read 1']['Non-Unique']['2mm']),
                               non_unique_1_2mm_percent=percentage_of(stats['Read 1']['Non-Unique']['2mm'],
                                                                      stats['Read 1']['Total Reads']),
                               non_unique_2_2mm=commafy(stats['Read 2']['Non-Unique']['2mm']),
                               non_unique_2_2mm_percent=percentage_of(stats['Read 2']['Non-Unique']['2mm'],
                                                                      stats['Read 2']['Total Reads']),
                               non_unique_1_3mm=commafy(stats['Read 1']['Non-Unique']['3+mm']),
                               non_unique_1_3mm_percent=percentage_of(stats['Read 1']['Non-Unique']['3+mm'],
                                                                      stats['Read 1']['Total Reads']),
                               non_unique_2_3mm=commafy(stats['Read 2']['Non-Unique']['3+mm']),
                               non_unique_2_3mm_percent=percentage_of(stats['Read 2']['Non-Unique']['3+mm'],
                                                                      stats['Read 2']['Total Reads']),
                               non_unique_1_indel=commafy(stats['Read 1']['Non-Unique']['w/Indel']),
                               non_unique_1_indel_percent=percentage_of(stats['Read 1']['Non-Unique']['w/Indel'],
                                                                        stats['Read 1']['Total Reads']),
                               non_unique_2_indel=commafy(stats['Read 2']['Non-Unique']['w/Indel']),
                               non_unique_2_indel_percent=percentage_of(stats['Read 2']['Non-Unique']['w/Indel'],
                                                                        stats['Read 2']['Total Reads']),
                               page_number=page_number)
            ofh.write(text)
    else:
        template_fn = os.path.splitext(LANE_RESULTS_SE_FDF_TEMPLATE)[0]
        pdf_template_fn = LANE_RESULTS_SE_PDF_TEMPLATE
        with open(template_fn, 'w') as ofh, open(LANE_RESULTS_SE_FDF_TEMPLATE) as ifh:
            text = ifh.read()
            text = text.format(date=current_date,
                               time=current_time,
                               title=title,
                               no_match=commafy(stats['Read 1']['Total Reads'] - stats['Read 1']['Mapped PF Reads']),
                               no_match_percent=percentage_of(stats['Read 1']['Total Reads'] - stats['Read 1']['Mapped PF Reads'],
                                                              stats['Read 1']['Total Reads']),
                               reads=commafy(stats['Read 1']['Total Reads']),
                               reads_percent='',
                               mapped=commafy(stats['Read 1']['Mapped PF Reads']),
                               mapped_percent=percentage_of(stats['Read 1']['Mapped PF Reads'],
                                                            stats['Read 1']['Total Reads']),
                               unique=commafy(sum(stats['Read 1']['Unique'].values())),
                               unique_percent=percentage_of(sum(stats['Read 1']['Unique'].values()),
                                                            stats['Read 1']['Total Reads']),
                               unique_0mm=commafy(stats['Read 1']['Unique']['0mm']),
                               unique_0mm_percent=percentage_of(stats['Read 1']['Unique']['0mm'],
                                                                stats['Read 1']['Total Reads']),
                               unique_1mm=commafy(stats['Read 1']['Unique']['1mm']),
                               unique_1mm_percent=percentage_of(stats['Read 1']['Unique']['1mm'],
                                                                stats['Read 1']['Total Reads']),
                               unique_2mm=commafy(stats['Read 1']['Unique']['2mm']),
                               unique_2mm_percent=percentage_of(stats['Read 1']['Unique']['2mm'],
                                                                stats['Read 1']['Total Reads']),
                               unique_3mm=commafy(stats['Read 1']['Unique']['3+mm']),
                               unique_3mm_percent=percentage_of(stats['Read 1']['Unique']['3+mm'],
                                                                stats['Read 1']['Total Reads']),
                               unique_indel=commafy(stats['Read 1']['Unique']['w/Indel']),
                               unique_indel_percent=percentage_of(stats['Read 1']['Unique']['w/Indel'],
                                                                  stats['Read 1']['Total Reads']),
                               non_unique=commafy(sum(stats['Read 1']['Non-Unique'].values())),
                               non_unique_percent=percentage_of(sum(stats['Read 1']['Non-Unique'].values()),
                                                                stats['Read 1']['Total Reads']),
                               non_unique_0mm=commafy(stats['Read 1']['Non-Unique']['0mm']),
                               non_unique_0mm_percent=percentage_of(stats['Read 1']['Non-Unique']['0mm'],
                                                                    stats['Read 1']['Total Reads']),
                               non_unique_1mm=commafy(stats['Read 1']['Non-Unique']['1mm']),
                               non_unique_1mm_percent=percentage_of(stats['Read 1']['Non-Unique']['1mm'],
                                                                    stats['Read 1']['Total Reads']),
                               non_unique_2mm=commafy(stats['Read 1']['Non-Unique']['2mm']),
                               non_unique_2mm_percent=percentage_of(stats['Read 1']['Non-Unique']['2mm'],
                                                                    stats['Read 1']['Total Reads']),
                               non_unique_3mm=commafy(stats['Read 1']['Non-Unique']['3+mm']),
                               non_unique_3mm_percent=percentage_of(stats['Read 1']['Non-Unique']['3+mm'],
                                                                    stats['Read 1']['Total Reads']),
                               non_unique_indel=commafy(stats['Read 1']['Non-Unique']['w/Indel']),
                               non_unique_indel_percent=percentage_of(stats['Read 1']['Non-Unique']['w/Indel'],
                                                                      stats['Read 1']['Total Reads']),
                               page_number=page_number)
            ofh.write(text)

    ofn = pdf_template_fn.replace('_Template.pdf', '_Page_' + str(page_number) + '.pdf')
    cmd = 'pdftk {0} fill_form {1} output {2} flatten'.format(pdf_template_fn, template_fn, ofn)
    run_cmd(cmd)

    return ofn

@incr_page_number(1)
def make_mismatches_per_read_cycle_plot(mismatch_stats_file, current_date, current_time, page_number):
    script = os.path.join(qcToolsDir,"plot_mismatch_summary.r")
    cmd = script + ' datafile=\\"{0}\\" plotfile=\\"{1}\\"'
    cmd = cmd.format(mismatch_stats_file, MISMATCHES_PER_READ_CYCLE_PLOT_FN)
    run_cmd(cmd)

    ofn = create_plot_pdf(MISMATCHES_VS_READ_CYCLE_PDF_TEMPLATE, MISMATCHES_PER_READ_CYCLE_PLOT_FN, current_date, current_time, page_number)
    return ofn

@incr_page_number(1)
def make_quality_score_per_read_cycle_plot(lane_number, current_date, current_time, page_number):
    """
    Generates the plot of the quality score distribution per cycle
    from the InterOp files.
    """
    script = os.path.join(qcToolsDir,"illumina_qscores.rb")
    qMetricsOut = os.path.join(interOpDir, 'QMetricsOut.bin')
    #cmd = "ruby " + script + ' --qmetrics /home/dnanexus/InterOp/QMetricsOut.bin ' + '--summary_plot {0} --lane {1} --verbose --force'
    cmd = "ruby " + script + ' --qmetrics ' + qMetricsOut + ' --summary_plot {0} --lane {1} --verbose --force'
    cmd = cmd.format(QUALITY_SCORE_PER_READ_CYCLE_PLOT_FN, lane_number)
    run_cmd(cmd)

    ofn = create_plot_pdf(QUALITY_SCORE_VS_READ_CYCLE_PDF_TEMPLATE, QUALITY_SCORE_PER_READ_CYCLE_PLOT_FN, current_date, current_time, page_number)
    return ofn

@incr_page_number(2)
def make_base_call_and_image_intensity_plots(lane_number, current_date, current_time, page_number):
    """
    Generates plots of the image intensity and base call
    composition per cycle from the InterOp files.
    """
    script = os.path.join(qcToolsDir,"illumina_intensities.rb")
    extractionMetricsOut = os.path.join(interOpDir, 'ExtractionMetricsOut.bin')
    correctedIntMetricsOut = os.path.join(interOpDir, 'CorrectedIntMetricsOut.bin')
    #cmd = "ruby " + script + ' --extraction InterOp/ExtractionMetricsOut.bin '
    cmd = "ruby " + script + ' --extraction ' + extractionMetricsOut
    #cmd += ' --corrected_int InterOp/CorrectedIntMetricsOut.bin '
    cmd += ' --corrected_int ' + correctedIntMetricsOut
    cmd += ' --raw_summary_plot {0} --call_summary_plot {1} --lane {2} --verbose --force'
    cmd = cmd.format(IMAGE_INTENSITY_PER_READ_CYCLE_PLOT_FN, BASE_CALL_PER_READ_CYCLE_PLOT_FN, lane_number)
    run_cmd(cmd)

    output = []
    output += [create_plot_pdf(IMAGE_INTENSITY_VS_READ_CYCLE_PDF_TEMPLATE, IMAGE_INTENSITY_PER_READ_CYCLE_PLOT_FN, current_date, current_time, page_number)]
    page_number += 1
    output += [create_plot_pdf(BASE_CALL_VS_READ_CYCLE_PDF_TEMPLATE, BASE_CALL_PER_READ_CYCLE_PLOT_FN, current_date, current_time, page_number)]

    return output

@incr_page_number(1)
def create_tool_versions_pdf(current_date, current_time, page_number):
    """Generates pdf with tool versions.  Note, the template for tool
    versions pdf should already have the list of tools and versions,
    all this function really does is put the time, date, and appropriate
    page number on the page."""
    template_fn = os.path.splitext(LETTERHEAD_FDF_TEMPLATE)[0]
    with open(template_fn, 'w') as ofh, open(LETTERHEAD_FDF_TEMPLATE) as ifh:
        text = ifh.read()
        text = text.format(page_number=page_number,
                           date=current_date,
                           time=current_time)
        ofh.write(text)

    ofn = TOOL_VERSIONS_PDF_TEMPLATE.replace('_Template.pdf', '.pdf')
    cmd = 'pdftk {0} fill_form {1} output {2} flatten'.format(TOOL_VERSIONS_PDF_TEMPLATE, template_fn, ofn)
    run_cmd(cmd)

    return ofn

# We'll have to increment the page number ourselves for this function as
# we won't know a priori how many pages we'll be producing
def create_tools_used_pdf(tools_used_txt_fn, current_date, current_time):
    global PAGE_NUMBER
    tools_used_pdf_fns = []

    with open(tools_used_txt_fn) as fh:
        tools_used_txt = fh.read().strip()

    # Print
    for tools_used in chunks(tools_used_txt.split('\n'), TOOLS_USED_LINES_PER_PAGE):
        curr_txt = '\n'.join(tools_used) + '\n'
        template_fn = os.path.splitext(TOOLS_USED_FDF_TEMPLATE)[0]
        with open(template_fn, 'w') as ofh, open(TOOLS_USED_FDF_TEMPLATE) as ifh:
            text = ifh.read()
            text = text.format(date=current_date, time=current_time, tools_used=curr_txt, page_number=PAGE_NUMBER)
            ofh.write(text)

        ofn = TOOLS_USED_PDF_TEMPLATE.replace('_Template.pdf', '_{0}.pdf'.format(PAGE_NUMBER))
        cmd = 'pdftk {0} fill_form {1} output {2} flatten'.format(TOOLS_USED_PDF_TEMPLATE, template_fn, ofn)
        run_cmd(cmd)
        tools_used_pdf_fns += [ofn]
        PAGE_NUMBER += 1

    return tools_used_pdf_fns

###############################################################################
def main():
    args = get_args()
    pdfs_to_merge = []

    cmd = 'tar -zxvf ' + args.interop
    run_cmd(cmd)
    if args.mismatch_files is not None:
        combined_mismatch_metrics_fn = merge_mismatch_metrics(args.mismatch_files)

    with open(args.run_details) as fh:
        run_details = json.loads(fh.read())
    with open(args.sample_stats) as fh:
        sample_stats = json.loads(fh.read())
    with open(args.barcodes) as fh:
        barcodes = json.loads(fh.read())

    current_time = datetime.datetime.now(pytz.timezone('US/Pacific'))
    current_date = current_time.strftime('%B %d, %Y')
    current_time = current_time.strftime('%I:%M %p %Z').lstrip('0')
    run_name = run_details['run_name']
    lab_name = run_details['lab_name']
    pdfs_to_merge += [create_cover_sheet(lab_name, current_date, current_time, run_name)]

    if args.basic:
        pdfs_to_merge += [create_basic_run_info_pdf(run_details, current_date, current_time)]
    else:
        qc_summary = create_qc_summary_data(sample_stats, args.collect_duplicate_info)
        pdfs_to_merge += [create_run_info_pdf(run_details, qc_summary, current_date, current_time, args.paired_end)]

        for i, ss in enumerate(sample_stats):
            pdfs_to_merge += [create_sample_stats_pdf(ss, args.collect_duplicate_info, barcodes[i], current_date, current_time, args.paired_end)]

        if args.mismatch_files is not None:
            pdfs_to_merge += [make_mismatches_per_read_cycle_plot(combined_mismatch_metrics_fn, current_date, current_time)]

    pdfs_to_merge += [make_quality_score_per_read_cycle_plot(run_details['lane'], current_date, current_time)]
    pdfs_to_merge += make_base_call_and_image_intensity_plots(run_details['lane'], current_date, current_time)
    pdfs_to_merge += [create_tool_versions_pdf(current_date, current_time)]
    pdfs_to_merge += create_tools_used_pdf(args.tools_used, current_date, current_time)

    if args.out_file is None:
        args.out_file = DEFAULT_REPORT_FN
    cmd = 'pdftk {0} cat output {1}'.format(' '.join(pdfs_to_merge), args.out_file)
    run_cmd(cmd)

if __name__ == '__main__':
    main()
