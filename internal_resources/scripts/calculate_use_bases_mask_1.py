#!/usr/bin/env python

"""
Calculates an appropriate value for the --use-bases-mask argument to
CASAVA's BCL-to-FASTQ conversion. The value is determined by the
contents of a sample sheet file and the RunInfo.xml file from a run.

RunInfo.xml describes the actual read lengths being produced by the run
(e.g., a 2x101bp paired run with an 8bp index read). CASAVA normally
guesses the length of the barcode based on the length of the index read
specified in RunInfo.xml. Specifically, an N bp index read implies an N-1
bp barcode (i.e., CASAVA ignores the last base of the index read).

In some cases, the length of the actual barcodes may not match that guessed
by CASAVA. For example, a user might specify 6bp barcodes to be used with
an 8bp index read. The purpose of this script is to reconcile those two
sources of information.
"""

import argparse
import sys
import xml.etree.ElementTree as ET

# Usage:
#
#  calculate_use_bases_mask <RunInfo.xml> <SampleSheet.csv> <lane_index>
#
# Arguments:
#
# <RunInfo.xml>
#
#  Path to the RunInfo.xml file specifying the run configuration.
#
# <SampleSheet.csv>
#
#  Path to the sample sheet CSV file specifying the barcodes.
#
# <lane_index>
#
#  Index of the lane (a number between 1 and 8, inclusive) identifying the
#  lane for which the --use-bases-mask option is being generated (since
#  barcodes may differ in length between lanes).
#
# Operation:
#
# Parses the RunInfo.xml file, yielding a list of read descriptions. Parses
# the sample sheet CSV file to obtain the barcode length(s) for the
# specified lane. Iterates through the read descriptions, constructing the
# --use-bases-mask, which is printed to standard output.

def parse_args():
    """Parses the command-line arguments."""

    parser = argparse.ArgumentParser()

    parser.add_argument('run_info_file', metavar='<RunInfo.xml>', help='Path to RunInfo.xml file.')
    parser.add_argument('sample_sheet_file', metavar='<SampleSheet.csv>', help='Path to sample sheet CSV file.')
    parser.add_argument('lane_index', metavar='<lane_index>', help='Index of lane for which to look up barcodes.')

    return parser.parse_args()

def parse_run_info(run_info_file):
    """Parses the RunInfo.xml file. Returns a list of dicts, each
    describing the configuration of one read. """

    reads = []

    tree = ET.parse(run_info_file)

    for read_elt in tree.findall(".//Read"):
        read_desc = {}

        read_desc['Number'] = int(read_elt.get('Number'))
        read_desc['NumCycles'] = int(read_elt.get('NumCycles'))

        is_indexed = read_elt.get('IsIndexedRead')
        if is_indexed == 'Y':
            read_desc['IsIndexedRead'] = True
        elif is_indexed == 'N':
            read_desc['IsIndexedRead'] = False
        else:
            raise RuntimeError('Invalid value for IsIndexedRead: %s' % is_indexed)

        reads.append(read_desc)

    return sorted(reads, key=lambda read_desc: read_desc['Number'])

def parse_sample_sheet(sample_sheet_file, lane_index):
    """Parses the sample sheet CSV file and returns a list of the
    barcode(s) for the given lane."""

    barcodes = []

    with open(sample_sheet_file, 'r') as sfile:
        for line in sfile:
            line = line.strip()
            fields = line.split(',')

            assert len(fields) == 10, "Expected 10 fields but found %s; line '%s'" % (len(fields), line)

            if fields[0] == 'FCID':
                # header line
                continue
            elif fields[1] != lane_index:
                # wrong lane
                continue
            else:
                barcodes.append(fields[4])

    return barcodes

def get_barcode_length(barcode):
    """Returns the length of the given barcode. If the barcode is empty or
    'Undetermined', returns None. If the barcode is a dual-indexed barcode,
    returns a tuple containing the lengths of the two parts. Otherwise,
    returns the length of the barcode."""

    if barcode == '' or barcode == 'Undetermined':
        return None
    elif '-' in barcode:
        barcodes = barcode.split('-')
        assert len(barcodes) == 2
        return (len(barcodes[0]), len(barcodes[1]))
    else:
        return len(barcode)

def get_distinct_lengths(barcode_lengths):
    """Given a list of barcode lengths, possibly including None, returns a
    list of the distinct lengths, excluding None."""

    len_set = {bc_len for bc_len in barcode_lengths if bc_len != None}
    return list(len_set)

def get_actual_barcode_length(barcode_lengths):
    """Verify that the given list of barcode lengths contains a single
    element, and return it."""

    if len(barcode_lengths) == 0:
        return None
    elif len(barcode_lengths) == 1:
        return barcode_lengths[0]
    else:
        sys.exit('Found multiple barcode lengths (%s) in sample sheet; exiting...')

def get_use_bases_mask(read_config, barcode_length):
    """Calculates the correct value of --use-bases-mask, given the read
    configuration and barcode length(s) determined from the sample
    sheet."""

    components = []

    for read_desc in read_config:
        if read_desc['IsIndexedRead'] == False:
            # non-index read: keep all bases
            components.append('y%d' % read_desc['NumCycles'])
        else:
            # index read: construct based on barcode length
            read_len = read_desc['NumCycles']
            barcode_len = None
            if read_desc['Number'] == 2:
                # first index
                barcode_len = barcode_length[0]
            elif read_desc['Number'] == 3:
                # second index
                barcode_len = barcode_length[1]
            else:
                sys.exit('Invalid read number for index read: %d' % read_desc['Number'])

            icomp = ''
            if barcode_len != 0:
                icomp = 'I%d' % barcode_len

            ncomp = ''
            if read_len - barcode_len != 0:
                ncomp = 'n%d' % (read_len - barcode_len)

            components.append(icomp + ncomp)

    return ','.join(components)

def main():
    """Main function."""

    args = parse_args()

    read_config = parse_run_info(args.run_info_file)
    print >> sys.stderr, 'read_config: %s' % read_config

    barcodes = parse_sample_sheet(args.sample_sheet_file, args.lane_index)
    print >> sys.stderr, 'barcodes: %s' % barcodes

    barcode_lengths = [get_barcode_length(bc) for bc in barcodes]
    print >> sys.stderr, 'barcode_lengths: %s' % barcode_lengths

    distinct_lengths = get_distinct_lengths(barcode_lengths)
    print >> sys.stderr, 'distinct_lengths: %s' % distinct_lengths

    barcode_length = get_actual_barcode_length(distinct_lengths)
    print >> sys.stderr, 'barcode_length: %s' % (barcode_length,)

    if barcode_length == None:
        print >> sys.stderr, 'Found no barcodes'
        barcode_length = (0, 0)
    elif isinstance(barcode_length, int):
        print >> sys.stderr, 'Found single-index barcodes of length %s' % barcode_length
        barcode_length = (barcode_length, 0)
    elif isinstance(barcode_length, tuple):
        print >> sys.stderr, 'Found dual-index barcodes of lengths %s and %s' % barcode_length

    print >> sys.stderr, 'barcode_length: %s' % (barcode_length,)

    use_bases_mask = get_use_bases_mask(read_config, barcode_length)
    print >> sys.stderr, 'use_bases_mask: %s' % use_bases_mask

    print use_bases_mask

if __name__ == "__main__":
    main()
