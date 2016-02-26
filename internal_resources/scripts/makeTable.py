#!/usr/bin/env python

import sys
import dxpy

def main(name, reference):

    ref = dxpy.DXRecord(reference)
    ref_id = ref.get_id()

    schema = [
        {"name": "sequence", "type":"string"},
        {"name": "insert", "type":"string"},
        {"name": "chr", "type": "string"},
        {"name": "lo", "type": "int32"},
        {"name": "hi", "type": "int32"},
        {"name": "negative_strand", "type": "boolean"},
        {"name": "cigar", "type": "string"}
         ]

    mappingsTable = dxpy.new_dxgtable(schema, indices=[dxpy.DXGTable.genomic_range_index("chr", "lo", "hi", "gri")])
    mappingsTable.add_types(["ReferenceCompressedBAM"])

    mappingsTable.set_details({"original_contigset":{"$dnanexus_link":reference}})
    mappingsTable.rename(name)

    print mappingsTable.get_id()

main(sys.argv[1], sys.argv[2])
