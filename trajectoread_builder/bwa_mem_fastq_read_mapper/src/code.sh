#!/bin/bash
#

# The following line causes bash to exit at any point if there is any error
# and to output each line as it is executed -- useful for debugging
set -e -x -o pipefail

#
# Fetch and uncompress genome
#
mkdir genome
dx cat "$genomeindex_targz" | tar zxvf - -C genome  # => genome/<ref>, genome/<ref>.ann, genome/<ref>.bwt, etc.
genome_file=`ls genome/*.bwt`     # Locate a file called <ref>.bwt
genome_file="${genome_file%.bwt}" # Remove the bwt suffix to keep the <ref>

#
# Fetch reads
#
dx-download-all-inputs --except genomeindex_targz --parallel

#
# Set up options
#
opts=""
if [ "$all_alignments" == "true" ]; then
  opts="$opts -a"
fi
if [ "$mark_as_secondary" == "true" ]; then
  opts="$opts -M"
fi
if [ "$add_read_group" == "true" ]; then
  opts="$opts -R @RG\\tID:${read_group_id}\\tPL:${read_group_platform}\\tPU:${read_group_platform_unit}\\tLB:${read_group_library}\\tSM:${read_group_sample}"
fi
if [ "$advanced_options" != "" ]; then
  opts="$advanced_options"
fi

#
# Run bwa mem
#
input="./in/reads_fastqgz/*"
if [ "$reads2_fastqgz" != "" ]; then
  input="$input ./in/reads2_fastqgz/*"
fi
bwa mem -t `nproc` "$genome_file" $input $opts | samtools view -u -S - | samtools sort -m 256M -@ `nproc` - output
samtools index output.bam

#
# Upload result
#
name="$reads_fastqgz_prefix"
# Remove any _R1 / _1 suffixes
name="${name%_1}"
name="${name%_R1}"
mkdir -p ~/out/sorted_bam/ ~/out/sorted_bai/
mv output.bam ~/out/sorted_bam/"$name".bam
mv output.bam.bai ~/out/sorted_bai/"$name".bai
dx-upload-all-outputs --parallel
