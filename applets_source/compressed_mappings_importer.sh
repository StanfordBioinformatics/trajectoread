#!/bin/bash
# compressed_mappings_importer 0.0.1
# Generated by dx-app-wizard.
#
# Basic execution pattern: Your app will run on a single machine from
# beginning to end.
#
# Your job's input variables (if any) will be loaded as environment
# variables before this script runs.  Any array inputs will be loaded
# as bash arrays.
#
# Any code outside of main() (or any entry point you may add) is
# ALWAYS executed, followed by running the entry point itself.
#
# See https://wiki.dnanexus.com/Developer-Portal for tutorials on how
# to modify this file.

main() {

   set -x

    echo "Value of bam: '$bam'"
    echo "Value of reference_file: '$reference_file'"
    echo "Value of reference_index: '$reference_index'"
    echo "Value of genome: '$genome'"
    echo "Value of restrict_file: '$restrict_file'"

    # The following line(s) use the dx command-line tool to download your file
    # inputs to the local file system using variable names for the filenames. To
    # recover the original filenames, you can use the output of "dx describe
    # "$variable" --name".

    dx download "$bam" -o input.bam
    samtools index input.bam &
    dx download "$reference_file" -o - | zcat > ref.fa

    if [ -n "$reference_index" ]
    then
	dx download "$reference_index" -o ref.fa.fai
    else
	samtools faidx ref.fa &
    fi

    if [ -n "$restrict_file" ]
    then
	name=`dx describe "$restrict_file" --name`
	extension="${name##*.}"
	base_extension="${name##*.}"
	if [ "$extension" == "gz" ]
	    then
	    name=${name%.gz}
	    base_extension="${name##*.}"
	    dx download "$restrict_file" -o - | zcat > locations."$base_extension"
	elif [ "$extension" == "bz2" ]
	then
	    name=${name%.bz2}
            base_extension="${name##*.}"
	    dx download "$restrict_file" -o - | bzcat > locations."$base_extension"
	else
	    dx download "$restrict_file" -o locations."$base_extension"
	fi
    fi

    samtools view -H input.bam > header.txt

    bam_name=`dx describe "$bam" --name`
    bam_name=${bam_name%.bam}
    bam_name=${bam_name%.BAM}

    genome=`dx-jobutil-parse-link "$genome"`
    table=`python /makeTable.py "$bam_name" "$genome"`

    wait

    bam_name="input.bam"

    for line in `python /extractChromosomes.py header.txt`
    do
	idle=`top -bn2 | grep "Cpu(s)" | pypy /checkProcessor.py`
	while [ $idle -lt 10 ]
	do
	    echo "Processor idleness is $idle. Sleeping for 30seconds"
	    sleep 30
	    idle=`top -bn2 | grep "Cpu(s)" | pypy /checkProcessor.py`
	done
	
	if [ -n "$restrict_file" ]
	then
	    samtools view -b input.bam $line > $line.bam
	    intersectBed -sorted -abam $line.bam -b locations."$base_extension" > $line.subset.bam
	    samtools fillmd -e $line.subset.bam ref.fa | pypy /parseSam $table &
	else  
	    samtools view -b -F 1 input.bam $line | samtools fillmd -e - ref.fa | pypy /parseSam $table &
	    samtools view -b -f 80 input.bam $line | samtools fillmd -e - ref.fa | pypy /parseSam $table &
	    samtools view -b -f 64 -F 16 input.bam $line | samtools fillmd -e - ref.fa | pypy /parseSam $table &
	    samtools view -b -f 144 input.bam $line | samtools fillmd -e - ref.fa | pypy /parseSam $table &
	    samtools view -b -f 128 -F 16 input.bam $line | samtools fillmd -e - ref.fa | pypy /parseSam $table &
	fi

	top -bn2 | grep "Cpu(s)"
    done

    wait
    dx close $table
    dx_table=`dx-jobutil-dxlink $table`

    dx-jobutil-add-output reference_compressed_mappings "$dx_table" --class gtable

}
