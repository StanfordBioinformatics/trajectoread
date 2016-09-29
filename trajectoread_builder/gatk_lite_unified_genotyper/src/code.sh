#!/bin/bash

# The following line causes bash to exit at any point if there is any error
# and to output each line as it is executed -- useful for debugging
set -e -x -o pipefail

# Calculate 90% of memory size, for java
mem_in_mb=`head -n1 /proc/meminfo | awk '{print int($2*0.9/1024)}'`
java="java -Xmx${mem_in_mb}m"

#
# Fetch genome
#
dx download "$genome_fastagz" -o genome.fa.gz
gunzip genome.fa.gz

#
# Fetch and index mappings
#
input=""
for i in "${!sorted_bams[@]}"; do
  dx download "${sorted_bams[$i]}" -o "input-$i.bam"
  samtools index "input-$i.bam"
  input="$input -I input-$i.bam"
done

#
# Fetch dbsnp
#
dbsnp=""
if [ "$dbsnp_vcfgz" != "" ]; then
  dx download "$dbsnp_vcfgz" -o dbsnp.vcf.gz
  gunzip dbsnp.vcf.gz
  dbsnp="-D dbsnp.vcf"
fi

#
# Set up options
#
opts="-glm $glm"
if [ "$advanced_options" != "" ]; then
  opts="$advanced_options"
fi

#
# Run GATK
#
$java -jar /GenomeAnalysisTKLite.jar -nt `nproc` -T UnifiedGenotyper $input $dbsnp -R genome.fa -o output.vcf $opts
gzip output.vcf

#
# Upload results
#
name=`dx describe "${sorted_bams[0]}" --name`
name="${name%.bam}"

file_id=`dx upload output.vcf.gz -o "$name.vcf.gz" --brief`
dx-jobutil-add-output "variants_vcfgz" "$file_id"
