# GATK-Lite Variant Caller (Unified Genotyper)

## What does this app do?

This app calls SNPs and/or indels (using GATK-lite Unified Genotyper).

## What are typical use cases for this app?

This app is used when you have mappings and need to identify variants (SNPs and/or indels). It is
usually the variation calling step in typical processing pipelines (which first map reads to a reference
genome, then perform some mappings improvements such as deduplication, realignment, and recalibration, and
finally call variants). Variation calling is performed with the GATK-lite Unified Genotyper software.

## What data are required for this app to run?

This app requires coordinate-sorted mappings in BAM format (`*.bam`), and the associated reference
genome sequence in gzipped fasta format (`*.fasta.gz`, `*.fa.gz`). The input mappings are typically
expected to have been previously refined through a series of common steps, mainly deduplication,
indel realignment, and base quality score recalibration.

IMPORTANT: The mappings must contain associated read sample information (in technical language, each
read must have an RG tag pointing to a read group with an SM tag). The app accepts any combination of
inputs, such as a single BAM file with one sample, or multiple BAM files representing multiple lanes
of the same sample, or multiple BAM files representing multiple samples, etc.; GATK will distinguish
among samples in the input file(s) based on the SM tag.

Optionally, the app can receive a gzipped VCF file (`*dbsnp*.vcf.gz`) containing dbSNP. If this input
is provided, the resulting variants will be annotated for their presence in dbSNP. For the human
genome (hg19 and b37 assemblies), the best option is to use the dbSNP files that have been
compiled by the GATK team. For your convenience, these GATK-compiled dbSNP files files are provided
as suggestions.

## What does this app output?

This app outputs a gzipped VCF file (`*.vcf.gz`) with the called variants. If multiple samples are
present in the input file(s), the output will be a "multi-sample" VCF file.

## How does this app work?

This app runs the UnifiedGenotyper module from the GATK-lite toolkit (v2.3).
GATK-lite is free for anybody to use (but there will be no more new GATK-lite releases).

For more information, consult the manual at:

http://www.broadinstitute.org/gatk/gatkdocs/org_broadinstitute_sting_gatk_walkers_genotyper_UnifiedGenotyper.html
