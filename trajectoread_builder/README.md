# Setting up trajectoread builder

This will guide you through the steps of installing trajectoread builder, configuring the environment files, and building workflows on DNAnexus.

## 1. Install the DNAnexus SDK
Go to https://wiki.dnanexus.com/Downloads and follow the instruction, there, to install the latest version of the DNAnexus SDK.

## 2. Clone the trajectoread builder repository

```r
git clone git@github.com:StanfordBioinformatics/trajectoread_builder.git
```

## 3. Configure builder.json
Add your own project dxids to the production and/or development workflow and applet JSON entried.

```r
cd trajectoread_builder
vi builder.json
```

Now open the dnanexus_environment.json file in a text editor and fill in the missing fields

## 4. Build a sample workflow, in your development project, on DNAnexus

```r
python builder.py -w workflows/fastq_bwa-mem_gatk-genotyper.json -e develop
```

