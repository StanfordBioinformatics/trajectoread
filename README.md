# Setting up trajectoread

This will guide you through the steps of installing trajectoread, configuring the environment files, and building workflows on DNAnexus.

## 1. Install the DNAnexus SDK
Go to https://wiki.dnanexus.com/Downloads and follow the instruction, there, to install the latest version of the DNAnexus SDK.

## 2. Clone the trajectoread repository

```r
git clone git@github.com:StanfordBioinformatics/trajectoread.git
```

## 3. Configure dnanexus_environment.json

```r
cd trajectoread/environment
cp dnanexus_environment_template.json dnanexus_environment.json
```

Now open the dnanexus_environment.json file in a text editor and fill in the missing fields

## 4. Build your workflows on DNAnexus

```r
cd ../builder
python builder.py -w <WORKFLOW_NAME>
```

Once that is complete, log onto DNAnexus to confirm that your workflows are there and give them a spin!

If you run into issues, don't @ me.
