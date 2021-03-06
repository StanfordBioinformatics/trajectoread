# trajectoread

Trajectoread is a software suite designed to manage development and operations of the Stanford Sequencing Center pipeline on DNAnexus. There are 5 modules in the suite.

- [trajectoread_source](https://github.com/StanfordBioinformatics/trajectoread_source): Source code for DNAnexus applets used in sequencing pipeline workflows
- [trajectoread_builder](https://github.com/StanfordBioinformatics/trajectoread_builder): Utility to automatically configure and deploy workflows to DNAnexus
- trajectoread_upload: Code to upload sequencing runs to DNAnexus
- trajectoread_launcher: Code to start pipeline analyses
- [trajectoread_monitor](https://github.com/StanfordBioinformatics/trajectoread_monitor): Python and R scripts to get sequencing statistics and generate plots

This software is currently under development and will continue to be made available when ready.

## Module structure

- trajectoread_monitor
- trajectoread_builder
  - trajectoread_source

The trajectoread_source directory is located within, or downstream, of the builder directory, to match their functional orientation. This makes it easy to track source files when building applets or workflows using builder.

        $ python builder.py -e production -a trajectoread_source/bcl2fastq2_by_lane

