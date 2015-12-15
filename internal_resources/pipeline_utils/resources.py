import os
import subprocess
import glob
import time
from pipeline_utils import app_utils,conf

import dxpy

class UnknownToolError(Exception):
	pass

class Tools:
	#define the name of each tool that can be compiled.
	# Each name should have a method by the same name.
	SAMTOOLS = "samtools"
	BAMTOOLS = "bamtools"
	BWA_MISMATCHES = "bwa_mismatches"
	COLLECT_UNIQUENESS_METRICS = "collect_uniqueness_metrics"
	knownTools = [SAMTOOLS,BAMTOOLS,BWA_MISMATCHES,COLLECT_UNIQUENESS_METRICS]

	pwd = os.getcwd()
	bamtoolsDir = os.path.join(pwd,"bamtools")
	bamtoolsLibDir = os.path.join(bamtoolsDir,"lib")
	samtoolsDir = os.path.join(pwd,"samtools")
	bwaMismatchesDir = os.path.join(pwd,"bwaMismatches")
	collectUniqueMetricsDir = os.path.join(pwd,"collect_uniqueness_metrics")

	def __init__(self,tools):
		"""
		Args : tools - list of tool names (should come from self.knownTools).
		"""
		self.toCompile = tools
		for i in self.toCompile:
			if i not in self.knownTools:
				raise UnknownToolError("The tool '{tool}' does is not a known tool.".format(tool=i))
			getattr(self,i)()
				
	def samtools(self):
		"""
		Compiles Samtools.
		"""
		print("Building samtools")
		os.chdir(self.pwd)
		if os.path.exists(self.samtoolsDir):
			os.rename(self.samtoolsDir,self.samtoolsDir + str(time.time()))
		subprocess.check_call("git clone git://github.com/samtools/samtools.git " + self.samtoolsDir,shell=True)
		os.chdir(self.samtoolsDir)
		subprocess.call("git checkout 0.1.19",shell=True)
		subprocess.check_call("make",shell=True)
		#in this version of samtools, the 'samtools' binary is installed in the cloned folder itself.

	
	def bwa_mismatches(self):
		"""
		Compiles bwa_mismatches. Depends on samtools.	
		"""
		print("Building bwa_mismatches")
		if not os.path.exists(self.bwaMismatchesDir):
			os.mkdir(self.bwaMismatchesDir)
		os.chdir(self.bwaMismatchesDir)
		dxFolder = conf.tools["bwa_mismatches"]["dxSrcFolder"]
		projectid = conf.accountProjectID
		app_utils.find_and_download_file(projectid=projectid,folder=dxFolder,name="bwa_mismatches.c")
		outputBinary = os.path.join(self.bwaMismatchesDir,"bwa_mismatches")
		bwaMismatchesCFile = os.path.join(self.bwaMismatchesDir,"bwa_mismatches.c")
		samtoolsLibbamaFile = os.path.join(self.samtoolsDir,"libbam.a")
		if not os.path.exists(samtoolsLibbamaFile):
			#then build samtools
			self.samtools()
		cmd = "gcc -O3 -I {samtoolsDir} {bwaMismatchesCFile} {samtoolsLibbamaFile} -o {outputBinary} -lpthread".format(samtoolsDir=self.samtoolsDir,bwaMismatchesCFile=bwaMismatchesCFile,samtoolsLibbamaFile=samtoolsLibbamaFile,outputBinary=outputBinary)
		subprocess.check_call(cmd,shell=True)
	
	
	def bamtools(self):
		"""
		Compiles bamtools.
		Bamtools has a CMake-based build system. The Ubuntu 12 servers on DNANexus already have cmake 2.8.7 in the path.
		Build instructions below from https://github.com/pezmaster31/bamtools/wiki/Building-and-installing.
		In the top-level directory of BamTools, type the following commands:
	
		$ mkdir build
		$ cd build
		$ cmake ..
	
		After running cmake, just run:
	
		$ make
	
		Then go back up to the BamTools root directory.
	
		$ cd ..
		Assuming the build process finished correctly, you should be able to find the toolkit executable here:
	
		./bin/
		The BamTools API and Utils libraries will be found here:
	
		./lib/
		The BamTools API headers will be found here:
	
		./include/*
		"""
		print("Building Bamtools")
		if os.path.exists(self.bamtoolsDir):
			os.rename(self.bamtoolsDir,self.bamtoolsDir + str(time.time()))
		subprocess.check_call("git clone https://github.com/pezmaster31/bamtools.git " + self.bamtoolsDir,shell=True)
		os.chdir(self.bamtoolsDir)
		subprocess.check_call("git checkout v2.3.0",shell=True)
		buildDir = "build"
		subprocess.check_call("mkdir {}".format(buildDir),shell=True)
		os.chdir(buildDir)
		print(os.listdir("."))
		subprocess.check_call("cmake ..",shell=True)
		subprocess.check_call("make",shell=True)
		
		
	def collect_uniqueness_metrics(self):
		"""
		Compiles collect_uniqueness_metrics. Depends on bamtools.
		If you test this locally on scg, you'll need to have libz installed and in the environment. On the DNAnexus servers, this is alredy done.
		"""
		print("Building collect_uniqueness_metrics")
		dxProjectId = conf.accountProjectID
		dxFolderPath = conf.tools["collect_uniqueness_metrics"]["dxSrcFolder"]
		lastDir = os.path.basename(dxFolderPath.rstrip("/"))
		#I created the lastDir variable above because the "dx download -r" command that I issue below
		# always creates the last folder name in the resource path in your output directory
		dxLocation = dxProjectId + ":" +  dxFolderPath
		if os.path.exists(self.collectUniqueMetricsDir):
			os.rename(self.collectUniqueMetricsDir,self.collectUniqueMetricsDir + str(time.time()))
		os.mkdir(self.collectUniqueMetricsDir)
		cmd = "dx download -r {dxLocation} -o {outdir}".format(dxLocation=dxLocation,outdir=self.collectUniqueMetricsDir)
		print("Running commmand '{cmd}'".format(cmd=cmd))
		subprocess.check_call(cmd,shell=True)
		#the above call download a folder called collect_uniqueness_metrics, which in turn contains and 'include' folder and the file
		# collect_uniqueness_metrics.cpp.
		includeDir = os.path.join(self.collectUniqueMetricsDir,lastDir,"include")
		cFile = os.path.join(self.collectUniqueMetricsDir,lastDir,"collect_uniqueness_metrics.cpp")
		if not os.path.exists(self.bamtoolsLibDir):
			#then make Bamtools
			self.bamtools()
		bamtoolsLibAFiles = " ".join(glob.glob(os.path.join(self.bamtoolsLibDir,"lib*.a")))
		outputBinary = os.path.join(self.collectUniqueMetricsDir,"collect_uniqueness_metrics")
		os.chdir(os.path.join(self.collectUniqueMetricsDir,lastDir))
		cmd = "g++ -O3  -I {includeDir} {cFile} {bamtoolsLibAFiles} -o {outputBinary}".format(includeDir=includeDir,cFile=cFile,bamtoolsLibAFiles=bamtoolsLibAFiles,outputBinary=outputBinary)
		print("Running command '{cmd}'".format(cmd=cmd))
		subprocess.check_call(cmd,shell=True)


if __name__ == "__main__":
	from argparse import ArgumentParser

	description = "Compiles tools to be used in the pipeline."
	parser = ArgumentParser(description=description)
	parser.add_argument('-t','--tools',required=True,choices=Tools.knownTools,nargs="+")

	args = parser.parse_args()
	tools = args.tools
	t = Tools(tools)
