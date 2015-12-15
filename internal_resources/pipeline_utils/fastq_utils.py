import app_utils

def read_count(fastq_file):
	"""
	Args : fastq_file - str. FASTQ file name. 
	"""
	compressor_type = app_utils.get_compressor_type(fastq_file)	
	if compressor_type == conf.compressors["GZIP"]:
		tmpFileNameSuffix = "gz"
		cmd = "gunzip "
	elif compressor_type == conf.compressors["BZIP"]:
		cmd = "bunzip2"
		tmpFileNameSuffix = "bz2"
	else:
		cmd = "cat "
		tmpFileNameSuffix = "fastq"
	tmpFileName = "fqfile." + tmpFileNameSuffix
	cmd += tmpFileName + " | wc -l"
	#wc -l returns the line count followed by a space and the file name.
	stdout,stderr = gbsc_utils.createSubprocess(cmd=cmd,checkRetcode=True)
	return stdout.split()[0]


if __name__ == "__main__":
	import sys
	read_count(sys.argv[1])
