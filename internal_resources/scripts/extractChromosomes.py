#!/usr/bin/env python

import re
import sys
from operator import itemgetter


fileName = sys.argv[1]

inputFile=open(fileName, 'r')
for line in inputFile:
    if line[:3] == "@SQ":
        parse = re.findall("SN:([^\t\n]*).*LN:([^\t\n]*)", line.strip())
        print parse[0][0]
        