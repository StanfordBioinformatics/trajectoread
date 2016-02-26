#!/usr/bin/env python 
import re
import sys

first = True

for line in sys.stdin:
    cpu = line

print re.findall("(\d+).\d%id", cpu)[0]
