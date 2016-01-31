#!/usr/bin/python
import sys
import os
from  pyPdf import PdfFileReader, PdfFileWriter

def get_shifts(shifts):
    x_shift = 0
    y_shift = 0
    for shift in shifts:
        if shift[0].lower() == 'd':
            y_shift += int(shift[1:])
        elif shift[0].lower() == 'u':
            y_shift -= int(shift[1:])
        elif shift[0].lower() == 'l':
            x_shift += int(shift[1:])
        elif shift[0].lower() == 'r':
            x_shift += int(shift[1:])
        else:
            print 'Expecting either d/u/l/r directions for shifts.  Saw: ' + shift[0] + '.'
            sys.exit(1)

    return (x_shift, y_shift)

#not sure what default user space units are. 
# just guessed until current document i was looking at worked
uToShift = 50;

if (len(sys.argv) < 4):
  print "Usage shift_pdf [in_file] [out_file] [direction]"
  sys.exit()

if not os.path.exists(sys.argv[1]):
  print "%s does not exist." % sys.argv[1]
  sys.exit()

pdfInput = PdfFileReader(file( sys.argv[1], "rb"))
pdfOutput = PdfFileWriter()

pages=pdfInput.getNumPages()

(x_shift, y_shift) = get_shifts(sys.argv[3:])

for i in range(0,pages):
  p = pdfInput.getPage(i)
  for box in (p.mediaBox, p.cropBox, p.bleedBox, p.trimBox, p.artBox):
    box.lowerLeft = (box.getLowerLeft_x() + x_shift, box.getLowerLeft_y() + y_shift)
    box.upperRight = (box.getUpperRight_x() + x_shift, box.getUpperRight_y() + y_shift)
  pdfOutput.addPage( p )

outputStream = file(sys.argv[2], "wb")
pdfOutput.write(outputStream)
outputStream.close()
