#!/usr/bin/env python

import pyfits
from numpy import percentile
from sys import argv

# percentile to give a decent image
lpct=0.2
# No, it's not just 100 - lpct
hpct=99.98

#print "pct.py Processing %s" % argv[1]
if argv[1]:
    hdulist = pyfits.open(argv[1])
    if hdulist[0].data.shape[0]>1:
    #format is e.g. RA, Dec, stokes, spectral -- unusual for MWA images
        scidata=hdulist[0].data[0:side,0:side,0,0][:][:][0][0]
    else:
    # format is e.g. stokes, spectral, RA, Dec -- usual for MWA images
        print percentile(hdulist[0].data[0][0],lpct),percentile(hdulist[0].data[0][0],hpct)

else:
    print "Give me a filename as an argument!"

