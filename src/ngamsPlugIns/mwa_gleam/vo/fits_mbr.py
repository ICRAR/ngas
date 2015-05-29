#! /usr/bin/env python

"""
fits_mbr.py

1 - converts pixels that are identically zero into masked pixels
2 - returns the minimum bounding rectangle (MBR) that contains all the data pixels

Ported for pg_sphere indexing from MWA_Tools/script/fits_trim.py (P. Hancock)
chen.wu@icrar.org May-2015
"""


import numpy as np
from astropy.io import fits
import sys
import pywcs, pyfits

def get_fits_mbr(fin):
	"""

	"""
	hdulist = fits.open(fin)
	data = hdulist[0].data
	#turn pixels that are identically zero, into masked pixels
	data[np.where(data==0.)]=np.nan
	hdulist1 = pyfits.open(fin)
	wcs = pywcs.WCS(hdulist1[0].header)
	imin,imax=0,data.shape[1]-1
	jmin,jmax=0,data.shape[0]-1
	#select [ij]min/max to exclude rows/columns that are all zero
	for i in xrange(0,imax):
		if np.all(np.isnan(data[:,i])):
			imin=i
		else:
			break
	#print imin,
	for i in xrange(imax,imin,-1):
		if np.all(np.isnan(data[:,i])):
			imax=i
		else:
			break
	#print imax,
	for j in xrange(0,jmax):
		if np.all(np.isnan(data[j,:])):
			jmin=j
		else:
			break
	#print jmin,
	for j in xrange(jmax,jmin,-1):
		if np.all(np.isnan(data[j,:])):
			jmax=j
		else:
			break
	#print jmax

	pixcrd = np.array([[imin,jmin],[imin,jmax],[imax,jmax],[imax, jmin]], np.float_)
	sky = wcs.wcs_pix2sky(pixcrd, 1)
	#print sky

	#http://pgsphere.projects.pgfoundry.org/types.html
	# produce the polygon
	sqlStr = "SELECT spoly '{ (%10fd, %10fd), (%10fd, %10fd), (%10fd, %10fd), (%10fd, %10fd) } '" % (sky[0][0], sky[0][1], sky[1][0], sky[1][1], sky[2][0], sky[2][1], sky[3][0], sky[3][1])
	return sqlStr


if __name__ == '__main__':
	if len(sys.argv) == 2:
		fin = sys.argv[-1]
		print "Reading {0}".format(fin)
	else:
		print "usage: python {0} infile.fits".format(__file__)
		sys.exit()
	pgSqlStr = get_fits_mbr(fin)
	print pgSqlStr
