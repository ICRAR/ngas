#! /usr/bin/env python

"""
fits_mbr.py

returns the spherical minimum bounding rectangle (MBR) that contains all the data pixels

Also ingest some test data to the VO database

"""

"""
mosaic_20130822_162-170MHz_YY_r-1.0.fits
mosaic_20130822_162-170MHz_XX_r-1.0.fits
mosaic_20130822_154-162MHz_YY_r-1.0.fits
mosaic_20130822_154-162MHz_XX_r-1.0.fits
mosaic_20130822_147-154MHz_YY_r-1.0.fits
mosaic_20130822_147-154MHz_XX_r-1.0.fits
mosaic_20130822_139-147MHz_YY_r-1.0.fits
mosaic_20130822_139-147MHz_XX_r-1.0.fits
mosaic_20130808_162-170MHz_YY_r-1.0.fits
mosaic_20130808_162-170MHz_XX_r-1.0.fits
"""

import numpy as np
from astropy.io import fits
import sys, os, datetime, math
import pywcs, pyfits

# used to connect to MWA M&C database

from psycopg2.pool import ThreadedConnectionPool

g_db_pool = ThreadedConnectionPool(1, 3, database = 'gavo', user = 'zhl',
                            password = 'emhsZ2x5\n'.decode('base64'),
                            host = 'mwa-web.icrar.org')

mime = "images/fits"
DEBUG = False

def getVODBConn():
    if (g_db_pool):
        return g_db_pool.getconn()
    else:
        raise Exception('VO connection pool is None when get conn')

def putVODBConn(conn):
    if (g_db_pool):
        g_db_pool.putconn(conn)
    else:
        error("Fail to get VO DB connection pool")
        raise Exception('VO connection pool is None when put conn')

def executeQuery(conn, sqlQuery):
    try:
        cur = conn.cursor()
        cur.execute(sqlQuery)
        return cur.fetchall()
    finally:
        if (cur):
            del cur
        putVODBConn(conn)

def get_fits_mbr(fin, row_ignore_factor=10):
	"""
	step 1: get the MBR on the 2D plane
	step 2: get the MBR on the sphere

	"""
	print "Getting the MBR of {0}".format(fin)
	hdulist = fits.open(fin)
	data = hdulist[0].data
	data[np.where(data == 0.)] = np.nan
	hdulist1 = pyfits.open(fin)
	wcs = pywcs.WCS(hdulist1[0].header)

	"""
	MBR on 2D plane is quite different from MBR on the sphere.
	e.g. pixel (imin,jmin) may not be the RA_min or DEC_min
	likewise, pixel (imax,jmax) may not be the RA_max or DEC_max on the sphere
	use the following algorithm to find RA_min/max and DEC_min/max

	1. go thru each "row" (actually col?) of the shrinked image
		1.1 get the RA of the leftmost/ rightmost pixel that is not zero
		1.2 normalise rightmost RA (in case cross the 360/0 degree)
		1.3 update the global min / max RA
	2. go thru each "col" of the shrinked image
		2.1 get the DEC of topmost / bottommost pixel that is not zero
		2.2 update the global min / max DEC
	"""
	RA_min = 1000
	RA_max = -1000
	DEC_min = 1000
	DEC_max = -1000

	width = data.shape[1]
	height = data.shape[0]

	print "Getting RA...",
	for j in xrange(height):
		row = data[j, :]
		indices = np.where(~np.isnan(row))[0]
		if (width / row_ignore_factor >= len(indices)): #ignore "small" rows
			continue
		left = [indices[0], j] # the first index (min)
		right = [indices[-1], j] # the last index (max)
		sky_lr = wcs.wcs_pix2sky(np.array([left, right], np.float_), 1)
		if (sky_lr[1][0] > sky_lr[0][0]):
			sky_lr[1][0] -= 360.0
		if (sky_lr[1][0] < RA_min):
			RA_min = sky_lr[1][0]
		if (sky_lr[0][0] > RA_max):
			RA_max = sky_lr[0][0]
	print "Done"
	print "Getting DEC...",
	for i in xrange(width):
		col = data[:, i]
		indices = np.where(~np.isnan(col))[0]
		if (0 == len(indices)):
			continue
		top = [i, indices[-1]]
		bottom = [i, indices[0]]
		sky_tb = wcs.wcs_pix2sky(np.array([top, bottom], np.float_), 1)
		if (sky_tb[1][1] < DEC_min):
			DEC_min = sky_tb[1][1]
		if (sky_tb[0][1] > DEC_max):
			DEC_max = sky_tb[0][1]
	print "Done"

	center_ra = (RA_min + RA_max) / 2.0
	center_dec = (DEC_min + DEC_max) / 2.0

	if (RA_min < 0):
		RA_min += 360
	if (RA_max < 0):
		RA_max += 360
	#pixcrd = np.array([[imin,jmin],[imin,jmax],[imax,jmax],[imax, jmin]], np.float_)
	#pixcrd = np.array([[imax, jmin], [imax,jmax], [imin,jmax], [imin,jmin]], np.float_)
	#sky = wcs.wcs_pix2sky(pixcrd, 1)
	#print sky

	#http://pgsphere.projects.pgfoundry.org/types.html
	# produce the polygon
	#print "({0}, {1}) and ({2}, {3})".format(imin, jmin, imax, jmax)
	# sqlStr = "SELECT spoly '{ (%10fd, %10fd), (%10fd, %10fd), (%10fd, %10fd), (%10fd, %10fd) } '" % (sky[0][0], sky[0][1], sky[1][0], sky[1][1], sky[2][0], sky[2][1], sky[3][0], sky[3][1])
	#sqlStr = "SELECT sbox '((%10fd, %10fd), (%10fd, %10fd))'" % (sky[0][0], sky[0][1], sky[2][0], sky[2][1])
	sqlStr = "SELECT sbox '((%10fd, %10fd), (%10fd, %10fd))'" % (RA_min, DEC_min, RA_max, DEC_max)
	#print sqlStr
	return sqlStr, center_ra, center_dec

def get_centre_freq(fileId):
	ends = fileId.split("MHz")[0].split("_")[-1].split("-")
	r = int(ends[0])
	l = int(ends[1])
	return (r + l) / 2

def insert_db(conn, filename):
	"""
	e.g.
	mosaic_20130822_162-170MHz_YY_r-1.0.fits
	mosaic_20130822_162-170MHz_XX_r-1.0.fits
	"""
	import pccFits.PccSimpleFitsReader as fitsapi
	hdr = fitsapi.getFitsHdrs(filename)[0]
	required_hdrs = ['CRVAL1', 'CRVAL2']
	for rhdr in required_hdrs:
		if (not hdr.has_key(rhdr)):
			print "Missing header keyword {0}".format(rhdr)
			return

	#ra = float(hdr['CRVAL1'][0][1])
	#dec = float(hdr['CRVAL2'][0][1])
	accsize = os.path.getsize(filename)
	embargo = datetime.date.today() - datetime.timedelta(days = 1)
	owner="ICRAR"
	fileId = os.path.basename(filename)
	accref_file =  "gleam/%s" % fileId
	myhost = "store04.icrar.org"
	file_url =  "http://%s:7777/RETRIEVE?file_id=%s" % (myhost, fileId)
	sqlStr, ra, dec = get_fits_mbr(filename)
	cur = conn.cursor()
	center_freq = get_centre_freq(fileId)
	cur.execute(sqlStr)
	res = cur.fetchall()
	if (not res or len(res) == 0):
		errMsg = "fail to calculate sbox {0}".format(sqlStr)
		print(errMsg)
		raise Exception(errMsg)
	coverage = res[0][0]
	sqlStr = """INSERT INTO mwa.gleam_mosaic(centeralpha,centerdelta,coverage,center_freq,filename,accref,owner,embargo,mime,accsize) VALUES('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}')"""
	sqlStr = sqlStr.format(ra, dec, coverage, center_freq, fileId, accref_file, owner, embargo, mime, accsize)
	print sqlStr
	if (not DEBUG):
		cur.execute(sqlStr)

	sqlStr = """INSERT INTO dc.products(embargo,owner,accref, mime,accesspath,sourcetable) VALUES('{0}','{1}','{2}','{3}','{4}','{5}')"""
	sqlStr = sqlStr.format(embargo, owner, accref_file, mime, file_url, 'mwa.gleam_mosaic')
	print sqlStr
	print
	if (not DEBUG):
		cur.execute(sqlStr)
		conn.commit()
	if (cur):
		del cur

def insert_new_mosaic(conn, filename):
	accsize = os.path.getsize(filename)
	embargo = datetime.date.today() - datetime.timedelta(days = 1)
	owner="ICRAR"
	fileId = os.path.basename(filename) # e.g. mosaic_Week1_154-162MHz.fits
	freq = fileId.split('MHz')[0].split('_')[-1]
	accref_file =  "gleam/%s" % fileId
	myhost = "store04.icrar.org"
	file_url =  "http://%s:7777/RETRIEVE?file_id=%s" % (myhost, fileId)
	sqlStr = """INSERT INTO mwa.gleam_postage(filename,accref,owner,embargo,mime,accsize,freq) VALUES('{0}','{1}','{2}','{3}','{4}','{5}','{6}')"""
	sqlStr = sqlStr.format(fileId, accref_file, owner, embargo, mime, accsize, freq)
	print sqlStr
	cur = conn.cursor()
	if (not DEBUG):
		cur.execute(sqlStr)

	sqlStr = """INSERT INTO dc.products(embargo,owner,accref, mime,accesspath,sourcetable) VALUES('{0}','{1}','{2}','{3}','{4}','{5}')"""
	sqlStr = sqlStr.format(embargo, owner, accref_file, mime, file_url, 'mwa.gleam_postage')
	print sqlStr
	print
	if (not DEBUG):
		cur.execute(sqlStr)
		conn.commit()
	if (cur):
		del cur


def get_distance(long1, lat1, long2, lat2):
 	# http://www.johndcook.com/blog/python_longitude_latitude/
    # Convert latitude and longitude to
    # spherical coordinates in radians.
    # http://www.johndcook.com/lat_long_details.html
    degrees_to_radians = math.pi / 180.0

    # phi = 90 - latitude
    phi1 = (90.0 - lat1) * degrees_to_radians
    phi2 = (90.0 - lat2) * degrees_to_radians

    # theta = longitude
    theta1 = long1 * degrees_to_radians
    theta2 = long2 * degrees_to_radians

    # Compute spherical distance from spherical coordinates.

    # For two locations in spherical coordinates
    # (1, theta, phi) and (1, theta, phi)
    # cosine( arc length ) =
    #    sin phi sin phi' cos(theta-theta') + cos phi cos phi'
    # distance = rho * arc length

    cos = (math.sin(phi1) * math.sin(phi2) * math.cos(theta1 - theta2) +
           math.cos(phi1) * math.cos(phi2))
    arc = math.acos(cos) / degrees_to_radians
    print arc
    return arc


def get_week_by_coord(ra, dec):
	# need to consider the border case
	# do not use "elif"
	from operator import itemgetter
	res = []
	if (0 <= dec <= 30):
		if (0 <= ra <= 120): # 8h
			res.append((2, 60.0, 15.0)) # tuple (week, center_ra, center_dec)
		if (120 <= ra <= 217.5): # 14:30
			res.append((3, 168.75, 15.0))
		if (217.5 <= ra <= 330): # 22:00
			res.append((4, 273.75, 15.0))
	if (-30 <= dec <= 0):
		if (292.5 <= ra <= 360 or ra == 0):
			res.append((1, 326.25, -15.0))
		if (0 <= ra <= 120):
			res.append((2, 60.0, -15.0))
		if (120 <= ra <= 232.5):
			res.append((3, 176.25, -15.0))
		if (232.5 <= ra <= 292.5):
			res.append(4, 262.5, -15.0)
	if (-90 <= dec <= -30):
		if (315 <= ra <= 360 or ra == 0):
			res.append((1, 337.5, -60.0))
		if (0 <= ra <= 120):
			res.append((2, 60.0, -60.0))
		if (120 <= ra <= 202.5):
			res.append((3, 161.25, -60.0))
		if (202.5 <= ra <= 315):
			res.append((4, 258.75, -60.0))
	leng = len(res)
	if (leng == 1):
		return res[0][0]
	elif (leng > 1):
		dist = [get_distance(ra, dec, x[1], x[2]) for x in res]
		index = min(enumerate(dist), key=itemgetter(1))[0]
		return res[index][0]
	else:
		return 0

def do_it():
	"""
	"""
	# read from CSV file
	mosaic_list = "/home/ngas/processing/mosaic_cutout/new_mosaic_list"
	with open(mosaic_list) as f:
		content = f.readlines()
	conn = getVODBConn()
	try:
		for fn in content:
			#insert_db(conn, fn.replace("\n",""))
			insert_new_mosaic(conn, fn.replace("\n",""))
	finally:
		putVODBConn(conn)

if __name__ == '__main__':
	"""
	if len(sys.argv) == 2:
		fin = sys.argv[-1]
		print "Reading {0}".format(fin)
	else:
		print "usage: python {0} infile.fits".format(__file__)
		sys.exit()
	pgSqlStr = get_fits_mbr(fin)
	#print pgSqlStr
	"""
	do_it()
	#print get_week_by_coord(float(sys.argv[1]), float(sys.argv[2]))
