import numarray 
import pcfitsio

#print "PCFITSIO Based version"
def copy_hdu(fromfile,tofile,fromhdu = -1, verbose = 0):
#####################################################################
# Copies the fromhdu of fromfile to the end of tofile               #
#####################################################################
    import posixpath
    if verbose: print "Opening ",fromfile,
    p = pcfitsio.fits_open_file(fromfile,0)
    if verbose: print ". Done."
    if fromhdu!=-1:
	pcfitsio.fits_movabs_hdu(p,fromhdu)
    if posixpath.isfile(tofile):
	if verbose: print "Opening old file ",tofile
	t = pcfitsio.fits_open_file(tofile,1)
    else:
	if verbose: print "Creating new file ",tofile
	t = pcfitsio.fits_create_file(tofile)
    pcfitsio.fits_copy_hdu(p,t,0)
    pcfitsio.fits_close_file(p)
    pcfitsio.fits_close_file(t)


def get_number_extensions(filename, verbose=0):
#####################################################################
# Returns the number of extensions of the given filename            #
#####################################################################
    if verbose: print "Opening ",filename,
    p = pcfitsio.fits_open_file(filename,0)
    num = pcfitsio.fits_get_num_hdus(p)
    if verbose: print "Closing",filename,
    pcfitsio.fits_close_file(p)
    return num
 
def read_data(filename, hdu = -1, verbose=0):
#####################################################################
# Reads and return a NumPy array containing the data in extension   #
# hdu.                                                              #
#####################################################################
    if verbose: print "Opening ",filename,
    p = pcfitsio.fits_open_file(filename,0)
    if verbose: print ". Done."
    if hdu!=-1:
	pcfitsio.fits_movabs_hdu(p,hdu)
    if verbose: print "Reading image size.",
    n = pcfitsio.fits_read_keys_lng(p,"NAXIS",1,10)
    if verbose: print n,"Done."
    if n==[] or n==[0]:
	pcfitsio.fits_close_file(p)
	raise KeyError," Could not read NAXIS values"
    if verbose: print "Reading data.",
    data = 0
    l = []
    for i in range(len(n)):
	l.append(n[len(n)-i-1])
    data = pcfitsio.fits_read_img(p,1,numarray.multiply.reduce(l) ,0.)[0]
    if verbose: print "Data read ok.",numarray.shape(data),numarray.multiply.reduce(l)
    if verbose: print "Reshape ",l
	
    data=numarray.reshape(data,l)
    if verbose: print "Reshape ",l," ok."
    if verbose: print "Read",n,"=",numarray.multiply.reduce(n),"elements.",
    if verbose: print "Done."
    if verbose: print "Closing",filename,
    pcfitsio.fits_close_file(p)
    if verbose: print ".Done."
    return data
    

def write_data(filename, data, bitpix = -1, hdu = -1, append=0, verbose=0):
#############################################################################
# Write an array, or a list, to the file filename                           #
# The type of the array determines the value of bitpix, which can be        #
# overwritten if necessary. Data will be written to the extension           #
# indicated by the parameter hdu=. If that extension does not exist but the #
# parameter appen= is set to 1, then a new hdu is appended to the data      #
# Setting verbose=1 turns on the verbose mode                               #
#############################################################################
     import posixpath


     if type(filename) != type(" "): raise InvalidFilename
     try:
	 data = numarray.asarray(data)
#	 data = numarray.transpose(data)
	 naxes = numarray.asarray(numarray.shape(data)).tolist()
	 l = []
	 for i in range(len(naxes)):
	     l.append(naxes[len(naxes)-i-1])
	 naxes=l
     except:
	 raise InvalidData
     if bitpix == -1:
	 if data.typecode()=='l':
	     bitpix = 32
	 if data.typecode()=='d':
	     bitpix = -32
     if not (bitpix in [-32,8,16,32]):
	 print "Invalid BITPIX: ",bitpix
	 return
     ok = 0
     new = 1
     if (posixpath.isfile(filename)):
	 p = pcfitsio.fits_open_file(filename,1)
	 ok = 1
	 new = 0
	 if hdu != -1:
	     try:
		 if verbose: print "Trying to move to extension #"+`hdu`
		 pcfitsio.fits_movabs_hdu(p,hdu)
		 append = 0
		 ok = 1
	     except:
		 if verbose: print "Failed to move to extension #"+`hdu`
	 if append:
	     # Create a new image extension
	     if verbose: print "Appending an image  extension.",
	     pcfitsio.fits_create_img(p,bitpix,naxes)
	     if verbose: print "Done.",
	     # Find out the number of extensions
	 #    hdunum = pcfitsio.fits_get_num_hdus(p)
	 #    if verbose: print "Total #hdu:",hdunum
	 #    pcfitsio.fits_movabs_hdu(p,hdunum)
	     ok = 1
     else:
	 if verbose: print "File not found."
	 if verbose: print "Creating:",filename,
	 p = pcfitsio.fits_create_file(filename)
	 new = 1
	 if verbose: print "Done."

	 if verbose: print "Appending an image  extension.",
	 pcfitsio.fits_create_img(p,bitpix,naxes)
	 pcfitsio.fits_close_file(p)
	 p = pcfitsio.fits_open_file(filename,1)
	 if verbose: print "Done.",
	 # Find out the number of extensions
	 hdunum = pcfitsio.fits_get_num_hdus(p)
	 if verbose: print "Total #hdu:",hdunum
	 pcfitsio.fits_movabs_hdu(p,hdunum)
	 ok = 1
     # Write the data to the file which is now opened/created
     if ok:
	 if new == 0:
	     if verbose: print "Resizing img ext..",
	     pcfitsio.fits_resize_img(p,bitpix,naxes)
	 if verbose: print "Writing data.",naxes,
	 if verbose: print numarray.multiply.reduce(naxes)
	 pcfitsio.fits_write_img(p,1,numarray.multiply.reduce(naxes),data)
	 if verbose: print "Done."

	 if verbose: print "Closing",filename,
	 pcfitsio.fits_close_file(p)
	 if verbose: print "Done."


def read_keys(filename, keys, hdu = -1, verbose = 0):
    import fnmatch
    import string
    if type(keys) != type([]):
	keys = [keys]
    result = {}
    p = pcfitsio.fits_open_file(filename,0)
    if hdu!=-1:
	pcfitsio.fits_movabs_hdu(p,hdu)
    for key in keys:
	if "*" in key or "?" in key:
	    if verbose:
		print "Scanning all keys"
	    num=pcfitsio.fits_get_hdrspace(p)[0]
	    for i in range(1,num+1):
	        record = pcfitsio.fits_read_record(p,i)
		try:
		    if fnmatch.fnmatch(string.strip(record[0:8]),string.upper(key))\
		    and record[8]=='=':
			thiskey = record[0:8]
			result[string.strip(thiskey)]= pcfitsio.fits_read_keyword(p,thiskey)
		except:
		    1		    
	else:
	    result[string.strip(key)]=pcfitsio.fits_read_keyword(p,key)
    pcfitsio.fits_close_file(p)

    for key in result.keys():
	t = pcfitsio.fits_get_keytype(result[key][0])[0]
	if verbose: print "KEY TYPE=",t
	if t=='C':
	    result[key][0]=result[key][0][1:-1]
	if t == 'L':
	    result[key][0]=result[key][0]
	if t == 'I':
	    result[key][0]=string.atoi(result[key][0])
	if t == 'F':
	    result[key][0]=string.atof(result[key][0])
    return result


def dic_to_arr(h):
    import copy
    arr=[]
    for k in h.keys():
        tmp = copy.copy(h[k])
        tmp.insert(0,k)
        arr.append(tmp)
    return arr


def write_keys(filename, keys, hdu = -1, verbose = 0, add = 0):
##################################################################################
# This function take a list of records and write them as new records or updates  #
# exisiting ones (default). Each record should have 2 or three elements in the   #
# following order: key name, value, option comment.                              #
##################################################################################
    def write_one_record(p,record, add, verbose):
	try:
	    name = record[0]
	except:
	    raise InvalidRecord
	try:
	    value = record[1]
	except:
	    raise InvalidRecord
	try:
	    comment = record[2]
	except:
	    comment = ''

	if add == 1: 
	    if verbose: print "Adding the record (%s,%s,%s)" % (name,`value`,comment)
	    pcfitsio.fits_write_key(p,name,value,comment)
	if add == 0: 
	    if verbose: print "Updating the record (%s,%s,%s)" % (name,`value`,comment)
	    pcfitsio.fits_update_key(p,name,value,comment)

    p = pcfitsio.fits_open_file(filename,1)
    if hdu!=-1:
	pcfitsio.fits_movabs_hdu(p,hdu)
    if type(keys)==type([]):
        # The passed keys are in an array format
        for key in keys:
            if verbose: print 'key:',key
            write_one_record(p,key,add,verbose)
    pcfitsio.fits_close_file(p)


def xy2rd(filename, x,y,hdu = -1, hms = 0,  verbose = 0):
    from numarray import sin,cos,arctan2,sqrt
    def degtorad(num):
	return num/180.0*numarray.pi
    def radtodeg(num):
	return num/numarray.pi * 180.0
    def degtoHMS(num):
	mmm = int((num-int(num))*60.0)
	sss = ((num-int(num))*60.0-mmm)*60.0
	num = `int(num)`+":"+ `mmm`+":"+`sss`
	return num
	
    keys = ["CRPIX1","CRPIX2","CRVAL1","CRVAL2","CD1_1","CD1_2","CD2_1","CD2_2"]
    CD =  read_keys(filename,keys,hdu)
    crpix = numarray.zeros((2),numarray.Float)
    cd = numarray.zeros((2,2),numarray.Float)

    crpix[0] = CD['CRPIX1'][0]
    crpix[1] = CD['CRPIX2'][0]
    ra0 = CD['CRVAL1'][0]
    dec0 = CD['CRVAL2'][0]
    cd[0,0] = CD['CD1_1'][0]
    cd[0,1] = CD['CD1_2'][0]
    cd[1,0] = CD['CD2_1'][0]
    cd[1,1] = CD['CD2_2'][0]
    xi = cd[0, 0] * (x - crpix[0]) + cd[0, 1] * (y - crpix[1])
    eta = cd[1, 0] * (x - crpix[0]) + cd[1, 1] * (y - crpix[1])
    xi = degtorad(xi)
    eta = degtorad(eta)
    ra0 = degtorad(ra0)
    dec0 = degtorad(dec0)

    ra = arctan2(xi,cos(dec0)-eta*sin(dec0)) + ra0
    dec = arctan2(eta*cos(dec0)+sin(dec0),sqrt((cos(dec0)-eta*sin(dec0))**2 + xi**2))

    ra = radtodeg(ra)# % 360.0
  #  if ra < 0: ra = ra + 360.0
    dec = radtodeg(dec)

    if (hms):
	return degtoHMS(ra/15.0),degtoHMS(dec)
    else:
	return ra,dec


def rd2xy(filename,ra,dec,verbose=0,hdu=-1,hour=0):
    from numarray import sin,cos,arctan2,sqrt
    def degtorad(num):
	return num/180.0*numarray.pi
    def radtodeg(num):
	return num/numarray.pi * 180.0
    keys = ["CRPIX1","CRPIX2","CRVAL1","CRVAL2","CD1_1","CD1_2","CD2_1","CD2_2"]
    CD =  read_keys(filename,keys,hdu)
    crpix = numarray.zeros((2),numarray.Float)
    cd = numarray.zeros((2,2),numarray.Float)
    cdinv = numarray.zeros((2,2),numarray.Float)

    crpix[0] = CD['CRPIX1'][0]
    crpix[1] = CD['CRPIX2'][0]
    ra0 = CD['CRVAL1'][0]
    dec0 = CD['CRVAL2'][0]
    cd[0,0] = CD['CD1_1'][0]
    cd[0,1] = CD['CD1_2'][0]
    cd[1,0] = CD['CD2_1'][0]
    cd[1,1] = CD['CD2_2'][0]
    det = cd[0,0]*cd[1,1] - cd[0,1]*cd[1,0]
    if det == 0:
	raise SingularMatrix
    cdinv[0,0] = cd[1,1] / det
    cdinv[0,1] = -cd[0,1] / det
    cdinv[1,0] = -cd[1,0] / det
    cdinv[1,1] = cd[0,0] / det
    print det,cdinv
    ra0 = degtorad(ra0)
    dec0 = degtorad(dec0)
    if hour: ra=ra*15.0
    ra = degtorad(ra)
    dec = degtorad(dec)
    bottom = sin(dec)*sin(dec0) + cos(dec)*cos(dec0)*cos(ra-ra0)
    if bottom == 0:
	raise InvalidRaDecRange
    xi = cos(dec) * sin(ra-ra0) / bottom
    eta = (sin(dec)*cos(dec0) - cos(dec)*sin(dec0)*cos(ra-ra0)) / bottom
    xi = radtodeg(xi)
    eta = radtodeg(eta)
    x = cdinv[0, 0] * xi + cdinv[0, 1] * eta + crpix[0]
    y = cdinv[1, 0] * xi + cdinv[1, 1] * eta + crpix[1]
    return x,y
