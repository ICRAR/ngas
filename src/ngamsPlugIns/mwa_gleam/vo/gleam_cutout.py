"""
======
Cutout using montage if possible.
Ported by from
https://code.google.com/p/agpy/source/browse/trunk/agpy/cutout.py
======

Generate a cutout image from a .fits file
"""
try:
    import astropy.io.fits as pyfits
    import astropy.wcs as pywcs
except ImportError:
    import pyfits
    import pywcs
import numpy
import astrolib.coords as coords
"""
try:
    import astrolib.coords as coords
except ImportError:
    pass # maybe should do something smarter here, but I want agpy to install...
"""
try:
    import os
    os.environ['PATH'] += ':/home/ngas/software/Montage_v3.3/bin'
    import montage_wrapper as montage
    import os
    CanUseMontage = True
except ImportError:
    CanUseMontage = False

class DimensionError(ValueError):
    pass

def cutout(filename, xc, yc, xw=25, yw=25, units='wcs', outfile=None,
        clobber=True, useMontage=False, coordsys='celestial', verbose=False):
    """
    Inputs:
        file  - .fits filename or pyfits HDUList (must be 2D)
        xc,yc - x and y coordinates in the fits files' coordinate system (CTYPE)
        xw,yw - x and y width (pixels or wcs)
        units - specify units to use: either pixels or wcs
        outfile - optional output file
    """

    if isinstance(filename, str):
        file = pyfits.open(filename)
        opened = True
    elif isinstance(filename, pyfits.HDUList):
        file = filename
        opened = False
    else:
        raise Exception("cutout: Input file is wrong type (string or HDUList are acceptable).")

    head = file[0].header.copy()


    if head['NAXIS'] > 2:
        raise DimensionError("Too many (%i) dimensions!" % head['NAXIS'])

    cd1 = head.get('CDELT1') if head.get('CDELT1') else head.get('CD1_1')
    cd2 = head.get('CDELT2') if head.get('CDELT2') else head.get('CD2_2')
    if cd1 is None or cd2 is None:
        raise Exception("Missing CD or CDELT keywords in header")
    wcs = pywcs.WCS(head)

    """
    if units == 'wcs':
        if coordsys=='celestial' and wcs.wcs.lngtyp=='GLON':
            xc,yc = coords.Position((xc,yc),system=coordsys).galactic()
        elif coordsys=='galactic' and wcs.wcs.lngtyp=='RA':
            xc,yc = coords.Position((xc,yc),system=coordsys).j2000()
    """

    if useMontage and CanUseMontage:
        head['CRVAL1'] = xc
        head['CRVAL2'] = yc
        if units == 'pixels':
            head['CRPIX1'] = xw
            head['CRPIX2'] = yw
            head['NAXIS1'] = int(xw * 2)
            head['NAXIS2'] = int(yw * 2)
        elif units == 'wcs':
            head.toTxtFile('/tmp/original_temp_montage.hdr', clobber=True)
            cdelt = numpy.sqrt(cd1 ** 2 + cd2 ** 2)
            head['CRPIX1'] = xw / cdelt
            head['CRPIX2'] = yw / cdelt
            head['NAXIS1'] = int(xw * 2 / cdelt)
            head['NAXIS2'] = int(yw * 2 / cdelt)
            """
            head['CTYPE1'] = "RA---TAN"
            head['CTYPE2'] = "DEC---TAN"
            """

        head.toTxtFile('temp_montage.hdr', clobber=True)
        head.toTxtFile('/tmp/temp_montage.hdr', clobber=True)
        newfile = montage.wrappers.reproject_hdu(file[0],header='temp_montage.hdr',exact_size=True)
        os.remove('temp_montage.hdr')
    else:

        xx,yy = wcs.wcs_sky2pix(xc,yc,0)

        if units=='pixels':
            xmin,xmax = numpy.max([0,xx-xw]),numpy.min([head['NAXIS1'],xx+xw])
            ymin,ymax = numpy.max([0,yy-yw]),numpy.min([head['NAXIS2'],yy+yw])
        elif units=='wcs':
            xmin,xmax = numpy.max([0,xx-xw/numpy.abs(cd1)]),numpy.min([head['NAXIS1'],xx+xw/numpy.abs(cd1)])
            ymin,ymax = numpy.max([0,yy-yw/numpy.abs(cd2)]),numpy.min([head['NAXIS2'],yy+yw/numpy.abs(cd2)])
        else:
            raise Exception("Can't use units %s." % units)

        if xmax < 0 or ymax < 0:
            raise ValueError("Max Coordinate is outside of map: %f,%f." % (xmax,ymax))
        if ymin >= head.get('NAXIS2') or xmin >= head.get('NAXIS1'):
            raise ValueError("Min Coordinate is outside of map: %f,%f." % (xmin,ymin))

        head['CRPIX1']-=xmin
        head['CRPIX2']-=ymin
        head['NAXIS1']=int(xmax-xmin)
        head['NAXIS2']=int(ymax-ymin)

        if head.get('NAXIS1') == 0 or head.get('NAXIS2') == 0:
            raise ValueError("Map has a 0 dimension: %i,%i." % (head.get('NAXIS1'),head.get('NAXIS2')))

        img = file[0].data[ymin:ymax,xmin:xmax]
        newfile = pyfits.PrimaryHDU(data=img,header=head)
        if verbose: print "Cut image %s with dims %s to %s.  xrange: %f:%f, yrange: %f:%f" % (filename, file[0].data.shape,img.shape,xmin,xmax,ymin,ymax)

    if isinstance(outfile, str):
        newfile.writeto(outfile, clobber=clobber)

    if opened:
        file.close()

    return newfile