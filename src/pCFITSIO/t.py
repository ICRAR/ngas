import pcfitsio
import sys
import fitsio

fitsfilename = sys.argv[1]
p=pcfitsio.fits_open_file(fitsfilename,0)

print pcfitsio.fits_read_keyword(p,'NAXIS1')

print pcfitsio.fits_read_keyword(p,'NAXIS2')

print "Reading data"
data = pcfitsio.fits_read_img(p,1,41*41 ,0.)
print "done"
print data
pcfitsio.fits_close_file(p)
