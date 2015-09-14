
#    ICRAR - International Centre for Radio Astronomy Research
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#

#******************************************************************************
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      2015/Aug/17  Created

"""
This module provides functions to manipulate gleam mosaics
"""
import os, sys, math, commands, time
from os import listdir
#import pccFits.PccSimpleFitsReader as fitsapi
from optparse import OptionParser
import ephem
import astropy.io.fits as pyfits
import multiprocessing as mp
import numpy as np

montage_cutout_exec = "/home/ngas/software/Montage_v3.3/bin/mSubimage"
wcstools_cutout_exec = "/home/ngas/software/wcstools-3.9.2/bin/getfits -sv -o %s -d %s %s %s %s J2000 %d %d"
montage_reproj_exec = "/home/ngas/software/Montage_v3.3/bin/mProject"
if (not (os.path.exists(montage_reproj_exec))):
    montage_reproj_exec = "/Users/Chen/proj/Montage_v3.3/bin/mProject"
montage_jpeg_exec = "/home/ngas/software/Montage_v3.3/bin/mJPEG"

DEBUG = False

def execCmd(cmd, failonerror=True, okErr=[]):
    """
    sp = subprocess.Popen(cmd.split())
    return sp.wait()
    """
    if (DEBUG):
        print "DEBUG -- {0}".format(cmd)
        return [0, 'OK']
    else:
        re = commands.getstatusoutput(cmd)
        if (re[0] != 0 and not (re[0] in okErr)):
            errMsg = 'Fail to execute command: "%s". Exception: %s' % (cmd, re[1])
            if (failonerror):
                raise Exception(errMsg)
            else:
                print errMsg
        return re

def is_mosaic(file_id):
    return file_id.startswith('mosaic_')

def cut_effective_area(infile_nm, outfile_path, hires_path=None):
    """
    infile_nm:  full path (string)
    outfile_path: full path (string)
    hires_path: full path to the high resolution fits that have been cut
    """
    ifn = os.path.basename(infile_nm)
    if (ifn.find("mosaic_part") > -1):
        print "File '{0}'' has already been processed".format(ifn)
        return
    print "Cutting out {0} into {1}".format(infile_nm, outfile_path)
    if (not os.path.exists(outfile_path)):
        print "Output directory {0} does not exist!".format(outfile_path)
        return

    ra_dec_w_h = []
    week = int(ifn.split('_')[1][-1]) # highly brittle
    if (1 == week):
        w = (19.5 - 0) * 15
        ra = (19.5 + 24) * 15 / 2
        h = 30
        dec = -15
        ra_dec_w_h.append((ra, dec, w, h))

        w = (21 - 0) * 15
        ra = (21 + 24) * 15 / 2
        h = 60
        dec = (-30 - 90) / 2
        ra_dec_w_h.append((ra, dec, w, h))

    elif (2 == week):
        w = 8 * 15
        ra = w / 2
        h = 120
        dec = -30
        ra_dec_w_h.append((ra, dec, w, h))

        """
        w = 8 * 15
        ra = w / 2
        h = 30
        dec = -15
        ra_dec_w_h.append((ra, dec, w, h))

        w = 8 * 15
        ra = w / 2
        h = 60
        dec = -60
        ra_dec_w_h.append((ra, dec, w, h))
        """

    elif (3 == week):
        w = (14.5 - 8) * 15
        ra = (14.5 + 8) * 15 / 2
        h = 30
        dec = h / 2
        ra_dec_w_h.append((ra, dec, w, h))

        w = (15.5 - 8) * 15
        ra = (15.5 + 8) * 15 / 2
        h = 30
        dec = -15
        ra_dec_w_h.append((ra, dec, w, h))

        w = (13.5 - 8) * 15
        ra = (13.5 + 8) * 15 / 2
        h = 60
        dec = -60
        ra_dec_w_h.append((ra, dec, w, h))

    elif (4 == week):
        w = (22 - 14.5) * 15
        ra = (22 + 14.5) * 15 / 2
        h = 30
        dec = 15
        ra_dec_w_h.append((ra, dec, w, h))

        w = (19.5 - 15.5) * 15
        ra = (19.5 + 15.5) * 15 / 2
        h = 30
        dec = -15
        ra_dec_w_h.append((ra, dec, w, h))

        w = (21 - 13.5) * 15
        ra = (21 + 13.5) * 15 / 2
        h = 60
        dec = -60
        ra_dec_w_h.append((ra, dec, w, h))
    else:
        print "Unrecognised week {0}".format(week)
    cutout_first = False
    for c, it in enumerate(ra_dec_w_h):
        cut_fitsnm_pt = ifn.replace("mosaic_", "mosaic_part{0}_".format(c))
        rg_infile = outfile_path + "/" + cut_fitsnm_pt
        if (not os.path.exists(rg_infile)):
            ra = str(ephem.hours(it[0] * math.pi / 180)).split('.')[0]
            dec = str(ephem.degrees(it[1] * math.pi / 180)).split('.')[0]
            hdr = pyfits.open(infile_nm)[0].header
            cdelt_x = float(hdr['CD1_1'])
            cdelt_y = float(hdr['CD2_2'])
            width = abs(int(it[2] / cdelt_x))
            height = abs(int(it[3] / cdelt_y))

            cmd = wcstools_cutout_exec % (cut_fitsnm_pt, outfile_path, infile_nm, ra, dec, width, height)
            print "Executing " + cmd
            execCmd(cmd, failonerror=False)

        # upsampling against the hi-res grid
        hires_outfile = rg_infile.replace("mosaic_part{0}_".format(c),
                                       "mosaic_part{0}_hires_".format(c))
        if (hires_path and (not os.path.exists(hires_outfile))):
            hires_hdr_fits = None
            for hires in listdir(hires_path):
                his = hires.split('_')
                if (week == int(his[2][-1]) and c == int(his[1][-1])): # highly brittle
                    hires_hdr_fits = hires_path + "/" + hires
                    break
            if (hires_hdr_fits is None):
                print "Failed to find high res fits for week {0} in {1}".format(week, hires_path)
                continue

            regrid_fits(hires_hdr_fits, rg_infile, hires_outfile, outfile_path)

def split_mosaics(mosaic_path, output_path, hires_path=None):
    flist = listdir(mosaic_path)
    pool = mp.Pool(4)
    results = [pool.apply_async(cut_effective_area, args=('{0}/{1}'.format(mosaic_path, f), output_path, hires_path)) for f in flist]
    output = [p.get() for p in results]
    for ou in output:
        print ou
    """
    for f in flist:
        if (is_mosaic(f)):
            ff = '{0}/{1}'.format(mosaic_path, f)
            cut_effective_area(ff, output_path, hires_path=hires_path)
    """

def regrid_fits(header_fits, infile, outfile, work_dir):
    """
    all units of distances are in degrees
    both file names are strings
    """
    in_file_hdu = pyfits.open(header_fits)
    head = in_file_hdu[0].header.copy()
    dim = in_file_hdu[0].data.shape

    st = str(time.time()).replace('.', '_')

    hdr_tpl = outfile.replace(".fits", ".hdr")
    if (os.path.exists(hdr_tpl)):
        os.remove(hdr_tpl)
    head.toTxtFile(hdr_tpl, clobber=True)

    #for i in range(4):
    area_file = outfile.replace(".fits", "_area.fits")
    if (os.path.exists(area_file)):
        os.remove(area_file)

    #factor = 1.1 + 0.1 * i # shrink factor
    cmd = "{0} {1} {2} {3}".format(montage_reproj_exec, infile, outfile, hdr_tpl)
    print("Executing regridding {0}".format(cmd))
    st = time.time()
    execCmd(cmd)
    print("Regridding {1} took {0} seconds".format((time.time() - st), dim))
    lh = pyfits.open(outfile)
    dim_low = lh[0].data.shape
    lh.close()
    if (dim_low != dim):
        print "dim mismatch {0}:{1} vs. {2}{3}".format(header_fits, dim, outfile, dim_low)
    in_file_hdu.close()
    return hdr_tpl

def upsample_fits_img(hires_fn, lores_fn):
    """
    upsample low resolution image to the same size as the high resolution
    using the drizzling algorithm (via Montage)
    """
    # regrid both low and high using high's header
    work_dir = '/tmp/gleamvo/upsample'
    lobnm = os.path.basename(lores_fn)
    outfnm = lobnm.split(".")[0] + "_upspl.fits"
    i = 1
    while (os.path.exists(outfnm)):
        outfnm = lobnm.split(".")[0] + "_upspl_%d.fits" % i
        i += 1
    regrid_fits(hires_fn, lores_fn, work_dir + '/' + outfnm, work_dir)

def colorfits_to_jpeg():
    """
    /Users/Chen/proj/Montage_v3.3/bin/mJPEG  -red fornax_103-134_upspl.fits 0% 99.98% linear -green ./fornax_139-170_upspl.fits 0% 99.98% linear -blue /Users/Chen/Downloads/fornax_170-231.fits 0% 99.98% linear -out out1.jpg
    """
    base_dir = "/mnt/mwa_scratch_01fs/aladin_img/gleam_hips/color"

    lr = listdir("%s/103-134/hires" % base_dir)
    lr.sort()
    lr = [base_dir + "/103-134/hires/" + f for f in lr]

    lg = listdir("%s/139-170/hires" % base_dir)
    lg.sort()
    lg = [base_dir + "/139-170/hires/" + f for f in lg]

    lb = listdir("%s/170-231/non-regrid" % base_dir)
    lb.sort()
    lb = [base_dir + "/170-231/non-regrid/" + f for f in lb]

    out_dir = "/mnt/mwa_scratch_01fs/aladin_img/gleam_hips/color/RGB"

    for i, f in enumerate(lb):
        bn = os.path.basename(f)
        cmd = "{0} -red {1} 0% 99.98% linear -green {2} 0% 99.98% linear -blue {3} 0% 99.98% linear -out {4}".format(montage_jpeg_exec,
                                                                                                                     lr[i],
                                                                                                                     lg[i],
                                                                                                                     f,
                                                                                                                     out_dir + "/" + bn.replace("170-231MHz.fits", "Color.jpg"))
        print cmd
        execCmd(cmd)


def check_dim(move_to=None, fix_to=None):
    base_dir = "/mnt/mwa_scratch_01fs/aladin_img/gleam_hips/color"

    lr = listdir("%s/103-134/hires" % base_dir)
    lr.sort()
    lr = [base_dir + "/103-134/hires/" + f for f in lr]

    lg = listdir("%s/139-170/hires" % base_dir)
    lg.sort()
    lg = [base_dir + "/139-170/hires/" + f for f in lg]

    lb = listdir("%s/170-231/non-regrid" % base_dir)
    lb.sort()
    lb = [base_dir + "/170-231/non-regrid/" + f for f in lb]

    # first check all the dimensions are correct
    if (move_to and (not os.path.exists(move_to))):
        print "Move to {0} does not exist.".format(move_to)
        move_to = None

    for i, f in enumerate(lb):
        bl = pyfits.open(f)
        hb = bl[0].data.shape

        gl = pyfits.open(lg[i])
        hg = gl[0].data.shape

        rl = pyfits.open(lr[i])
        hr = rl[0].data.shape

        blhdr = bl[0].header

        l_bad = None
        bad_l = None

        if (hb == hg):
            pass
        else:
            l_bad = lg # file path
            bad_l = gl # file handle
            bad_dim = hg
        if (hb == hr):
            if (hb != hg):
                print
        else:
            l_bad = lr
            bad_l = rl
            bad_dim = hr

        if (l_bad and bad_l):
            badhdr = bad_l[0].header
            print "Mismatch between {0}:{1} and {2}:{3}".format(os.path.basename(lb[i]), hb, os.path.basename(l_bad[i]), hg)
            print "CRVAL1 left: {0}, CRVAL1 right: {1}".format(blhdr['CRVAL1'], badhdr['CRVAL1'])
            print "CRVAL2 left: {0}, CRVAL2 right: {1}".format(blhdr['CRVAL2'], badhdr['CRVAL2'])
            print "CRPIX1 left: {0}, CRPIX1 right: {1}".format(blhdr['CRPIX1'], badhdr['CRPIX1'])
            print "CRPIX2 left: {0}, CRPIX2 right: {1}".format(blhdr['CRPIX2'], badhdr['CRPIX2'])
            if (l_bad == lr):
                print
            if (move_to):
                os.rename(l_bad[i], "%s/%s" % (move_to, os.path.basename(l_bad[i])))
            if (fix_to and os.path.exists(fix_to)):
                a = bad_l[0].data
                b = np.zeros(hb)
                delta_0 = hb[0] - bad_dim[0]
                delta_1 = hb[1] - bad_dim[1]
                if (delta_0 > 0 and delta_1 > 0):
                    b[delta_0:, delta_1:] = a
                    for j in range(delta_0):
                        b[j, delta_1:] = a[0, :]
                        b[delta_0:, j] = a[:, 0]
                elif (delta_0 > 0):
                    b[delta_0:, :] = a
                    for j in range(delta_0):
                        b[j, :] = a[0, :] #pad with a's first row
                elif (delta_1 > 0):
                    b[:, delta_1:] = a
                    for j in range(delta_1):
                        b[:, j] = a[:, 0] #pad with a's first col
                bad_l[0].data = b
                bad_l[0].header['CRPIX2'] = blhdr['CRPIX2']
                bad_l[0].header['NAXIS2'] = blhdr['NAXIS2']
                bad_l[0].header['CRPIX1'] = blhdr['CRPIX1']
                bad_l[0].header['NAXIS1'] = blhdr['NAXIS1']
                write_file = '{0}/{1}'.format(fix_to, os.path.basename(l_bad[i]))
                print "Writing fixed file to %s" % write_file
                bad_l.writeto(write_file)
                bad_l.close()

if __name__ == '__main__':
    test_single = True
    if (test_single):
        #montage_reproj_exec = "/Users/Chen/proj/Montage_v3.3/bin/mProject"
        #upsample_fits_img('/Users/Chen/Downloads/lmc_231_3d.fits', '/Users/Chen/Downloads/lmc_72_4d.fits')
        #upsample_fits_img('/Users/Chen/Downloads/fornax_170-231.fits', '/Users/Chen/Downloads/fornax_072-103.fits')
        #upsample_fits_img('/Users/Chen/Downloads/fornax_170-231.fits', '/Users/Chen/Downloads/fornax_103-134.fits')
        #upsample_fits_img('/Users/Chen/Downloads/fornax_170-231.fits', '/Users/Chen/Downloads/fornax_139-170.fits')
        #check_dim(move_to="/mnt/mwa_scratch_01fs/aladin_img/gleam_hips/color/mismatched")
        #          fix_to="/mnt/mwa_scratch_01fs/aladin_img/gleam_hips/color/fixed")
        #check_dim(fix_to="/mnt/mwa_scratch_01fs/aladin_img/gleam_hips/color/fixed")
        #check_dim()
        colorfits_to_jpeg()
    else:
        parser = OptionParser()
        parser.add_option("-m", "--mosaicdir", action="store", type="string", dest="mosaic_path",
                          help="mosaic path (input)")
        parser.add_option("-o", "--outdir", action="store", type="string", dest="output_path",
                          help="output path for cutout images")

        parser.add_option("-r", "--hiresdir", action="store", type="string", dest="hires_path",
                          help="high resolution fits path against which low res fits are upsampled")

        (options, args) = parser.parse_args()
        if (None == options.mosaic_path or None == options.output_path):
            parser.print_help()
            sys.exit(1)

        split_mosaics(options.mosaic_path, options.output_path, hires_path=options.hires_path)






