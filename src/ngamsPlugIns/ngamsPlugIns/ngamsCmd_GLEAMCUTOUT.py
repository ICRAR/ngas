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
#
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      2014/06/06  Created
#
# This is for new NGAS only
#

"""
read fits header, get cdel1,2 and epoch information
Cutout a gleam FITS image, convert it into png, and display in the browser, then remove the jpeg file
"""

import commands
import logging
import math
import os
from string import Template
import threading
import time
import traceback

from astropy.coordinates import SkyCoord
import astropy.io.fits as pyfits
import astropy.units as u
import astropy.wcs as pywcs
import ephem

from ngamsLib.ngamsCore import NGAMS_HTTP_SUCCESS, NGAMS_FAILURE, NGAMS_TEXT_MT


logger = logging.getLogger(__name__)

week_date_dict = {
'1':'2013-08-13', '2':'2013-11-15', '3':'2014-03-10', '4':'2014-06-13'
}

"""
disk_host_dict = {"3bcfe8b4996a5c15d91e32f287a1a574":"store02:7777",
"b66b9398e32632132b298311f838f752":"store04:7777",
"50adb38a33ab4230519f60cc74ad2095":"store06:7777"}
"""

# cache for "select host_id, ip_address from ngas_hosts"
host_id_ip_dict = {"store02:7777":"180.149.251.184",
"store04:7777":"180.149.251.176",
"store06:7777":"180.149.251.151",
"ip-172-31-3-243:7777":"13.54.25.105"}

"""
wcstools_path_dict = {"store02:7777":"/mnt/gleam/software/wcstools-3.8.7",
"store04:7777":"/home/ngas/software/wcstools-3.9.2",
"store06:7777":"/home/ngas/software/wcstools-3.9.2"}
"""

wcstools_path = "/home/ngas/software/wcstools-3.9.2"

"""
ds9_path_dict = {"store02:7777":"/mnt/gleam/software/bin",
"store04:7777":"/home/ngas/software",
"store06:7777":"/home/ngas/software"}
"""

ds9_path = "/home/ngas/software"

montage_reproj_exec = "/home/ngas/software/Montage_v3.3/bin/mProject"
montage_cutout_exec = "/home/ngas/software/Montage_v3.3/bin/mSubimage"
fits_copy_exec = "/home/ngas/software/fitscopy"

"""
/mnt/gleam/software/bin/ds9 -grid -geometry 1250x1250  $file -cmap Heat -scale limits $min_max -zoom 0.25 -saveimage "$outname" 100 -exit
/mnt/gleam/software/bin/ds9 -colorbar -grid -geometry 1250x1250  $file -cmap Heat -scale zscale -zoom 0.25 -saveimage "$outname" -exit
"""
#-grid skyformat degrees
cmd_ds9 = '{0}/ds9 -grid yes -grid numerics fontsize 14 -grid numerics fontweight bold -grid skyformat degrees -geometry 1250x1250  {1} -cmap Heat -zoom to fit -scale zscale -saveimage "{2}" -exit'
qs = "SELECT a.mount_point || '/' || b.file_name AS file_full_path, a.host_id, b.ingestion_date FROM ngas_disks a, ngas_files b WHERE a.disk_id = b.disk_id AND b.file_id = {0} ORDER BY b.file_version DESC"
cmd_cutout = "{0}/bin/getfits -sv -o %s -d %s %s %s %s J2000 %d %d".format(wcstools_path) # % (outputfname, outputdir, inputfname, ra, dec, width, height)
cmd_fits2jpg = "/mnt/gleam/software/bin/fits2jpeg -fits %s -jpeg %s -nonLinear" # % (fitsfname, jpegfname)
psf_seq = ['BMAJ', 'BMIN', 'BPA']
completeness_path = '/home/ngas/NGAS/gleam_metadata'
completeness_fnames = ['cmp_map_faint.fits', 'cmp_map_mid.fits', 'cmp_map_bright.fits']

ds9_sem = threading.Semaphore(10)

html_info = """
<html>
<title>GLEAM CUTOUT</title>
<style>
body{
font-family:Arial, Helvetica, sans-serif;
font-size:13px;
}
.info, .success, .warning, .error, .validation {
border: 1px solid;
margin: 10px 0px;
padding:15px 10px 15px 50px;
background-repeat: no-repeat;
background-position: 10px center;
}
.info {
color: #00529B;
background-color: #BDE5F8;
}
.success {
color: #4F8A10;
background-color: #DFF2BF;
}
.warning {
color: #9F6000;
background-color: #FEEFB3;
}
.error {
color: #D8000C;
background-color: #FFBABA;
}

</style>
<body>
${html_content}
</body>
</html>
"""

lmc_info = (80.89417 - 4.22 / 2, 80.89417 + 4.22 / 2, -69.75611 - 4.22 / 2, -69.75611 + 4.22 / 2, 4.22 / 2) #ra, dec, diameter in degrees
smc_info = (13.15833 - 3.492 / 2, 13.15833 + 3.492 / 2, -72.80028 - 3.492 / 2, -72.80028 + 3.492 / 2, 3.492 / 2)
overlap_regions = [lmc_info, smc_info]
overlap_names = ['LMC', 'SMC']
wrong_sql = ['drop', 'delete', 'update']
overlap_err = """The current publicly available GLEAM data does not include this region.
Please see Hurley-Walker et al (2016) for details of currently published
regions"""

class AddPSFException(Exception):
    pass

"""
To add:
1. REDIRECT if file_id does not belong to me
2. Load different paths based on host_id
3. Perhaps using DS9 to convert
"""

def sql_inject_test(query):
    """
    This is not desirable, but a quick fix for the cases of
    drop table and delete/update records
    """
    lq = query.lower()
    for ws in wrong_sql:
        if (lq.find(ws) > -1):
            err_msg = "Invalid query '{0}'".format(query)
            raise Exception(err_msg)


def overlap_check(ra, dec, radius):
    """
    A very rough implementation, not using any distance or coverage calculation
    for efficiency
    """
    for i, re in enumerate(overlap_regions):
        if (ra >= re[0] and ra <= re[1] and dec >= re[2] and dec <= re[3] and
        radius <= re[4]):
            #raise Exception("Sorry the region '{0}' does not contain any GLEAM sources".format(overlap_names[i]))
            raise Exception(overlap_err)

    icrs_coord = SkyCoord(ra * u.degree, dec * u.degree, frame='icrs')
    gg = icrs_coord.galactic
    b = float(str(gg.to_string().split(',')[0]).split()[1])
    if (b < 9 and b > -9):
        #raise Exception("Sorry Galactic Plane does not contain any GLEAM sources.")
        raise Exception(overlap_err)

def execCmd(cmd, failonerror = True, okErr=[]):
    """
    sp = subprocess.Popen(cmd.split())
    return sp.wait()
    """
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

def regrid_fits(infile, outfile, xc, yc, xw, yw, work_dir, projection="ZEA"):
    """
    all units of distances are in degrees
    both file names are strings

    return the header template file (string) to be deleted
    """
    #import astro_field
    file = pyfits.open(infile)
    head = file[0].header.copy()
    dim = file[0].data.shape
    #cd1 = head.get('CDELT1') if head.get('CDELT1') else head.get('CD1_1')
    #cd2 = head.get('CDELT2') if head.get('CDELT2') else head.get('CD2_2')
    #if cd1 is None or cd2 is None:
    #    raise Exception("Missing CD or CDELT keywords in header")
    #f1 = astro_field.Field([xc, yc], xw * 2)
    #ref_val = f1.cornersLonLat[2]
    if (projection != 'ZEA'):
        try:
            del head['RADESYS']
        except KeyError, ke:
            pass
        head['EQUINOX'] = 2000.
        head['CTYPE1'] = "RA---{0}".format(projection)
        head['CTYPE2'] = "DEC--{0}".format(projection)
    head['CRVAL1'] = xc#ref_val[0]#xc
    head['CRVAL2'] = yc#ref_val[1]#yc
    #cdelt = numpy.sqrt(cd1 ** 2 + cd2 ** 2)
    #1.5 = 3 / 2
    head['CRPIX1'] = dim[1] / 1.5 / 2#1#xw / cdelt
    head['CRPIX2'] = dim[0] / 1.5 / 2#1#yw / cdelt

    head['NAXIS1'] = int(dim[1] / 1.5)#int(xw * 2 / cdelt)
    head['NAXIS2'] = int(dim[0] / 1.5)#int(yw * 2 / cdelt)
    st = str(time.time()).replace('.', '_')
    hdr_tpl = '{0}/{1}_temp_montage.hdr'.format(work_dir, st)
    head.toTxtFile(hdr_tpl, clobber=True)
    cmd = "{0} {1} {2} {3}".format(montage_reproj_exec,infile, outfile,hdr_tpl)
    logger.debug("Executing regridding %s", cmd)
    st = time.time()
    execCmd(cmd)
    logger.debug("Regridding %s took %s seconds", dim, time.time() - st)
    return hdr_tpl

def cutout_mosaics(ra, dec, radius, work_dir, filePath, do_regrid, cut_fitsnm, to_be_removed, use_montage=True, projection="ZEA"):
    outfile_nm = "{0}/{1}".format(work_dir, cut_fitsnm)
    #factor = 2
    if (ra < 0):
        ra += 360

    if (do_regrid):
        factor = 3
    else:
        factor = 2

    if (use_montage):
        cmd1 = "{0} {1} {2} {3} {4} {5} {6}".format(montage_cutout_exec,
                                                    filePath, outfile_nm,
                                                    ra, #float(coord[0]),
                                                    dec, #float(coord[1]),
                                                    radius * factor,
                                                    radius * factor)
    else:
        ra0 = str(ephem.hours(ra * math.pi / 180)).split('.')[0]
        dec0 = str(ephem.degrees(dec * math.pi / 180)).split('.')[0]
        hdulist = pyfits.open(filePath)
        cdelt_x = float(hdulist[0].header['CD1_1'])
        cdelt_y = float(hdulist[0].header['CD2_2'])
        width = abs(int(factor * radius / cdelt_x))
        height = abs(int(factor * radius / cdelt_y))
        hdulist.close()
        cmd1 = cmd_cutout % (cut_fitsnm, work_dir, filePath, ra0, dec0, width, height)

    logger.debug("Executing command: %s", cmd1)
    execCmd(cmd1)
    to_be_removed.append(work_dir + '/' + cut_fitsnm)

    if (do_regrid):
        #import gleam_cutout
        outfile_proj_nm = "{0}/proj_{1}".format(work_dir, cut_fitsnm)
        hdr_tpl = regrid_fits(outfile_nm,
                              outfile_proj_nm,
                              ra,
                              dec,
                              radius,
                              radius,
                              work_dir,
                              projection=projection)
        """
        gleam_cutout.cutout(outfile_nm, float(coord[0]), float(coord[1]),
                            xw=radius, yw=radius, outfile=outfile_proj_nm,
                            useMontage=True)
        """
        to_be_removed.append(hdr_tpl)

        cut_fitsnm = "proj_" + cut_fitsnm
        area_fitsnm = cut_fitsnm.replace(".fits", "_area.fits")
        to_be_removed.append(work_dir + '/' + area_fitsnm)
        to_be_removed.append(work_dir + '/' + cut_fitsnm)
    return cut_fitsnm

def add_header(cut_fits_path, cut_psf_paths, ing_date, obs_date, completeness=None):
    """
    TODO
    ing_date:
        string (e.g. 2014-11-28T23:56:36.709)
    """
    output = pyfits.open(cut_fits_path, mode='update')
    for i, t in enumerate(psf_seq):
        #psflist = pyfits.open(cut_psf_paths[i])
        #output[0].header[t] = numpy.nanmean(psflist[0].data[0])
        output[0].header[t] = cut_psf_paths[i]
    yr = ing_date.split('-')[0]
    if (yr == '2015'):
        hdr_hist = 'GLEAM-IDR2 11-Aug-2015'
    else:
        if (ing_date.split('T')[0] == '2016-05-23'):
            hdr_hist = 'GLEAM-IDR4 23-May-2016'
        else:
            hdr_hist = 'GLEAM-IDR3 14-Mar-2016'
    output[0].header['history'] = hdr_hist
    #output.writeto(cut_fits_path, clobber=True)
    #if (not output[0].header.has_key('BUNIT')):
    output[0].header['BUNIT'] = ('JY/BEAM', 'Units are in Jansky per beam')
    output[0].header['DATE-OBS'] = obs_date
    hks = output[0].header.keys()
    if (not 'ROBUST' in hks):
        output[0].header['ROBUST'] = -1
    if (not 'WSCWEIGH' in hks):
        output[0].header['WSCWEIGH'] = 'Briggs(-1)'

    if (completeness is not None):
        ra = completeness[0]
        dec = completeness[1]
        for cf in completeness_fnames:
            ffp = '%s/%s' % (completeness_path, cf)
            if (not os.path.exists(ffp)):
                logger.error("Completeness map %s does not exist", ffp)
                continue
            try:
                compt_perctg = get_compt_perctg(ra, dec, ffp)
            except Exception:
                logger.exception("fail to add completeness")
                continue
            if (compt_perctg is None):
                continue
            for cpg in compt_perctg:
                output[0].header['CMPL%04d' % (cpg[0])] = (cpg[1], 'Completeness of 200MHz catalogue at {0}mJy (%)'.format(cpg[0]))

    output.close()

def get_compt_perctg(ra, dec, compt_path):
    """
    Get the completeness percentage

    Flux limit      keyword name     description
    example value
    25mJy           CMPL0025         # Completeness of catalogue at 25mJy (%)
       12.1
    100mJy          CMPL0100         # Completeness of catalogue at 100mJy (%)
       76.4
    1000mJy         CMPL1000         # Completeness of catalogue at 1000mJy (%)
       99.3

    The examples are made-up but show the precision that would be sensible to
    use. There are 20 flux limits; I'm only showing three examples here.

    return:
        A tuple of (completeness, percentage)

    completeness_path = '/home/ngas/NGAS/gleam_metadata'
    completeness_fnames = ['cmp_map_faint.fits', 'cmp_map_mid.fits', 'cmp_map_bright.fits']
    """
    cplist = pyfits.open(compt_path)
    header = cplist[0].header
    w = pywcs.WCS(header, naxis=2)
    num_intensity_level = int(header['NAXIS3'])
    start_intensity = int(header['CRVAL3'])
    step_width = int(header['CDELT3'])
    pixcoords = w.wcs_world2pix([[ra, dec]], 0)[0]
    pixcoords = [int(elem) for elem in pixcoords]
    a = pixcoords[1]
    b = pixcoords[0]
    a_len = cplist[0].data[0].shape[0]
    b_len = cplist[0].data[0].shape[1]

    if (a >= a_len or b >= b_len):
        logger.error("Cutout centre is out of the completness map: %d >= %d or %d >= %d ", a, a_len, b, b_len)
        return None

    ret = []
    for i in range(num_intensity_level):
        il = start_intensity + i * step_width
        pert = int(cplist[0].data[i][a][b])
        #pert = round(float("{0:.2f}".format(pert)), 2)
        ret.append((il, pert))

    return ret


def get_bparam(ra, dec, psf_path):
    """
    """
    #psflist = pyfits.open('mosaic_Week2_freqrange_psf.fits')
    psflist = pyfits.open(psf_path)
    w = pywcs.WCS(psflist[0].header, naxis=2)
    pixcoords = w.wcs_world2pix([[ra, dec]], 0)[0]
    #pixcoords = w.wcs_world2pix([[ra, dec]], 1)[0]
    #pixcoords = [int(round(elem)) for elem in pixcoords]
    pixcoords = [int(elem) for elem in pixcoords]
    a = pixcoords[1]
    b = pixcoords[0]
    a_len = psflist[0].data[0].shape[0]
    b_len = psflist[0].data[0].shape[1]
    if (a >= a_len):
        raise Exception("Cutout centre is out of the PSF map: {0} >= {1}".format(a, a_len))
        a = a_len - (a_len - a + 1) # a = a - 1?
    if (b >= b_len):
        raise Exception("Cutout centre is out of the PSF map: {0} >= {1}".format(b, b_len))
        b = b_len - (b_len - b + 1)
    bmaj = psflist[0].data[0][a][b]
    bmin = psflist[0].data[1][a][b]
    bpa = psflist[0].data[2][a][b]
    return (bmaj, bmin, bpa)

def get_date_obs(file_id):
    try:
        week = file_id.split('_')[1][-1]
        return week_date_dict[week]
    except:
        logger.error("fail to get the obsdate for %s", file_id)
        return 'UNKNOWN'

def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Find out which threads are still dangling

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    attnm_list = ['file_id', 'radec', 'radius']

    for attnm in attnm_list:
        if (not reqPropsObj.hasHttpPar(attnm)):
            srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE, #let HTTP returns OK so that curl can continue printing XML code
                     "GLEAMCUTOUT command failed: '%s' is not specified" % attnm)
            return
    coord = reqPropsObj.getHttpPar("radec").split(',')
    check_overlap = True
    if (reqPropsObj.hasHttpPar('gleamer') and '1' == reqPropsObj.getHttpPar("gleamer")):
        check_overlap = False

    if (check_overlap):
        try:
            overlap_check(float(coord[0]), float(coord[1]), float(reqPropsObj.getHttpPar("radius")))
        except Exception, ex:
            # srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE,
            #              "GLEAMCUTOUT parameter validation failed: '%s'" % str(ex))
            srvObj.httpReply(reqPropsObj, httpRef, 500, str(ex), NGAMS_TEXT_MT)
            return

    fits_format = False
    if (reqPropsObj.hasHttpPar('fits_format') and '1' == reqPropsObj.getHttpPar("fits_format")):
        fits_format = True
    fileId = reqPropsObj.getHttpPar("file_id")
    query = qs.format("'%s'" % fileId)
    sql_inject_test(query)
    logger.debug("Executing SQL query for GLEAM CUTOUT: %s", str(query))
    #res = srvObj.getDb().query(query, maxRetries=1, retryWait=0)
    #reList = res[0]
    reList = srvObj.getDb().query2(qs, args=(fileId,))
    if (len(reList) < 1):
        srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE,
                     "Cannot find image file: '%s'" % fileId)
        return
    file_host = reList[0][1]
    my_host = srvObj.getHostId()
    if (file_host != my_host):
        if (not host_id_ip_dict.has_key(file_host)):
            srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE,
                     "Invalid file_host: '%s'" % file_host)
            return

        """
        redirect to file_host
        """
        srvObj.httpRedirReply(reqPropsObj, httpRef, host_id_ip_dict[file_host], 7777)
        return


    try:
        if (not is_mosaic(fileId)):
            ra = str(ephem.hours(float(coord[0]) * math.pi / 180)).split('.')[0] # convert degree to hour:minute:second, and ignore decimal seconds
            dec = str(ephem.degrees(float(coord[1]) * math.pi / 180)).split('.')[0] # convert degree to degree:minute:second, and ignore decimal seconds
        radius = float(reqPropsObj.getHttpPar("radius"))
    except Exception, ex:
        srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE,
                     "GLEAMCUTOUT parameter validation failed: '%s'" % str(ex))
        return

    filePath = reList[0][0] #GET the latest version only
    try:
        ing_date = reList[0][2] # ingestion_date
    except:
        ing_date = '2016-03-14T23:56:36.709'
        logger.debug("ingestion date parse failure")

    work_dir = srvObj.getCfg().getRootDirectory() + '/processing'
    time_str = ('%f' % time.time()).replace('.', '_')
    cut_fitsnm = time_str + '.fits'
    to_be_removed = []
    try:
        if (not is_mosaic(fileId)):
            hdulist = pyfits.open(filePath)
            width = abs(int(2 * radius / float(hdulist[0].header['CDELT1'])))
            height = abs(int(2 * radius / float(hdulist[0].header['CDELT2'])))
            hdulist.close()
            cmd1 = cmd_cutout % (cut_fitsnm, work_dir, filePath, ra, dec, width, height)
            logger.debug("Executing command: %s", cmd1)
            execCmd(cmd1)
            to_be_removed.append(work_dir + '/' + cut_fitsnm)
        else:
            ra = float(coord[0])
            dec = float(coord[1])
            do_regrid = (reqPropsObj.hasHttpPar('regrid') and '1' == reqPropsObj.getHttpPar("regrid"))
            projection = 'ZEA'
            if (reqPropsObj.hasHttpPar('projection')):
                projection = reqPropsObj.getHttpPar("projection")
            if (projection != 'ZEA'):
                do_regrid = True # always regrid if reprojection is needed
            no_psf = (reqPropsObj.hasHttpPar('nopsf') and '1' == reqPropsObj.getHttpPar("nopsf"))
            use_montage_cut = False
            if (reqPropsObj.hasHttpPar('use_montage') and '1' == reqPropsObj.getHttpPar("use_montage")):
                use_montage_cut = True

            cut_fitsnm = cutout_mosaics(ra, dec, radius, work_dir, filePath, do_regrid, cut_fitsnm, to_be_removed, use_montage=use_montage_cut, projection=projection)
            if (no_psf == False and fits_format):
                psf_fileId = fileId.split('.fits')[0] + '_psf.fits'
                query = qs.format("'%s'" % psf_fileId)
                sql_inject_test(query)
                logger.debug("Executing SQL query for GLEAM PSF CUTOUT: %s", str(query))
                #pres = srvObj.getDb().query(query, maxRetries=1, retryWait=0)
                #psfList = pres[0]
                psfList = srvObj.getDb().query2(qs, args=(psf_fileId,))
                if (len(psfList) > 0):
                    psf_path = psfList[0][0]

                    """
                    dim = pyfits.open(psf_path)[0].shape
                    cut_psfnm_list = []
                    for i, t in enumerate(psf_seq):
                        psf_path_splitnm = work_dir + '/' + psf_fileId.replace('_psf.fits', '{1}_psf{0}.fits'.format(i, time_str))
                        cmd_split = "{0} '{1}[1:{2}, 1:{3}, {4}:{4}]' {5}".format(fits_copy_exec,
                                                                                psf_path,
                                                                                dim[2],
                                                                                dim[1],
                                                                                i + 1,
                                                                                psf_path_splitnm)
                        info(3, "Executing fitscopy split: %s" % cmd_split)
                        execCmd(cmd_split)
                        cut_psfnm = cut_fitsnm.replace('.fits', '_psf{0}.fits'.format(i))
                        cutout_mosaics(ra, dec, radius, work_dir, psf_path_splitnm, do_regrid, cut_psfnm, to_be_removed)
                        cut_psfnm_list.append(work_dir + '/' + cut_psfnm)
                        to_be_removed.append(psf_path_splitnm)
                    """
                    try:
                        add_header(work_dir + '/' + cut_fitsnm, get_bparam(ra, dec, psf_path), ing_date, get_date_obs(fileId), completeness=(ra, dec))
                    except Exception, hdr_except:
                        if (reqPropsObj.hasHttpPar('skip_psf_err') and '1' == reqPropsObj.getHttpPar("skip_psf_err")):
                            logger.debug("PSF error skipped: %s", hdr_except)
                        else:
                            raise AddPSFException(str(hdr_except))

    except Exception, excmd1:
        """
        srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE,
                     "Cutout failed: '%s'" % str(excmd1))
        """
        if (type(excmd1) is AddPSFException):
            do_regrid = (reqPropsObj.hasHttpPar('regrid') and '1' == reqPropsObj.getHttpPar("regrid"))
            if (do_regrid):
                re_grid = 1
            else:
                re_grid = 0
            url = "http://store04.icrar.org:7777/GLEAMCUTOUT?file_id={0}&radec={1}&regrid={2}&radius={3}&fits_format=1&nopsf=1".format(fileId,
                                                                                                                    reqPropsObj.getHttpPar("radec"),
                                                                                                                    re_grid,
                                                                                                                    reqPropsObj.getHttpPar("radius"))
            html_dict = {}
            html_content = "<div class=\"warning\">Failed to add PSF info to the FITS header: {0}.</div> <div class=\"info\"><a href='{1}'>Proceed without PSF info.</a></div>".format(excmd1, url)
            html_dict["html_content"] = html_content
            ts = Template(html_info)
            err_msg = ts.safe_substitute(html_dict)
            srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, err_msg, "text/html")
        else:
            if (reqPropsObj.hasHttpPar('debug') and '1' == reqPropsObj.getHttpPar("debug")):
                err_msg = traceback.format_exc()
            else:
                err_msg = "Cutout failed: '%s'" % str(excmd1)
            srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, err_msg, NGAMS_TEXT_MT)
        logger.debug(traceback.format_exc())
        for fn in to_be_removed:
            if (os.path.exists(fn)):
                os.remove(fn)
        return
    #http://store04.icrar.org:7777/GLEAMCUTOUT?radec=92.4125,20.4867&radius=1&file_id=mosaic_Week2_223-231MHz.fits&regrid=0
    #http://store04.icrar.org:7777/GLEAMCUTOUT?radec=92.4125,20.4867&radius=1&file_id=mosaic_Week2_223-231MHz.fits&regrid=0&fits_format=1
    fn_suff = ""
    if (is_mosaic(fileId)):
        freq_part = fileId.split("_")[-1].split(".fits")[0]
        loc_part = "{0}_{1}".format(coord[0], coord[1])
        fn_suff = "_{0}_{1}".format(freq_part, loc_part)
    if (fits_format):
        hdr_fnm = "gleam_cutout{0}.fits".format(fn_suff)
        hdr_cttp = "image/fits"
        hdr_dataref = work_dir + '/' + cut_fitsnm
    else:
        ds9_sem.acquire()
        ttt = time.time()
        jpfnm = ('%f' % ttt).replace('.', '_') + '.jpg'
        cmd2 = cmd_ds9.format(ds9_path, work_dir + '/' + cut_fitsnm, work_dir + '/' + jpfnm)
        try:
            os.environ['DISPLAY'] = ":7777"
            logger.debug("Executing command: %s", cmd2)
            execCmd(cmd2)
        except Exception, excmd2:
            srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE,
                         "Conversion from FITS to JPEG failed: '%s', display = '%s'" % (str(excmd2), os.getenv('DISPLAY', 'NOTSET!')))
            return
        finally:
            ds9_sem.release()

        hdr_fnm = "gleam_cutout{0}.jpg".format(fn_suff)
        hdr_cttp = "image/jpeg"
        hdr_dataref = work_dir + '/' + jpfnm
        to_be_removed.append(hdr_dataref)

    hdrInfo = ["Content-Disposition", "inline;filename={0}".format(hdr_fnm)]

    srvObj.httpReplyGen(reqPropsObj,
                     httpRef,
                     NGAMS_HTTP_SUCCESS,
                     dataRef = hdr_dataref,
                     dataInFile = 1,
                     contentType = hdr_cttp,
                     contentLength = 0,
                     addHttpHdrs = [hdrInfo],
                     closeWrFo = 1)

    for fn in to_be_removed:
        if (os.path.exists(fn)):
            os.remove(fn)
