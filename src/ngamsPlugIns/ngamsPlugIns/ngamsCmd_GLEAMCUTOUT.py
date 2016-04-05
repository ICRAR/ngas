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
"""
read fits header, get cdel1,2 and epoch information
Cutout a gleam FITS image, convert it into png, and display in the browser, then remove the jpeg file
"""

import math, time, commands, os, traceback, threading
import ephem
import pyfits as pyfits_real
import astropy.io.fits as pyfits
import astropy.wcs as pywcs
from string import Template

from ngamsLib.ngamsCore import NGAMS_HTTP_SUCCESS, NGAMS_FAILURE, info, NGAMS_TEXT_MT


"""
disk_host_dict = {"3bcfe8b4996a5c15d91e32f287a1a574":"store02:7777",
"b66b9398e32632132b298311f838f752":"store04:7777",
"50adb38a33ab4230519f60cc74ad2095":"store06:7777"}
"""

# cache for "select host_id, ip_address from ngas_hosts"
host_id_ip_dict = {"store02:7777":"180.149.251.184",
"store04:7777":"180.149.251.176", "store06:7777":"180.149.251.151"}

wcstools_path_dict = {"store02:7777":"/mnt/gleam/software/wcstools-3.8.7",
"store04:7777":"/home/ngas/software/wcstools-3.9.2",
"store06:7777":"/home/ngas/software/wcstools-3.9.2"}

ds9_path_dict = {"store02:7777":"/mnt/gleam/software/bin",
"store04:7777":"/home/ngas/software",
"store06:7777":"/home/ngas/software"}

montage_reproj_exec = "/home/ngas/software/Montage_v3.3/bin/mProject"
montage_cutout_exec = "/home/ngas/software/Montage_v3.3/bin/mSubimage"
fits_copy_exec = "/home/ngas/software/fitscopy"

"""
/mnt/gleam/software/bin/ds9 -grid -geometry 1250x1250  $file -cmap Heat -scale limits $min_max -zoom 0.25 -saveimage "$outname" 100 -exit
/mnt/gleam/software/bin/ds9 -colorbar -grid -geometry 1250x1250  $file -cmap Heat -scale zscale -zoom 0.25 -saveimage "$outname" -exit
"""
#-grid skyformat degrees
cmd_ds9 = '{0}/ds9 -grid yes -grid numerics fontsize 14 -grid numerics fontweight bold -grid skyformat degrees -geometry 1250x1250  {1} -cmap Heat -zoom to fit -scale zscale -saveimage "{2}" -exit'
qs = "SELECT a.mount_point || '/' || b.file_name AS file_full_path, a.host_id FROM ngas_disks a, ngas_files b WHERE a.disk_id = b.disk_id AND b.file_id = {0} ORDER BY b.file_version DESC"

cmd_fits2jpg = "/mnt/gleam/software/bin/fits2jpeg -fits %s -jpeg %s -nonLinear" # % (fitsfname, jpegfname)
psf_seq = ['BMAJ', 'BMIN', 'BPA']

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

class AddPSFException(Exception):
    pass

"""
To add:
1. REDIRECT if file_id does not belong to me
2. Load different paths based on host_id
3. Perhaps using DS9 to convert
"""

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
    """
    try:
        import astropy.io.fits as pyfits
        import astropy.wcs as pywcs
    except ImportError:
        import pyfits
        import pywcs
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
    info(3, "Executing regridding {0}".format(cmd))
    st = time.time()
    execCmd(cmd)
    info(3, "Regridding {1} took {0} seconds".format((time.time() - st), dim))
    return hdr_tpl

def cutout_mosaics(ra, dec, radius, work_dir, filePath, do_regrid, cut_fitsnm, to_be_removed, cmd_cutout, use_montage=True, projection="ZEA"):
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
        hdulist = pyfits_real.open(filePath)
        cdelt_x = float(hdulist[0].header['CD1_1'])
        cdelt_y = float(hdulist[0].header['CD2_2'])
        width = abs(int(factor * radius / cdelt_x))
        height = abs(int(factor * radius / cdelt_y))
        hdulist.close()
        cmd1 = cmd_cutout % (cut_fitsnm, work_dir, filePath, ra0, dec0, width, height)

    info(3, "Executing command: %s" % cmd1)
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

def add_header(cut_fits_path, cut_psf_paths):
    """
    TODO
    """
    output = pyfits.open(cut_fits_path, mode='update')
    for i, t in enumerate(psf_seq):
        #psflist = pyfits.open(cut_psf_paths[i])
        #output[0].header[t] = numpy.nanmean(psflist[0].data[0])
        output[0].header[t] = cut_psf_paths[i]

    output[0].header['history'] = 'GLEAM-IDR2 11-Aug-2015'
    #output.writeto(cut_fits_path, clobber=True)
    output.close()

    """
    for cut_psf_path in cut_psf_paths:
        psflist = pyfits.open(cut_psf_path)
        bmaj = np.nanmean(psflist[0].data[0]) #is the array of major axis values
        bmin = np.nanmean(psflist[0].data[1]) #is the array of minor axis values
        bpa = np.nanmean(psflist[0].data[2]) #is the array of position angle values


        output[0].header['BMAJ'] = bmaj
        output[0].header['BMIN'] = bmin
        output[0].header['BPA'] = bpa
        output.writeto(cut_fits_path, clobber=True)
    """

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
        a = a_len - (a_len - a + 1)
    if (b >= b_len):
        raise Exception("Cutout centre is out of the PSF map: {0} >= {1}".format(b, b_len))
        b = b_len - (b_len - b + 1)
    bmaj = psflist[0].data[0][a][b]
    bmin = psflist[0].data[1][a][b]
    bpa = psflist[0].data[2][a][b]
    return (bmaj, bmin, bpa)


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

    my_host = srvObj.getHostId()
    if ("store04:7779" == my_host):
        my_host = "store04:7777"
    cmd_cutout = "{0}/bin/getfits -sv -o %s -d %s %s %s %s J2000 %d %d".format(wcstools_path_dict[my_host]) # % (outputfname, outputdir, inputfname, ra, dec, width, height)

    for attnm in attnm_list:
        if (not reqPropsObj.hasHttpPar(attnm)):
            srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE, #let HTTP returns OK so that curl can continue printing XML code
                     "GLEAMCUTOUT command failed: '%s' is not specified" % attnm)
            return
    fits_format = False
    if (reqPropsObj.hasHttpPar('fits_format') and '1' == reqPropsObj.getHttpPar("fits_format")):
        fits_format = True
    fileId = reqPropsObj.getHttpPar("file_id")
    info(3, "Executing SQL query for GLEAM CUTOUT: %s" % str(qs))
    reList = srvObj.getDb().query2(qs, args=(fileId,))
    if (len(reList) < 1):
        srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE,
                     "Cannot find image file: '%s'" % fileId)
        return
    file_host = reList[0][1]
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

    coord = reqPropsObj.getHttpPar("radec").split(',')
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

    work_dir = srvObj.getCfg().getRootDirectory() + '/processing'
    time_str = ('%f' % time.time()).replace('.', '_')
    cut_fitsnm = time_str + '.fits'
    to_be_removed = []
    try:
        if (not is_mosaic(fileId)):
            hdulist = pyfits_real.open(filePath)
            width = abs(int(2 * radius / float(hdulist[0].header['CDELT1'])))
            height = abs(int(2 * radius / float(hdulist[0].header['CDELT2'])))
            hdulist.close()
            cmd1 = cmd_cutout % (cut_fitsnm, work_dir, filePath, ra, dec, width, height)
            info(3, "Executing command: %s" % cmd1)
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

            cut_fitsnm = cutout_mosaics(ra, dec, radius, work_dir, filePath, do_regrid, cut_fitsnm, to_be_removed, cmd_cutout, use_montage=use_montage_cut, projection=projection)
            if (no_psf == False and fits_format):
                psf_fileId = fileId.split('.fits')[0] + '_psf.fits'
                info(3, "Executing SQL query for GLEAM PSF CUTOUT: %s" % str(qs))
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
                    #add_header(work_dir + '/' + cut_fitsnm, cut_psfnm_list)
                    try:
                        add_header(work_dir + '/' + cut_fitsnm, get_bparam(ra, dec, psf_path))
                    except Exception, hdr_except:
                        if (reqPropsObj.hasHttpPar('skip_psf_err') and '1' == reqPropsObj.getHttpPar("skip_psf_err")):
                            info(3, "PSF error skipped: {0}".format(hdr_except))
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
        info(3, traceback.format_exc())
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
        cmd2 = cmd_ds9.format(ds9_path_dict[my_host], work_dir + '/' + cut_fitsnm, work_dir + '/' + jpfnm)
        try:
            os.environ['DISPLAY'] = ":7777"
            info(3, "Executing command: %s" % cmd2)
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


