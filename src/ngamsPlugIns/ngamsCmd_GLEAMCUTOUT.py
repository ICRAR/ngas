
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

from ngams import *

import math, time, commands, os
import ephem
import pyfits

qs = "select a.mount_point || '/' || b.file_name as file_full_path, b.file_version from ngas_disks a, ngas_files b where a.disk_id = b.disk_id and b.file_id = '%s' order by b.file_version desc"
cmd_cutout = "/mnt/gleam/software/wcstools-3.8.7/bin/getfits -sv -o %s -d %s %s %s %s J2000 %d %d" # % (outputfname, outputdir, inputfname, ra, dec, width, height)
cmd_fits2jpg = "/mnt/gleam/software/bin/fits2jpeg -fits %s -jpeg %s -nonLinear" # % (fitsfname, jpegfname)


def execCmd(cmd, failonerror = True, okErr = []):
    re = commands.getstatusoutput(cmd)
    if (re[0] != 0 and not (re[0] in okErr)):
        errMsg = 'Fail to execute command: "%s". Exception: %s' % (cmd, re[1])
        if (failonerror):
            raise Exception(errMsg)
        else:
            print errMsg
    return re

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
       
    fileId = reqPropsObj.getHttpPar("file_id")
    coord = reqPropsObj.getHttpPar("radec").split(',')
    
    try:
        ra = str(ephem.hours(float(coord[0]) * math.pi / 180)).split('.')[0] # convert degree to hour:minute:second, and ignore decimal seconds
        dec = str(ephem.degrees(float(coord[1]) * math.pi / 180)).split('.')[0] # convert degree to degree:minute:second, and ignore decimal seconds
        radius = float(reqPropsObj.getHttpPar("radius"))
    except Exception, ex:
        srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE, 
                     "GLEAMCUTOUT parameter validation failed: '%s'" % str(ex))
        return
    
    query = qs % fileId
    info(3, "Executing SQL query for GLEAM CUTOUT: %s" % str(query))
    res = srvObj.getDb().query(query, maxRetries=1, retryWait=0)
    reList = res[0]
    if (len(reList) < 1):
        srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE, 
                     "Cannot find image file: '%s'" % fileId)
        return
    
    filePath = reList[0][0] #GET the latest version only
    #filePath = '/Users/chen/Documents/StMan/StMan_distributed.png'
    
    hdulist = pyfits.open(filePath)
    width = abs(int(2 * radius / float(hdulist[0].header['CDELT1'])))
    height = abs(int(2 * radius / float(hdulist[0].header['CDELT2'])))
    hdulist.close()
    
    work_dir = srvObj.getCfg().getRootDirectory() + '/processing'    
    cut_fitsnm = ('%f' % time.time()).replace('.', '_') + '.fits'
    cmd1 = cmd_cutout % (cut_fitsnm, work_dir, filePath, ra, dec, width, height)
    try:
        info(3, "Executing command: %s" % cmd1)
        execCmd(cmd1)
    except Exception, excmd1:
        srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE, 
                     "Cutout failed: '%s'" % str(excmd1))
        return
    
    jpfnm = ('%f' % time.time()).replace('.', '_') + '.jpg'
    cmd2 = cmd_fits2jpg % (work_dir + '/' + cut_fitsnm, work_dir + '/' + jpfnm)
    try:
        info(3, "Executing command: %s" % cmd2)
        execCmd(cmd2)
    except Exception, excmd2:
        srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE,
                     "Conversion from FITS to JPEG failed: '%s'" % str(excmd2))
        return
    
    hdrInfo = ["Content-disposition", "inline;filename=gleamcutout.jpg"]
    
    srvObj.httpReplyGen(reqPropsObj,
                     httpRef,
                     NGAMS_HTTP_SUCCESS,
                     dataRef = work_dir + '/' + jpfnm,
                     dataInFile = 1,
                     contentType = 'image/jpeg',
                     contentLength = 0,
                     addHttpHdrs = [hdrInfo],
                     closeWrFo = 1)
    
    if (os.path.exists(work_dir + '/' + cut_fitsnm)):
        cmd_rm = 'rm %s/%s' % (work_dir, cut_fitsnm)
        execCmd(cmd_rm, failonerror = False)
    
    if (os.path.exists(work_dir + '/' + jpfnm)):
        cmd_rm = 'rm %s/%s' % (work_dir, jpfnm)
        execCmd(cmd_rm, failonerror = False)


