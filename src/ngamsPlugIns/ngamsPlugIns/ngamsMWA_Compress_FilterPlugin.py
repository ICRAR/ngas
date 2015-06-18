#
#    (c) University of Western Australia
#    International Centre of Radio Astronomy Research
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
#    Copyright by UWA,
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
# cwu      07/Aug/2014  Created
"""
This filter will keep  all already-compressed FITS file from entering the job queue
"""

import os, commands

from ngamsLib.ngamsCore import error, info, warning

BINTB_STR = "XTENSION= 'BINTABLE'" # used to decide if a FITS file is compressed or not (only works for MWA visibility files)


def isMWAVisFile(fileId):
    fileName, fileExtension = os.path.splitext(fileId)
    if ('.fits' != fileExtension.lower()):
        return False # only FITS file is considered
    
    if (fileName.find('_gpubox') == -1):
        return False
    
    ss = fileName.split('_')
    if (len(ss) != 4):
        return False
    
    return True
    

def ngamsMWA_Compress_FilterPlugin(srvObj,
                          plugInPars,
                          filename,
                          fileId,
                          fileVersion = -1,
                          reqPropsObj = None):
    
    """
    srvObj:        Reference to NG/AMS Server Object (ngamsServer).

    plugInPars:    Parameters to take into account for the plug-in
                   execution (string).
   
    fileId:        File ID for file to test (string).

    filename:      Filename of (complete) (string).

    fileVersion:   Version of file to test (integer).

    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).
 
    Returns:       0 if the file does not match, 1 if it matches the
                   conditions (integer/0|1).
    """
    if (not isMWAVisFile(fileId)):
        return 0  # only add MWA Vis FITS file
    
    cmd = 'head -c %d %s' % (1024 * 3, filename)    
    try:
        #re = ngamsPlugInApi.execCmd(cmd, 60)
        re = commands.getstatusoutput(cmd)
    except Exception, ex:
        if (str(ex).find('timed out') != -1):
            error('Timed out when checking FITS header %s' % cmd)
        else:
            error('Exception when checking FITS header %s: %s' % (cmd, str(ex)))
        return 0
    
    
    if (0 == re[0]):
        a = re[1].find("XTENSION= 'BINTABLE'")
        if (a > -1):
            return 0 # if the file is already compressed, do not add again
        else:
            info(3, "File %s added" % filename)
            return 1 
    else:
        warning('Fail to check header for file %s: %s' % (filename, re[1]))
        return 0