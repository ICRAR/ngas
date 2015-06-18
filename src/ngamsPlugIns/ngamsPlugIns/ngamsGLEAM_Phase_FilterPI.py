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
# cwu      18/Nov/2014  Created
"""
Pick images based on its GLEAM phase (phase 1 or 2?)
"""

import os

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import warning
import pccFits.PccSimpleFitsReader as fitsapi


QUERY_MAX_VER = "SELECT MAX(file_version) FROM ngas_files WHERE file_id = '%s'"

def _isLatestVer(srvObj, fileId, fileVersion):
    res = srvObj.getDb().query(QUERY_MAX_VER % fileId)
    if (res == [[]]):
        return True
    else:
        max_ver = int(res[0][0][0])
        return (fileVersion == max_ver)

def isGLEAMImage(fileId):
    return (fileId.lower().endswith('.fits') and (len(fileId.split('_')) == 5) and (fileId.find('mosaic') == -1))

def getGLEAMPhase(filename):
    fileId = os.path.basename(filename)
    hdrs = fitsapi.getFitsHdrs(filename)
    """
    copied from ngamsGLEAM_VO_JobPlugin
    """
    gleam_phase = 1
    getf_frmfn = 0
    if (hdrs[0].has_key('ORIGIN')):
        fits_origin = hdrs[0]['ORIGIN'][0][1]
        if (fits_origin.find('WSClean') > -1):
            gleam_phase = 2
    else:
        getf_frmfn = 1
    
    if (getf_frmfn == 1 and fileId.split('_v')[1].split('.')[0] == '2'): # filename pattern is brittle, only use it if no fits header key: ORIGIN
        gleam_phase = 2
    return gleam_phase

def ngamsGLEAM_Phase_FilterPI(srvObj,
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
    if (not isGLEAMImage(fileId)):
        return 0
    
    if (not _isLatestVer(srvObj, fileId, fileVersion)):
        return 0
    
    parDic = []
    pars = ""
    if ((plugInPars != "") and (plugInPars != None)):
        pars = plugInPars
    elif (reqPropsObj != None):
        if (reqPropsObj.hasHttpPar("plug_in_pars")):
            pars = reqPropsObj.getHttpPar("plug_in_pars")
    parDic = ngamsPlugInApi.parseRawPlugInPars(pars)
    
    if (parDic.has_key('phase')):
        phase = int(parDic['phase'])
    else:
        return 1 # no need to check phase
    
    img_phase = None
    try:
        img_phase = getGLEAMPhase(filename)
    except Exception, exp:
        warning("cannot get phase info from %s, file not added: %s" % (filename, str(exp)))
        return 0
    
    if (phase == img_phase):
        return 1
    else:
        return 0
    