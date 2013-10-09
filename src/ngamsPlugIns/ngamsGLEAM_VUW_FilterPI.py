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
# cwu      20/09/2013  Created

from ngams import *
import ngamsPlugInApi
import ngamsPClient

import os

file_ext = ['.fits', '.png']

def ngamsGLEAM_VUW_FilterPI(srvObj,
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
    match = 0
    fn, fext = os.path.splitext(fileId)
    if (fext.lower() in file_ext): # only send FITS files, no measurement sets
        parDic = []
        pars = ""
        if ((plugInPars != "") and (plugInPars != None)):
            pars = plugInPars
        elif (reqPropsObj != None):
            if (reqPropsObj.hasHttpPar("plug_in_pars")):
                pars = reqPropsObj.getHttpPar("plug_in_pars")
        parDic = ngamsPlugInApi.parseRawPlugInPars(pars)
        if (not parDic.has_key("remote_host") or 
            not parDic.has_key("remote_port")):
            errMsg = "ngamsGLEAM_VUW_FilterPI: Missing Plug-In Parameter: " +\
                     "remote_host / remote_port"
            #raise Exception, errMsg
            alert(errMsg)
            return 1 # matched as if the remote checking is done
        
        host = parDic["remote_host"]
        sport = parDic["remote_port"]
        
        if (not sport.isdigit()):
            errMsg = "ngamsGLEAM_VUW_FilterPI: Invalid port number: " + sport
            alert(errMsg)
            return 1 # matched as if the filter does not exist
    
        port = int(sport)
            
        # Perform the matching.
        client = ngamsPClient.ngamsPClient(host, port, timeOut = NGAMS_SOCK_TIMEOUT_DEF)        
        try:
            rest = client.sendCmd(NGAMS_STATUS_CMD, 1, "", [["file_id", fileId]])
        except Exception, e:
            errMsg = "Error occurred during checking remote file status " +\
                         "ngamsGLEAM_VUW_FilterPI. Exception: " + str(e)
            alert(errMsg)
            return 1 # matched as if the filter does not exist
        
        #info(5, "filter return status = " + rest.getStatus())
        if (rest.getStatus().find(NGAMS_FAILURE) != -1):
            match = 1
        #info(4, "filter match = " + str(match))    
    
    return match    
