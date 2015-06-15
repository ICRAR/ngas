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
# cwu      01/10/2012  Created
"""
Contains a Filter Plug-In used to filter out those files that 
(1) have already been delivered to the remote destination
(2) belong to Solar observations with project_id 'c105' or 'c106'
"""

import os, threading

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import genLog, alert, info, NGAMS_SOCK_TIMEOUT_DEF, NGAMS_STATUS_CMD, NGAMS_FAILURE
from ngamsPClient import ngamsPClient
import pccFits.PccSimpleFitsReader as fitsapi


g_db_conn = None # MWA metadata database connection

#eor_list = ["'G0001'", "'G0004'", "'G0009'", "'G0008'", "'G0010'"] # EOR scientists are only interested in these projects
eor_list = [] # this has become a parameter of the plug-in
proj_separator = '___'

def ngamsMWA_MIT_NoTape_FilterPI(srvObj,
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
    projectId = ''
    onTape = 0
    
    try:        
        fh = fitsapi.getFitsHdrs(filename)
        projectId = fh[0]['PROJID'][0][1]
        
    except:
        err = "Did not find keyword PROJID in FITS file or PROJID illegal"
        errMsg = genLog("NGAMS_ER_DAPI_BAD_FILE", [os.path.basename(filename),
                                                   "ngamsMWA_MIT_FilterPlugIn", err])
        #raise Exception, errMsg
        #so still possible to deliver if the file is not there yet
    
    """
    if (projectId == "'C105'" or projectId == "'C106'"):
        return 0
    """
  
     # Parse plug-in parameters.
    parDic = []
    pars = ""
    if ((plugInPars != "") and (plugInPars != None)):
        pars = plugInPars
    elif (reqPropsObj != None):
        if (reqPropsObj.hasHttpPar("plug_in_pars")):
            pars = reqPropsObj.getHttpPar("plug_in_pars")
    parDic = ngamsPlugInApi.parseRawPlugInPars(pars)
    if (not parDic.has_key("remote_host") or 
        not parDic.has_key("remote_port") or
        not parDic.has_key("project_id")):
        errMsg = "ngamsMWACheckRemoteFilterPlugin: Missing Plug-In Parameter: " +\
                 "remote_host / remote_port / project_id"
        #raise Exception, errMsg
        alert(errMsg)
        return 1 # matched as if the filter did not exist
    
    host = parDic["remote_host"]
    sport = parDic["remote_port"]
    proj_ids = parDic["project_id"]
    
    if (proj_ids and len(proj_ids)):
        for proj_id in proj_ids.split(proj_separator):
            eor_list.append("'%s'" % proj_id)
        
        if (not (projectId in eor_list)):
            info(4, 'File %s is not EOR project' % fileId)
            return 0
    
    if (not sport.isdigit()):
        errMsg = "ngamsMWACheckRemoteFilterPlugin: Invalid port number: " + sport
        alert(errMsg)
        return 1 # matched as if the filter does not exist
    
    port = int(sport)
        
    # Perform the matching.
    client = ngamsPClient.ngamsPClient(host, port, timeOut = NGAMS_SOCK_TIMEOUT_DEF)
    
    try:
        rest = client.sendCmd(NGAMS_STATUS_CMD, 1, "", [["file_id", fileId]])
    except Exception, e:
        errMsg = "Error occurred during checking remote file status " +\
                     "ngamsMWACheckRemoteFilterPlugin. Exception: " + str(e)
        alert(errMsg)
        return 1 # matched as if the filter does not exist
    
    #info(5, "filter return status = " + rest.getStatus())
    
    if (rest.getStatus().find(NGAMS_FAILURE) != -1):
        tname = threading.current_thread().name
        beingSent = srvObj._subscrDeliveryFileDic.values()
        for fi in beingSent:
            if (srvObj._subscrDeliveryFileDic[tname] != fi and fi[0] == fileId and fi[2] == fileVersion):
                return 0 # this file is currently being sent, so do not send it again
        match = 1
    
    #info(4, "filter match = " + str(match))    
    
    return match    


# EOF
