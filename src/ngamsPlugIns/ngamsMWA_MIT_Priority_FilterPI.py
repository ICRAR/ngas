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

from ngams import *
import os
import ngamsPlugInApi
import ngamsPClient
import ngamsMWACortexTapeApi
import pccFits.PccSimpleFitsReader as fitsapi

g_db_conn = None # MWA metadata database connection

#eor_list = ["'G0001'", "'G0004'", "'G0009'", "'G0008'", "'G0010'"] # EOR scientists are only interested in these projects
eor_list = [] # this has become a parameter of the plug-in
proj_separator = '___'

"""
# Requested on 23-Nov-2013
obs_list = ['1062531320','1062531440','1062533760','1062535344','1062536200','1062538640','1062443688','1062443808','1062443936','1062444056',
             '1062444176','1062444296','1062444424','1062444544','1062444664','1062444784','1062444912','1062445032','1062445152','1062445272',
             '1062445400','1062445520','1062445640','1062445760','1062445888','1062446008','1062446128','1062446248','1062446376','1062446496',
             '1062446616','1062446736','1062446864','1062446984','1062447104','1062447224','1062447352','1062447472','1062447592','1062447712',
             '1062447840','1062447960','1062448080','1062448200','1062448328','1062448448','1062448568','1062448688','1062448816','1062448936',
             '1062449056','1062449176','1062449304','1062449424','1062449544','1062449664','1062449792','1062449912','1062450032','1062450152',
             '1062450280','1062450400','1062450520','1062450640','1062450768','1062450888','1062451008','1062451128','1062451256','1062451376',
             '1062451496','1062451616','1062451744','1062451864','1062451984','1062452104','1062452232','1062452352','1062452472','1062452592',
             '1062452720','1062452840','1062452960','1062453080','1062453208','1062453328','1062453448','1062453568','1062530096','1062532296',
             '1062532656','1062532784','1062532904','1062533024','1062533144','1062533272','1062533392','1062533512','1062533632','1062533760',
             '1062533880','1062534000','1062534120','1062534248','1062534368','1062534488','1062534608','1062534976','1062535712','1062536072',
             '1062536200','1062536320','1062536440','1062536560','1062536688','1062536808','1062536928','1062537048','1062537176','1062537296',
             '1062537416','1062537536','1062537664','1062537784','1062537904','1062538760','1062538880','1062539000','1062539128','1062539248',
             '1062539368','1062539488','1062539616','1062539736','1062539856']
"""
# Requested on 5-Dec-2013
obs_list = ['1068376200', '1062539736', '1062539856', '1062539976', '1062529856', '1062529976', '1062530096', '1062530216', '1062530344',
            '1062530464', '1062530584', '1062530704', '1062530832']

def ngamsMWA_MIT_Priority_FilterPI(srvObj,
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
    obsId = fileId.split('_')[0]
    if (not obsId in obs_list):
        return 0
    
    # Parse plug-in parameters.
    pars = ""
    if ((plugInPars != "") and (plugInPars != None)):
        pars = plugInPars
    elif (reqPropsObj != None):
        if (reqPropsObj.hasHttpPar("plug_in_pars")):
            pars = reqPropsObj.getHttpPar("plug_in_pars")
    parDic = ngamsPlugInApi.parseRawPlugInPars(pars)
    if (not parDic.has_key("remote_host") or 
        not parDic.has_key("remote_port")):
        errMsg = "ngamsMWACheckRemoteFilterPlugin: Missing Plug-In Parameter: " +\
                 "remote_host / remote_port"
        #raise Exception, errMsg
        alert(errMsg)
        return 1 # matched as if the filter did not exist
    
    host = parDic["remote_host"]
    sport = parDic["remote_port"]
    
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
        match = 1
    
    #info(4, "filter match = " + str(match))    
    
    return match    


# EOF
