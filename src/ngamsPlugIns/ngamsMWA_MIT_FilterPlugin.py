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
import os, threading
import ngamsPlugInApi
import ngamsPClient
#import ngamsMWACortexTapeApi
import ngamsSubscriptionThread
import pccFits.PccSimpleFitsReader as fitsapi

import psycopg2 # used to connect to MWA M&C database
from psycopg2.pool import ThreadedConnectionPool

# maximum connection = 5
g_db_pool = ThreadedConnectionPool(1, 5, database = 'mwa', user = 'mwa', 
                            password = 'Qm93VGll\n'.decode('base64'), 
                            host = 'ngas01.ivec.org')

g_db_conn = None # MWA metadata database connection

#eor_list = ["'G0001'", "'G0004'", "'G0009'", "'G0008'", "'G0010'"] # EOR scientists are only interested in these projects
eor_list = [] # this has become a parameter of the plug-in
proj_separator = '___'


def getMWADBConn():
    if (g_db_pool):
        return g_db_pool.getconn()
    else:
        raise Exception('connection pool is None when get conn')
    """
    global g_db_conn
    if (g_db_conn and (not g_db_conn.closed)):
        return g_db_conn
    try:        
        g_db_conn = psycopg2.connect(database = 'mwa', user = 'mwa', 
                            password = 'Qm93VGll\n'.decode('base64'), 
                            host = 'ngas01.ivec.org')
        return g_db_conn 
    except Exception, e:
        errStr = 'Cannot create MWA DB Connection: %s' % str(e)
        raise Exception, errStr
    """
def putMWADBConn(conn):
    if (g_db_pool):
        g_db_pool.putconn(conn)
    else:
        raise Exception('connection pool is None when put conn')

def executeQuery(conn, sqlQuery):
    try:
        cur = conn.cursor()
        cur.execute(sqlQuery)
        return cur.fetchall()
    finally:
        if (cur):
            del cur
        putMWADBConn(conn)
            
def getProjectIdFromMWADB(fileId):
    conn = getMWADBConn()
    sqlQuery = "SELECT projectid FROM mwa_setting WHERE starttime = %s" % (fileId.split('_')[0])
    res = executeQuery(conn, sqlQuery)
    
    for re in res:
        return re[0]
    
    return None

def ngamsMWA_MIT_FilterPlugin(srvObj,
                          plugInPars,
                          filename,
                          fileId,
                          fileVersion = -1,
                          reqPropsObj = None,
                          checkMode = ngamsSubscriptionThread.FPI_MODE_BOTH):
    
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
        alert(errMsg)
        return 0 
        
    proj_ids = parDic["project_id"]
    match = 0
    
    fspi = srvObj.getCfg().getFileStagingPlugIn()
    if (not fspi):
        offline = -1
    else:
        exec "import " + fspi
        info(2,"Invoking FSPI.isFileOffline: " + fspi + " to check file: " + filename)
        offline = eval(fspi + ".isFileOffline(filename)")  
    
    if (checkMode == ngamsSubscriptionThread.FPI_MODE_BOTH or 
        checkMode == ngamsSubscriptionThread.FPI_MODE_METADATA_ONLY):    
        try:    
            if (offline == 1 or offline == -1):
                # if the file is on Tape or query error, query db instead, otherwise implicit tape staging will block all other threads!!
                info(3, 'File %s appears on Tape, connect to MWA DB to check' % filename)
                projId = getProjectIdFromMWADB(fileId)
                if (not projId or projId == ''):
                    alert('Cannot get project id from MWA DB for file %s' % fileId)
                    return 0
                projectId = "'%s'" % projId # add single quote to be consistent with FITS header keywords 
            else:
                fh = fitsapi.getFitsHdrs(filename)
                projectId = fh[0]['PROJID'][0][1]
        except:
            err = "Did not find keyword PROJID in FITS file or PROJID illegal"
            errMsg = genLog("NGAMS_ER_DAPI_BAD_FILE", [os.path.basename(filename),
                                                       "ngamsMWA_MIT_FilterPlugIn", err])
        if (proj_ids and len(proj_ids)):
            for proj_id in proj_ids.split(proj_separator):
                eor_list.append("'%s'" % proj_id)
            
            if (not (projectId in eor_list)):
                return 0
            elif (checkMode == ngamsSubscriptionThread.FPI_MODE_METADATA_ONLY):
                return 1
    
    if (checkMode == ngamsSubscriptionThread.FPI_MODE_BOTH or
        checkMode == ngamsSubscriptionThread.FPI_MODE_DATA_ONLY):
        host = parDic["remote_host"]
        sport = parDic["remote_port"]  
        if (not sport.isdigit()):
            errMsg = "ngamsMWACheckRemoteFilterPlugin: Invalid port number: " + sport
            alert(errMsg)
            return 1 # matched as if the filter does not exist
        
        port = int(sport)
            
        # Perform the remote checking see if the file has been sent.
        client = ngamsPClient.ngamsPClient(host, port, timeOut = NGAMS_SOCK_TIMEOUT_DEF)
        queryError = 0
        try:
            rest = client.sendCmd(NGAMS_STATUS_CMD, 1, "", [["file_id", fileId]])
        except Exception, e:
            errMsg = "Error occurred during checking remote file status " +\
                         "ngamsMWACheckRemoteFilterPlugin. Exception: " + str(e)
            alert(errMsg)
            match = 1 # matched as if the filter does not exist  
            queryError = 1          
        #info(5, "filter return status = " + rest.getStatus())
        if (queryError == 0 and rest.getStatus().find(NGAMS_FAILURE) != -1):
            #info(4, 'file %s is not at MIT, checking if other threads are sending it' % fileId)
            tname = threading.current_thread().name
            beingSent = srvObj._subscrDeliveryFileDic.values()
            for fi in beingSent:
                if (srvObj._subscrDeliveryFileDic[tname] != fi and fi[0] == fileId and fi[2] == fileVersion):
                    #info(4, 'file %s is being sent by another thead, so do not send it in this thread' % fileId)
                    return 0 # this file is currently being sent, so do not send it again
            info(3, 'file %s will be sent' % fileId)
            match = 1       
        #info(4, "filter match = " + str(match))    
        if (1 == offline and 1 == match and fspi):
            info(3, "File " + filename + " is offline, staging for delivery...")
            num = eval(fspi + ".stageFiles([filename])")
            if (num == 0):
                errMsg = 'File %s is offline, errors occurred when staging online for delivery' % filename
                error(errMsg)
                raise Exception(errMsg)
            info(3, "File " + filename + " staging completed for delivery.")
        
        return match    


# EOF
