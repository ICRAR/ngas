#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
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
# Who                   When             What
# -----------------   ----------      ------------
# chen.wu@icrar.org  29/Aug/2013        Created
"""
This module pushes important Cortex files to Pawsey in a semi-automated fashion
usage:

nohup python ngamsMWA_Pawsey_PushFiles.py -s 146.118.84.64 -p 7777 -m cortex.ivec.org:7781 > ~/MWA_HSM/test/pushfile.log &
"""

import cPickle as pickle
from cPickle import UnpicklingError
import logging
from optparse import OptionParser
import socket
import urllib2, time
import psycopg2

from ngamsLib.ngamsCore import NGAMS_STATUS_CMD, NGAMS_FAILURE, \
    NGAMS_SOCK_TIMEOUT_DEF
from ngamsPClient import ngamsPClient
from ngamsPlugIns.ngamsMWAAsyncProtocol import AsyncListRetrieveRequest


mime_type = 'application/octet-stream'
proxy_archive = 'storage01.icrar.org:7777'

g_db_conn = None # MWA metadata database connection
l_db_conn = None

logger = logging.getLogger(__name__)

def getMWADBConn():
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

def getLTADBConn():
    global l_db_conn
    if (l_db_conn and (not l_db_conn.close)):
        return l_db_conn
    try:
        l_db_conn = psycopg2.connect(database = 'ngas', user= 'ngas', 
                            password = 'bmdhcyRkYmE=\n'.decode('base64'), 
                            host = '192.102.251.250')
        return l_db_conn
    except Exception, e:
        errStr = 'Cannot create LTA DB Connection: %s' % str(e)
        raise Exception, errStr

def executeQuery(conn, sqlQuery):
    try:
        cur = conn.cursor()
        cur.execute(sqlQuery)
        return cur.fetchall()
    finally:
        if (cur):
            del cur

def updateQuery(conn, sqlQuery):
    try:
        cur = conn.cursor()
        cur.execute(sqlQuery)
        conn.commit()
    finally:
        if (cur):
            del cur
        
    
def getFileIdsByObsNum(obs_num):
    """
    Query the mwa database to get a list of files
    associated with this observation number
    
    obs_num:        observation number (string)
    
    Return:         file_list
    """
    sqlQuery = "SELECT filename FROM data_files WHERE observation_num = '%s' ORDER BY SUBSTRING(filename, 27);" % str(obs_num)
    try:
        mwa_conn = getMWADBConn()
    except Exception, eee:
        logger.error("MWA database connection error: %s" % str(eee))
        exit(1)
        
    res = executeQuery(mwa_conn, sqlQuery)
    
    retList = []
    for re in res:
        fileId = re[0]
        retList.append(fileId)
    return retList

def getUnprocessedObs(isGleam = False):
    """
    Get all observation numbers that have not been processed (to be delivered
    """
    if (isGleam):
        sqlQuery = "SELECT DISTINCT(substring(file_id, 0, 11)) myfield from ngas_files, ngas_gleam where ngas_files.file_version = 1 and substring(ngas_files.file_id, 0, 11) = ngas_gleam.obs_id and ngas_files.disk_id <> '01f54315bb54e7a5901f04fcae8168fc' and ngas_files.disk_id <> '1a1b0dcc261ed79a471035a0e3211fde' and ngas_gleam.async_sent = 0 order by myfield"
    else:
        sqlQuery = "SELECT obs_id FROM ngas_migration WHERE async_sent = 0 ORDER BY obs_id DESC"
    try:
        lta_conn = getLTADBConn()
    except Exception, eee:
        logger.error("NGAS database connection error: %s" % str(eee))
        exit(1)
        
    res = executeQuery(lta_conn, sqlQuery)
    
    retList = []
    for re in res:
        obsId = re[0]
        retList.append(obsId)
    return retList

def hasFilesInCortex(obsNum):
    """
    
    """
    sqlQuery = "SELECT COUNT(*) FROM ngas_files where file_id like '%s%%'" % obsNum
    lta_conn = getLTADBConn()
    res = executeQuery(lta_conn, sqlQuery)
    
    for re in res:
        return int(re[0])

def markObsDeliveredStatus(obsId, status = 1, isGleam = False):
    """
    mark an observation as delivered
    """
    tblname = 'ngas_migration'
    if (isGleam):
        tblname = 'ngas_gleam'
    sqlQuery = "UPDATE %s SET delivered = %d WHERE obs_id = '%s'" % (tblname, status, obsId)
    lta_conn = getLTADBConn()
    updateQuery(lta_conn, sqlQuery)
    
def markAsncSentStatus(obsId, status = 1, isGleam = False):
    """
    mark an observation as "async retrieve request sent"
    
    """
    tblname = 'ngas_migration'
    if (isGleam):
        tblname = 'ngas_gleam'
    sqlQuery = "UPDATE %s SET async_sent = %d WHERE obs_id = '%s'" % (tblname, status, obsId)
    lta_conn = getLTADBConn()
    updateQuery(lta_conn, sqlQuery)

def checkIfObsDelivered():
    """
    """
    pass

def getFileFullPath(fileId):
    """
    Given a file id, return its full path on Cortex
    """
    lta_conn = getLTADBConn()
    sqlQuery = "SELECT a.mount_point || '/' || b.file_name FROM ngas_disks a, ngas_files b where a.disk_id = b.disk_id AND b.file_version = 1 AND b.file_id = '%s'" % fileId
    res = executeQuery(lta_conn, sqlQuery)
    
    for re in res:
        return re[0]
    
def parseOptions():
    """
    Obtain the following parameters
    obs_num_list:       a list of observation numbers, separated by comma
    push_url:           the url to which we push files
    
    """
    parser = OptionParser()
    #parser.add_option("-o", "--obslist", dest = "obs_list", help = "a list of observation numbers, separated by comma")
    #parser.add_option("-u", "--pushurl", dest = "push_url", help = "the url to which we push files")
    parser.add_option("-s", "--host", dest = "push_host", help = "the host that will receive the file")
    parser.add_option("-p", "--port", dest = "port", help = "the port of this host")
    parser.add_option("-m", "--dm", dest = "data_mover", help = "the url of the data mover")    

    (options, args) = parser.parse_args()
    if (None == options.push_host or None == options.port or None == options.data_mover):
        parser.print_help()
        print 'Missing parameters'
        return None
    return options

def hasPawseyGotIt(client, fileId):
    try:
        rest = client.sendCmd(NGAMS_STATUS_CMD, 1, "", [["file_id", fileId]])
    except Exception, e:
        errMsg = "Error occurred during checking remote file status " +\
                     "Exception: " + str(e)
        logger.error(errMsg)
        return 0 # matched as if the filter does not exist
    
    if (rest.getStatus().find(NGAMS_FAILURE) != -1):
        return 0
    
    return 1

def getPushURL(hostId, gateway = None):
    """
    Construct the push url based on the hostId in the cluster
    
    hostId:    the host (e.g. 192.168.1.1:7777) that will receive the file
    
    gateway:   a list of gateway hosts separated by comma
               The sequence of this list is from target to source
               e.g. if the dataflow is like:  source --> A --> B --> C --> target
               then, the gateway list should be ordered as: C,B,A
    """
    if (gateway):
        gateways = gateway.split(',')
        gurl = 'http://%s/QARCHIVE' % hostId
        for gw in gateways:
            gurl = 'http://%s/PARCHIVE?nexturl=%s' % (gw, urllib2.quote(gurl))
        #return 'http://%s/PARCHIVE?nexturl=http://%s/QAPLUS' % (gateway, hostId)
        return gurl
    else:
        return 'http://%s/QARCHIVE' % hostId

def waitForNextObs(obsNum, statusUrl, sessionId, maxWaitTime, checkInterval = 60, isgleam = False):
    max_time = 0
    while (max_time <= maxWaitTime):
        time.sleep(checkInterval)
        max_time += checkInterval
        
        try:
            strRes = urllib2.urlopen(statusUrl + sessionId).read()
            myRes = pickle.loads(strRes)
            if (0 == myRes.number_files_to_be_delivered):
                # modify database
                markObsDeliveredStatus(obsNum, isGleam = isgleam)
                break
            elif (myRes.errorcode):
                markObsDeliveredStatus(obsNum, -1, isGleam = isgleam)
                break
        except (UnpicklingError, socket.timeout) as uerr:
            logger.error("Something wrong while getting status for obsNum %s, %s" % (obsNum, str(uerr)))
            continue
        

def main():
    gleam = False
    FORMAT = "%(asctime)-15s - %(name)s - %(levelname)s - %(message)s"
    logname = '/home/chenwu/MWA_HSM/test/push_migration_ngas.log'
    if (gleam):
        logname = '/home/chenwu/MWA_HSM/test/push_gleam_ngas.log'
    logging.basicConfig(filename=logname, level=logging.DEBUG, format = FORMAT)
    logger.info('Migration Started.......')
    
    opts = parseOptions()
    if (not opts):
        exit(1)
    #pushUrl = opts.push_url
    obsList = getUnprocessedObs(isGleam = gleam) #opts.obs_list.split(',')
    host = opts.push_host
    port = int(opts.port)
    
    
    client = ngamsPClient.ngamsPClient(host, port, timeOut = NGAMS_SOCK_TIMEOUT_DEF)
    
    toUrl = getPushURL("%s:%d" % (host, port), gateway = None)
    stageUrl = 'http://%s/ASYNCLISTRETRIEVE' % opts.data_mover
    statusUrl = 'http://%s/ASYNCLISTRETRIEVE?cmd=status&uuid=' % opts.data_mover
    
    for obsNum in obsList:
        logger.info("First check if files at Cortex at all")
        if (not hasFilesInCortex(obsNum)):
            markAsncSentStatus(obsNum, isGleam = gleam)
            continue
        logger.info("Checking observation: %s" % obsNum)
        files = getFileIdsByObsNum(obsNum)
        deliverFileIds = []
        for fileId in files:
            # first check if MIT has it or not
            if (not hasPawseyGotIt(client, fileId)):
                deliverFileIds.append(fileId)
                logger.debug('Add file %s for obsNum: %s' % (fileId, obsNum))
            else:
                logger.info("\tFile %s is already at Pawsey. Skip it." % fileId)
        
        if (len(deliverFileIds) == 0):
            logger.info('All files for obsnum %s have already in Pawsey. Move to the next observation' % obsNum)
            markAsncSentStatus(obsNum, isGleam = gleam)
            continue  
        myReq = AsyncListRetrieveRequest(deliverFileIds, toUrl)
        strReq = pickle.dumps(myReq)
        sessionId = None
        try:
            logger.info("Sending async retrieve request to the data mover %s" % opts.data_mover)
            strRes = urllib2.urlopen(stageUrl, data = strReq, timeout = NGAMS_SOCK_TIMEOUT_DEF).read() 
            myRes = pickle.loads(strRes)
            if (myRes):
                errCode = myRes.errorcode
                if (errCode):
                    logger.error('Fail to send async retrieve for obs %s, errorCode = %d' % (obsNum, errCode))
                    continue
                else:
                    sessionId = myRes.session_uuid
                    logger.info('session uuid = %s' % sessionId)
            else:
                logger.error('Response is None when async staging files for obsNum %s' % obsNum)
                continue
        except (UnpicklingError, socket.timeout) as uerr:
            logger.error("Something wrong while sending async retrieve request for obsNum %s, %s" % (obsNum, str(uerr)))
            continue
        markAsncSentStatus(obsNum, isGleam = gleam)
        waitForNextObs(obsNum, statusUrl, sessionId, 600, isgleam = gleam) # maximum wait time 10 min, then go for the next observation

if __name__ == "__main__":
    main()