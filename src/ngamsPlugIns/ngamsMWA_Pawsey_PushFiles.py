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
from optparse import OptionParser

import psycopg2
import os.path
import urllib2, time
import cPickle as pickle
from cPickle import UnpicklingError
import socket

from ngams import *
import ngamsPlugInApi
import ngamsPClient
import ngamsMWACortexTapeApi

from ngamsMWAAsyncProtocol import *

mime_type = 'application/octet-stream'
proxy_archive = 'storage01.icrar.org:7777'

g_db_conn = None # MWA metadata database connection
l_db_conn = None

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
    mwa_conn = getMWADBConn()
    res = executeQuery(mwa_conn, sqlQuery)
    
    retList = []
    for re in res:
        fileId = re[0]
        retList.append(fileId)
    return retList

def getUnprocessedObs():
    """
    Get all observation numbers that have not been processed (to be delivered
    """
    sqlQuery = "SELECT obs_id FROM ngas_migration WHERE async_sent = 0 ORDER BY obs_id DESC"
    lta_conn = getLTADBConn()
    res = executeQuery(lta_conn, sqlQuery)
    
    retList = []
    for re in res:
        obsId = re[0]
        retList.append(obsId)
    return retList

def markObsDeliveredStatus(obsId, status = 1):
    """
    mark an observation as delivered
    """
    sqlQuery = "UPDATE ngas_migration SET delivered = %d WHERE obs_id = '%s'" % (status, obsId)
    lta_conn = getLTADBConn()
    updateQuery(lta_conn, sqlQuery)
    
def markAsncSentStatus(obsId, status = 1):
    """
    mark an observation as "async retrieve request sent"
    
    """
    sqlQuery = "UPDATE ngas_migration SET async_sent = %d WHERE obs_id = '%s'" % (status, obsId)
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
        print(errMsg)
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

def waitForNextObs(obsNum, statusUrl, sessionId, maxWaitTime, checkInterval = 60):
    max_time = 0
    while (max_time <= maxWaitTime):
        time.sleep(checkInterval)
        max_time += checkInterval
        
        try:
            strRes = urllib2.urlopen(statusUrl + sessionId).read()
            myRes = pickle.loads(strRes)
            if (0 == myRes.number_files_to_be_delivered):
                # modify database
                markObsDeliveredStatus(obsNum)
                break
            elif (myRes.errorcode):
                markObsDeliveredStatus(obsNum, -1)
                break
        except (UnpicklingError, socket.timeout) as uerr:
            print "Something wrong while getting status for obsNum %s, %s" % (obsNum, str(uerr))
            continue
        

def main():
    opts = parseOptions()
    if (not opts):
        exit(1)
    #pushUrl = opts.push_url
    obsList = getUnprocessedObs() #opts.obs_list.split(',')
    host = opts.push_host
    port = int(opts.port)
    
    client = ngamsPClient.ngamsPClient(host, port, timeOut = NGAMS_SOCK_TIMEOUT_DEF)
    
    toUrl = getPushURL("%s:%d" % (host, port), gateway = None)
    stageUrl = 'http://%s/ASYNCLISTRETRIEVE' % opts.data_mover
    statusUrl = 'http://%s/ASYNCLISTRETRIEVE?cmd=status&uuid=' % opts.data_mover
    
    for obsNum in obsList:
        print "Checking observation: %s" % obsNum
        files = getFileIdsByObsNum(obsNum)
        deliverFileIds = []
        for fileId in files:
            # first check if MIT has it or not
            if (not hasPawseyGotIt(client, fileId)):
                deliverFileIds.append(fileId)
            else:
                print "\tFile %s is already at Pawsey. Skip it." % fileId
            
        myReq = AsyncListRetrieveRequest(deliverFileIds, toUrl)
        strReq = pickle.dumps(myReq)
        sessionId = None
        try:
            print "Sending async retrieve request to the data mover %s" % opts.data_mover
            strRes = urllib2.urlopen(stageUrl, data = strReq, timeout = NGAMS_SOCK_TIMEOUT_DEF).read() 
            myRes = pickle.loads(strRes)
            if (myRes):
                errCode = myRes.errorcode
                if (errCode):
                    print 'Fail to send async retrieve for obs %s, errorCode = %d' % (obsNum, errCode)
                    continue
                else:
                    sessionId = myRes.session_uuid
                    print 'session uuid = %s' % sessionId
            else:
                print 'Response is None when async staging files for obsNum %s' % obsNum
                continue
        except (UnpicklingError, socket.timeout) as uerr:
            print "Something wrong while sending async retrieve request for obsNum %s, %s" % (obsNum, str(uerr))
            continue
        markAsncSentStatus(obsNum)
        waitForNextObs(obsNum, statusUrl, sessionId, 900) # maximum wait time 15 min, then go for the next observation

if __name__ == "__main__":
    main()