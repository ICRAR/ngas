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
# chen.wu@icrar.org  03/Aug/2013        Created
"""
This module pushes missing files to MIT in a semi-automated fashion
"""

import cPickle as pickle
from cPickle import UnpicklingError
from optparse import OptionParser
import socket, base64
import urllib2

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import NGAMS_STATUS_CMD, NGAMS_FAILURE, NGAMS_SOCK_TIMEOUT_DEF
from ngamsPClient import ngamsPClient
from ngamsPlugIns.ngamsMWAAsyncProtocol import AsyncListRetrieveRequest
import psycopg2


mime_type = 'application/octet-stream'
#proxy_archive = 'storage01.icrar.org:7777'
proxy_archive = None
#lta_db_host = '192.102.251.250'
lta_db_host = 'mwa-pawsey-db01.pawsey.ivec.org'
#lta_file_version = 1
lta_file_version = 2


g_db_conn = None # MWA metadata database connection

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

    try:
        l_db_conn = psycopg2.connect(database = 'ngas', user= 'ngas',
                            password = 'bmdhcyRkYmE=\n'.decode('base64'),
                            host = lta_db_host)
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

def getFileFullPath(fileId):
    """
    Given a file id, return its full path on Cortex
    """
    lta_conn = getLTADBConn()
    sqlQuery = "SELECT a.mount_point || '/' || b.file_name FROM ngas_disks a, ngas_files b where a.disk_id = b.disk_id AND b.file_version = %d AND b.file_id = '%s'" % (lta_file_version, fileId)
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
    parser.add_option("-o", "--obslist", dest = "obs_list", help = "a list of observation numbers, separated by comma")
    #parser.add_option("-u", "--pushurl", dest = "push_url", help = "the url to which we push files")
    parser.add_option("-s", "--host", dest = "push_host", help = "the host that will receive the file")
    parser.add_option("-p", "--port", dest = "port", help = "the port of this host")
    parser.add_option("-m", "--dm", dest = "data_mover", help = "the url of the data mover")


    (options, args) = parser.parse_args()
    if (None == options.obs_list or None == options.push_host or None == options.port or None == options.data_mover):
        parser.print_help()
        print 'Missing parameters'
        return None
    return options

def hasMITGotIt(client, fileId):
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

def stageFile(filename):
    cmd = "stage -w " + filename
    print "File %s is on tape, staging it now..." % filename
    ngamsPlugInApi.execCmd(cmd, -1) #stage it back to disk cache
    print "File " + filename + " staging completed."

def archiveFile(filename, client):
    try:
        stat = client.pushFile(filename, mime_type, cmd = 'QARCHIVE')
    except Exception as e:
        print "Exception '%s' occurred while archiving file %s" % (str(e), filename)
    msg = stat.getMessage().split()[0]
    if (msg != 'Successfully'):
        print "Exception '%s' occurred while archiving file %s" % (stat.getMessage(), filename)

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

def main():
    opts = parseOptions()
    if (not opts):
        exit(1)
    #pushUrl = opts.push_url
    obsList = opts.obs_list.split(',')
    host = opts.push_host
    port = int(opts.port)

    client = ngamsPClient.ngamsPClient(host, port, timeOut = NGAMS_SOCK_TIMEOUT_DEF)

    toUrl = getPushURL("%s:%d" % (host, port), gateway = proxy_archive)
    stageUrl = 'http://%s/ASYNCLISTRETRIEVE' % opts.data_mover

    for obsNum in obsList:
        print "Checking observation: %s" % obsNum
        files = getFileIdsByObsNum(obsNum)
        deliverFileIds = []
        for fileId in files:
            # first check if MIT has it or not
            if (not hasMITGotIt(client, fileId)):
                deliverFileIds.append(fileId)
                """
                fileName = getFileFullPath(fileId)
                if (not os.path.exists(fileName)):
                    print "\tFile %s does not exist" % fileName
                    continue
                onTape = ngamsMWACortexTapeApi.isFileOnTape(fileName)
                if (1 == onTape):
                    stageFile(fileName)
                print "\tPushing file %s to MIT" % fileId
                archiveFile(fileName, client)
                """
            else:
                print "\tFile %s is already at MIT. Skip it." % fileId

        myReq = AsyncListRetrieveRequest(deliverFileIds, toUrl)
        strReq = pickle.dumps(myReq)
        try:
            print "Sending async retrieve request to the data mover %s" % opts.data_mover
            request = urllib2.Request(stageUrl)
            base64string = base64.encodestring('ngasmgr:ngas$dba').replace('\n', '')
            request.add_header("Authorization", "Basic %s" % base64string)
            strRes = urllib2.urlopen(request, data = strReq, timeout = NGAMS_SOCK_TIMEOUT_DEF).read()
            myRes = pickle.loads(strRes)
            #strRes = urllib2.urlopen(stageUrl, data = strReq, timeout = NGAMS_SOCK_TIMEOUT_DEF).read()
            #myRes = pickle.loads(strRes)
            if (myRes):
                print myRes.errorcode
            else:
                print 'Response is None when async staging files for obsNum %s' % obsNum
        except (UnpicklingError, socket.timeout) as uerr:
            print "Something wrong while sending async retrieve request for obsNum %s, %s" % (obsNum, str(uerr))

if __name__ == "__main__":
    main()