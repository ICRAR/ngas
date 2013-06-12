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
# chen.wu@icrar.org  26/May/2013        Created

"""
This module provides MWA_RTS MRTask with functions for
metadata query, data movement, and HTTP-based communication
during job task execution and scheduling
"""

import os, threading, urllib, traceback, commands
from random import choice, shuffle
import psycopg2
import cPickle as pickle

#from Queue import Queue, Empty
from ngamsMWAAsyncProtocol import *
import ngamsJobMAN

g_db_conn = None # MWA metadata database connection
f_db_conn = None # Fornax NGAS database connection

io_ex_ip = {'io1':'202.8.39.136:7777', 'io2':'202.8.39.137:7777'}  # the two Copy Nodes external ip

stage_queue = []
stage_dic = {} # key - fileId, value - a list of CorrTasks
stage_sem = threading.Semaphore(1)

#ST_INTVL_STAGE = 5 # interval in seconds between staging file checks
#ST_BATCH_SIZE = 5 # minimum number of files in each stage request
#ST_RETRY_LIM = 3 # number of times min_number can be used, if exceeds, stage files anyway

def execCmd(cmd, failonerror = True):
    re = commands.getstatusoutput(cmd)
    if (failonerror and re[0] != 0):
        raise Exception('Fail to execute command: "%s". Exception: %s' % (cmd, re[1]))
    return re

def pingHost(url, timeout = 5):
    """
    To check if a host is successfully running
    
    Return:
    0        Success
    1        Failure
    """
    cmd = 'curl %s --connect-timeout %d' % (url, timeout)
    try:
        return execCmd(cmd)[0]
    except Exception, err:
        return 1

def getMWADBConn():
    global g_db_conn
    if (g_db_conn and (not g_db_conn.closed)):
        return g_db_conn
    
    config = ngamsJobMAN.getConfig()
    confSec = 'MWA DB'
    db_name = config.get(confSec, 'db')
    db_user = config.get(confSec, 'user')
    db_passwd = config.get(confSec, 'password')
    db_host = config.get(confSec, 'host')
    try:        
        """
        g_db_conn = psycopg2.connect(database = db_name, user = db_user, 
                            password = db_passwd.decode('base64'), 
                            host = db_host)
        """
        g_db_conn = psycopg2.connect(database = 'mwa', user = 'mwa', 
                            password = 'Qm93VGll\n'.decode('base64'), 
                            host = 'ngas01.ivec.org')
        return g_db_conn 
    except Exception, e:
        errStr = 'Cannot create MWA DB Connection: %s' % str(e)
        raise Exception, errStr

def getFornaxDBConn():
    global f_db_conn    
    if (f_db_conn and (not f_db_conn.closed)):
        return f_db_conn
    
    config = ngamsJobMAN.getConfig()
    confSec = 'NGAS DB'
    fdb_name = config.get(confSec, 'db')
    fdb_user = config.get(confSec, 'user')
    fdb_passwd = config.get(confSec, 'password')
    fdb_host = config.get(confSec, 'host')
    try:
        f_db_conn = psycopg2.connect(database = fdb_name, user= fdb_user, 
                            password = fdb_passwd.decode('base64'), 
                            host = fdb_host)
        return f_db_conn
    except Exception, e:
        errStr = 'Cannot create Fornax DB Connection: %s' % str(e)
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
    num_subband:    number of sub-bands, used to check if the num_corr is the same
    
    Return:     A dictionary, key - correlator id (starting from 1, int), value - a list of file ids belong to that correlator
    """
    sqlQuery = "SELECT filename FROM data_files WHERE observation_num = '%s' ORDER BY SUBSTRING(filename, 27);" % str(obs_num)
    conn = getMWADBConn()
    res = executeQuery(conn, sqlQuery)
    retDic = {}
    for re in res:
        fileId = re[0]
        corrId = int(fileId.split('_')[2][-2:])
        if (retDic.has_key(corrId)):
            retDic[corrId].append(fileId)
        else:
            retDic[corrId] = [fileId]
    return retDic
            
def testGetFileIds():
    print getFileIdsByObsNum('1052803816')[22][0]
    #print getFileIdsByObsNum('1052803816')[19][1] # this will raise key error
    
class FileLocation:
    """
    A class representing the location information on NGAS servers
    Each Correlator has a at least one FileLocation 
    """
    def __init__(self, svrHost, filePath, fileId = None):
        """
        Constructor
        
        svrHost:      host/ip and port
        filePath:    local path on the Fornax compute node with svrUrl
        fileId:      the id of this file whose location is being queried
        """
        self._svrHost = svrHost
        self._filePath = filePath
        self._ingestRate = 0
        if (fileId):
            self._fileId = fileId

def getFileLocations(fileId):
    """
    Given a SINGLE file id
    Return: a list of FileLocation's in the cluster
    """
    if (not fileId or len(fileId) == 0):
        return None
    conn = getFornaxDBConn()
    # hardcoded based on PostGreSQL and 
    # assumes multipleSrv options is turned on when launching these ngas servers
    sqlQuery = "SELECT a.host_id, a.mount_point || '/' || b.file_name FROM " +\
               "ngas_disks a, ngas_files b, ngas_hosts c where a.disk_id = b.disk_id AND b.file_id = '%s' " % fileId +\
               "AND a.host_id = c.host_id AND c.srv_state = 'ONLINE'"
    res = executeQuery(conn, sqlQuery)
    ret = []
    for re in res:
        #path_file = os.path.split(re[1])
        #if (len(path_file) < 1):
            #continue
        floc = FileLocation(re[0], re[1], fileId)
        ret.append(floc)
    
    return ret

def testGetFileLocations():
    #ret = getFileLocations('1365971011-6.data')
    file = '1053182656_20130521144711_gpubox08_03.fits'
    ret = getFileLocations(file)
    if (ret and len(ret) > 0):
        print 'server_url = %s, file_path = %s' % (ret[0]._svrHost, ret[0]._filePath)
    else:
        print 'Could not find locations for file %s' % file

def getBestHost(fileIds):
    """
    This function tries to find out which host is most ideal to run a task 
    if that task requires all files in fileIds to reside on that host
    e.g. A RTS task requires all files belong to a correlator on a single host
    
    Given a list of file ids, 
    Return:    A dict - key: file_id, value - FileLocation
               This dict is the "best" host that hosts
               MOST of the files in the fileId list. For files that 
               are not hosted on this host, they do not have key entries
               in this dict, which means they should either 
               (1) be staged from a remote server (i.e. cortex) to this host or
               (2) be staged from other hosts to this host
    """
    if (not fileIds or len(fileIds) == 0):
        return None
    conn = getFornaxDBConn()
    file_list = "'%s'" % fileIds[0]
    if (len(fileIds) > 1):
        for fid in fileIds[1:]:
            file_list += ", '%s'" % fid
        
    sqlQuery = "SELECT a.host_id, a.mount_point || '/' || b.file_name, b.file_id FROM " +\
               "ngas_disks a, ngas_files b, ngas_hosts c where a.disk_id = b.disk_id AND b.file_id in (%s) " % file_list +\
               "AND a.host_id = c.host_id AND c.srv_state = 'ONLINE'"
    
    res = executeQuery(conn, sqlQuery)
    
    if (len(res) == 0):
        return {}
    
    dictHosts = {} # key - host_id, # value - a list of FileLocations 
        
    for re in res:
        #path_file = os.path.split(re[1])
        #if (len(path_file) < 1):
            #continue
        floc = FileLocation(re[0], re[1], re[2]) # the path also includes the filename
        if (dictHosts.has_key(re[0])):
            dictHosts[re[0]].append(floc)
        else:
            dictHosts[re[0]] = [floc]
    
    candidateList = []
    for (hostId, floclist) in dictHosts.items():
        #for each host, count up unique file ids
        dictFileIds = {} # potential return value of this function
        for fl in floclist:
            if dictFileIds.has_key(fl._fileId):
                continue
            else:
                dictFileIds[fl._fileId] = fl
        candidateList.append(dictFileIds)
    
    candidateList.sort(key=_sortFunc)
    cc = -1
    for candict in candidateList:
        cc += 1
        if (len(candict.keys()) == 0):
            break
        else:
            canHost = candict.values()[0]._svrHost
            if (pingHost(canHost)):
                continue
            else:
                break
    return candidateList[cc]

def _sortFunc(dic):
    return -1 * len(dic.keys())
    
def testGetBestHost():
    # this test data works when 
    #                             fdb_host = '192.102.251.250'
    fileList = ['1049201112_20130405124558_gpubox16_01.fits', '1049201112_20130405124559_gpubox23_01.fits', 
                '1053182656_20130521144710_gpubox03_03.fits', '1028104360__gpubox01.rts.mwa128t.org.vis', '1053182656_20130521144711_gpubox06_03.fits']
    ret = getBestHost(fileList)
    for (fid, floc) in ret.items():
        print 'file_id = %s, host = %s, path = %s' % (fid, floc._svrHost, floc._filePath)

def getNextOnlineHost():
    """
    Return:    host:port (string, e.g. 192.168.1.1:7777)
    """
    conn = getFornaxDBConn()
    sqlQuery = "select host_id from ngas_hosts where srv_state = 'ONLINE' AND host_id <> '%s'" % getClusterGateway()
    res = executeQuery(conn, sqlQuery)
    if (len(res) == 0):
        return None
    shuffle(res)    
    for host in res:
        if (not pingHost(host[0])):
            return host[0]    
    return None

def testGetNextOnlineHostUrl():
    print getNextOnlineHost()

"""
class StageRequest():
    def __init__(self, fileId, corrTask, toHost, frmHost = None):
        self._fileId = fileId
        self._toHost = toHost
        self._corrTasks = [corrTask] # keep a reference for calling back 
        if (frmHost):
            self._frmHost = frmHost
    
    def merge(self, thatSR):
       
     #    Merge two StateRequests if they both ask for the same file from the same EXTERNAL location
     #   Return:    1 - merge did occur, the newly merged SR is self
      #             0 - merge condition did not meet
       
        if (self._frmHost != thatSR._frmHost or thatSR._fileId != self._fileId):
            return 0
        if (self._frmHost == None): # both external staging
            self._corrTasks += thatSR._corrTasks
            return 1
        else: # both internal staging
            if (self._toHost == thatSR._toHost):
                self._corrTasks += thatSR._corrTasks
                return 1
            else:
                return 0
"""

def stageFile(fileIds, corrTask, toHost, frmHost = None):
    """
    fileIds:    a list of files that need to be staged from external archive
    corrTask:   the CorrTask instance that invokes this function
                this corrTask will be used for calling back
    frmHost:    host that file is staged from. If none, from a well-known outside host, i.e. Cortex
    toHost:      host that file is staged to
    """      
    staged_by_others = 0
    deliverFileIds = [] # true file ids that will be staged
    stage_sem.acquire()
    try:
        for fileId in fileIds:
            #sr = StageRequest(fileId, corrTask, toHost, frmHost)
            skey = '%s___%s' % (fileId, toHost)
            if (stage_dic.has_key(skey)):
                # this file has already been requested for staging to the same host (could be by another job)
                list = stage_dic[skey]
                list.append(corrTask)
                staged_by_others += 1
            else:
                stage_dic[skey] = [corrTask]
                deliverFileIds.append(fileId)
                #stage_queue.append(fileId)
    finally:
        stage_sem.release()
    
    if (0 == len(deliverFileIds)): # the whole list has already been requested to stage by others
        return 0
    
    if (frmHost):
        toUrl = getPushURL(toHost)
    else:
        toUrl = getPushURL(toHost, getClusterGateway())
        
    myReq = AsyncListRetrieveRequest(deliverFileIds, toUrl)
    try:
        strReq = pickle.dumps(myReq)
        strRes = urllib.urlopen('%s/ASYNCLISTRETRIEVE' % getExternalArchiveURL(), strReq).read()
        myRes = pickle.loads(strRes)
        return myRes.errorcode
    except Exception, err:
        # log err
        print(str(err) + traceback.format_exc())
        return 500
    
        
def getExternalArchiveURL(fileId = None):
    """
    Obtain the url of the external archive, which
    could be different based on the fileId. (e.g. EOR data all from Cortex, GEG from ICRAR, etc.)
    This function behaves like a URI resolution service
    """
    # just a dummy implementation for now:
    config = ngamsJobMAN.getConfig()    
    return 'http://%s' % config.get('Archive Servers', 'LTA')

def getClusterGateway():
    config = ngamsJobMAN.getConfig()
    return config.get('Archive Servers', 'ClusterGateway')

def getPushURL(hostId, gateway = None):
    """
    Construct the push url based on the hostId in the cluster
    
    hostId:    the host (e.g. 192.168.1.1:7777) that will receive the file
    
    gateway:   1 - (Default) his host is behind a gateway (firewall), 0 - otherwise
    """
    if (gateway):
        return 'http://%s/PARCHIVE?nexturl=http://%s/QAPLUS' % (gateway, hostId)
    else:
        return 'http://%s/QAPLUS' % hostId
    

"""
def scheduleForStaging(num_repeats = 0):
    \"""
    This method is no longer useful
    \"""
    print 'Scheduling staging...'
    global stage_queue # since we will update it, need to declare as global
    
    if (len(stage_queue) == 0):
        return 0
    
    if (len(stage_queue) < ST_BATCH_SIZE and num_repeats < ST_RETRY_LIM):
        return 1
    #list = []
    \"""
    while (1):
        fileId = None
        try:
            fileId = stage_queue.get_nowait()
            list.append(fileId)
        except Empty, e:
            break
    \"""
    
    stage_sem.acquire()
    filelist = list(stage_queue)
    stage_queue = []
    stage_sem.release()
    myReq = AsyncListRetrieveRequest(filelist, ST_FORNAX_PUSH_HOST)
    strReq = pickle.dumps(myReq)
    strRes = urllib.urlopen(ST_CORTEX_URL, strReq).read()
    myRes = pickle.loads(strRes)
    
    # TODO - handle exceptions (error code later)   
    return 0  
"""      

def fileIngested(fileId, filePath, toHost, ingestRate):
    """
    This function is called by the Web server to notify
    jobs which are waiting for this file to be ingested
    
    fileId:      The file that has just been ingested in Fornax
    filePath:    The local file path on that machine
    toHost:      The host that has just ingested this file
    """
    # to notify all CorrTasks that are waiting for this file
    # reset the "Event" so CorrTasks can all continue
    skey = '%s___%s' % (fileId, toHost)
    stage_sem.acquire()
    try:
        if (stage_dic.has_key(skey)):
            corrList = stage_dic.pop(skey)
        else: 
            print 'File Ingested, but cannot find key %s' % skey
            return
    finally:
        stage_sem.release()
    for corr in corrList:
        corr.fileIngested(fileId, filePath, ingestRate)

def reportHostDown(fileId, toHost):
    """
    notfiy failed staging file and its destination
    due to down host
    so that following requests will have a chance to retry
    other hosts
    """

    skey = '%s___%s' % (fileId, toHost)
    stage_sem.acquire()
    try:
        if (stage_dic.has_key(skey)):
            corrList = stage_dic.pop(skey) # stop others from using this key-entry
            # but what about other files on this host?
        else: 
            print 'Report host down, but cannot find key %s' % skey
            return
    finally:
        stage_sem.release()
    for corr in corrList:
        corr.reportHostDown(fileId, toHost)

def closeConn(conn):
    if (conn):
        if (not conn.closed):
            conn.close()
        del conn


if __name__=="__main__":
    #testGetFileIds()
    #testGetFileLocations()
    #testGetBestHost()
    testGetNextOnlineHostUrl()
    #print pingHost('http://cortex.ivec.org:7799/STATUS')
    #print pingHost('http://fornax-io1.ivec.org:7777/STATUS')
    closeConn(g_db_conn)
    closeConn(f_db_conn)
    
    
