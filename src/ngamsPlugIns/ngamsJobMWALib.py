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

import os, threading, urllib
from random import choice
import psycopg2
import cPickle as pickle

#from Queue import Queue, Empty
from ngamsMWAAsyncProtocol import *

g_db_conn = None # MWA metadata database connection
f_db_conn = None # Fornax NGAS database connection

io_ex_ip = {'io1':'202.8.39.136:7777', 'io2':'202.8.39.137:7777'}  # the two Copy Nodes external ip
ST_INTVL_STAGE = 5 # interval in seconds between staging file checks
ST_BATCH_SIZE = 5 # minimum number of files in each stage request
ST_RETRY_LIM = 3 # number of times min_number can be used, if exceeds, stage files anyway
ST_CORTEX_URL = 'http://cortex.ivec.org:7777'
ST_FORNAX_PUSH_HOST = io_ex_ip['io1']

stage_queue = []
stage_dic = {} # key - fileId, value - a list of CorrTasks
stage_sem = threading.Semaphore(1)

def getMWADBConn():
    global g_db_conn
    if (g_db_conn and (not g_db_conn.closed)):
        return g_db_conn
    
    db_name = 'mwa'
    db_user = 'mwa'
    db_passwd = 'Qm93VGll\n'
    db_host = 'ngas01.ivec.org'    
    try:        
        g_db_conn = psycopg2.connect(database = db_name, user = db_user, 
                            password = db_passwd.decode('base64'), 
                            host = db_host)
        return g_db_conn 
    except Exception, e:
        errStr = 'Cannot create MWA DB Connection: %s' % str(e)
        raise Exception, errStr

def getFornaxDBConn():
    global f_db_conn    
    if (f_db_conn and (not f_db_conn.closed)):
        return f_db_conn
    
    fdb_name = 'ngas'
    fdb_user = 'ngas'
    fdb_passwd = 'bmdhcyRkYmE=\n'
    #fdb_host = 'fornaxspare'
    #fdb_host = 'localhost'   
    fdb_host = '192.102.251.250' #cortex testing
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
    def __init__(self, svrUrl, filePath, fileId = None):
        """
        Constructor
        
        svrUrl:      host/ip and port
        filePath:    local path on the Fornax compute node with svrUrl
        fileId:      the id of this file whose location is being queried
        """
        self._svrUrl = svrUrl
        self._filePath = filePath
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
        path_file = os.path.split(re[1])
        if (len(path_file) < 1):
            continue
        floc = FileLocation(re[0], path_file[0], fileId)
        ret.append(floc)
    
    return ret

def testGetFileLocations():
    #ret = getFileLocations('1365971011-6.data')
    ret = getFileLocations('1053182656_20130521144711_gpubox08_03.fits')
    print 'server_url = %s, file_path = %s' % (ret[0]._svrUrl, ret[0]._filePath)

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
    dictHosts = {} # key - host_id, # value - a list of FileLocations 
        
    for re in res:
        path_file = os.path.split(re[1])
        if (len(path_file) < 1):
            continue
        floc = FileLocation(re[0], path_file[0], re[2])
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
    
    max_index = 0
    max_count = 0
    cur_index = 0
    # find the Dict that has most key-value pairs
    for cand in candidateList:
        cur_count = len(cand.keys())
        if (cur_count > max_count):
            max_count = cur_count 
            max_index = cur_index
        cur_index += 1
    
    return candidateList[max_index]
    
def testGetFileListLocations():
    # this test data works when 
    #                             fdb_host = '192.102.251.250'
    fileList = ['1049201112_20130405124558_gpubox16_01.fits', '1049201112_20130405124559_gpubox23_01.fits', 
                '1053182656_20130521144710_gpubox03_03.fits', '1028104360__gpubox01.rts.mwa128t.org.vis', '1053182656_20130521144711_gpubox06_03.fits']
    ret = getBestHost(fileList)
    for (fid, floc) in ret.items():
        print 'file_id = %s, host = %s, path = %s' % (fid, floc._svrUrl, floc._filePath)

def getNextOnlineHostUrl():
    """
    Return:    host:port (string, e.g. 192.168.1.1:7777)
    """
    conn = getFornaxDBConn()
    sqlQuery = "select host_id from ngas_hosts where srv_state = 'ONLINE'"
    res = executeQuery(conn, sqlQuery)
    return choice(res)[0]

def testGetNextOnlineHostUrl():
    print getNextOnlineHostUrl()

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
    stage_sem.acquire()
    try:
        for fileId in fileIds:
            #sr = StageRequest(fileId, corrTask, toHost, frmHost)
            skey = '%s:%s' % (fileId, toHost)
            if (stage_dic.has_key(skey)):
                # this file has already been requested for staging to the same host (could be by another job)
                list = stage_dic[skey]
                list.append(corrTask)
                staged_by_others += 1
            else:
                stage_dic[skey] = [corrTask]
                #stage_queue.append(fileId)
    finally:
        stage_sem.release()
    
    if (staged_by_others == len(fileIds)): # the whole list has already been requested to stage by others
        return 0
    
    if (frmHost):
        toUrl = getPushURL(toHost)
    else:
        toUrl = getPushURL(toHost, getClusterGateway())
        
    myReq = AsyncListRetrieveRequest(fileIds, toUrl)
    try:
        strReq = pickle.dumps(myReq)
        strRes = urllib.urlopen(getExternalArchiveURL(), strReq).read()
        myRes = pickle.loads(strRes)
        return myRes.errorcode
    except Exception, err:
        # log err
        return 500
    
        
def getExternalArchiveURL(fileId):
    """
    Obtain the url of the external archive, which
    could be different based on the fileId. (e.g. EOR data all from Cortex, GEG from ICRAR, etc.)
    This function behaves like a URI resolution service
    """
    # just a dummy implementation for now
    return ST_CORTEX_URL

def getClusterGateway():
    #TODO - use configuration file    
    return ST_FORNAX_PUSH_HOST

def getPushURL(hostId, gateway = None):
    """
    Construct the push url based on the hostId in the cluster
    
    hostId:    the host (e.g. 192.168.1.1:7777) that will receive the file
    
    gateway:   1 - (Default) his host is behind a gateway (firewall), 0 - otherwise
    """
    if (gateway):
        return 'http://%s/PARCHIVE?nexturl=http://%s/QARCHIVE' % (gateway, hostId)
    else:
        return 'http://%s/QARCHIVE' % hostId
    

def scheduleForStaging(num_repeats = 0):
    print 'Scheduling staging...'
    global stage_queue # since we will update it, need to declare as global
    
    if (len(stage_queue) == 0):
        return 0
    
    if (len(stage_queue) < ST_BATCH_SIZE and num_repeats < ST_RETRY_LIM):
        return 1
    #list = []
    """
    while (1):
        fileId = None
        try:
            fileId = stage_queue.get_nowait()
            list.append(fileId)
        except Empty, e:
            break
    """
    
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

def fileIngested(fileId, filePath, toHost):
    """
    This function is called by the Web server to notify
    jobs which are waiting for this file to be ingested
    
    fileId:      The file that has just been ingested in Fornax
    filePath:    The local file path on that machine
    toHost:      The host that has just ingested this file
    """
    # to notify all CorrTasks that are waiting for this file
    # reset the "Event" so CorrTasks can all continue
    skey = '%s:%s' % (fileId, toHost)
    stage_sem.acquire()
    try:
        if (stage_dic.has_key(skey)):
            corrList = stage_dic.pop(skey)
        else: 
            return
    finally:
        stage_sem.release()
    for corr in corrList:
        corr.fileIngested(fileId, filePath)


def closeConn(conn):
    if (conn):
        if (not conn.closed):
            conn.close()
        del conn


if __name__=="__main__":
    #testGetFileIds()
    #testGetFileLocations()
    #testGetFileListLocations()
    testGetNextOnlineHostUrl()
    closeConn(g_db_conn)
    closeConn(f_db_conn)
    
    
