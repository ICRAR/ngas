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
import psycopg2
import cPickle as pickle

from Queue import Queue, Empty
from ngamsMWAAsyncProtocol import *

g_db_conn = None # MWA metadata database connection
f_db_conn = None # Fornax NGAS database connection

io_ex_ip = {'io1':'202.8.39.136', 'io2':'202.8.39.137'}  # the two Copy Nodes external ip
ST_INTVL_STAGE = 5 # interval in seconds between staging file checks
ST_BATCH_SIZE = 5 # minimum number of files in each stage request
ST_RETRY_LIM = 3 # number of times min_number can be used, if exceeds, stage files anyway
ST_CORTEX_URL = 'http://cortex.ivec.org:7777'
ST_FORNAX_PUSH_URL = io_ex_ip['io1']

stage_queue = Queue()
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
        g_db_conn = psycopg2.connect(database = db_name, user= db_user, 
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
    fdb_host = 'localhost'   
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
    def __init__(self, svrUrl, filePath):
        """
        Constructor
        
        svrUrl:      host/ip and port
        filePath:    local path on the Fornax compute node with svrUrl
        """
        self._svrUrl = svrUrl
        self._filePath = filePath

def getFileLocations(fileId):
    """
    Given a file id
    Return: a list of FileLocation's in the cluster
    """
    if (not fileId or len(fileId) == 0):
        return None
    conn = getFornaxDBConn()
    # hardcoded based on PostGreSQL and 
    # assumes multipleSrv options is turned on when launching these ngas servers
    sqlQuery = "SELECT a.host_id, a.mount_point || '/' || b.file_name FROM " +\
               "ngas_disks a, ngas_files b where a.disk_id = b.disk_id AND b.file_id = '%s'" % fileId
    res = executeQuery(conn, sqlQuery)
    ret = []
    for re in res:
        path_file = os.path.split(re[1])
        if (len(path_file) < 1):
            continue
        floc = FileLocation(re[0], path_file[0])
        ret.append(floc)
    
    return ret

def testGetFileLocations():
    ret = getFileLocations('1365971011-6.data')
    print 'server_url = %s, file_path = %s' % (ret[0]._svrUrl, ret[0]._filePath)


def stageFile(fileId, corrTask):
    """
    fileIds:    file that needs to be staged from Cortex
    corrTask:   the CorrTask instance that invokes this function
                this corrTask will be used for calling back 
    """
    stage_sem.acquire()
    try:
        if (stage_dic.has_key(fileId)):
            # this file has been requested for staging (but not yet staged)
            list = stage_dic[fileId]
            list.append(corrTask)
        else:
            stage_dic[fileId] = [corrTask]
            stage_queue.put(fileId)
    finally:
        stage_sem.release()

def scheduleForStaging(num_repeats = 0):
    print 'Scheduling staging...'
    if (len(stage_dic.keys()) < ST_BATCH_SIZE and num_repeats < ST_RETRY_LIM):
        return 1
    list = []
    while (1):
        fileId = None
        try:
            fileId = stage_queue.get_nowait()
            list.append(fileId)
        except Empty, e:
            break
    
    myReq = AsyncListRetrieveRequest(list, ST_FORNAX_PUSH_URL)
    strReq = pickle.dumps(myReq)
    strRes = urllib.urlopen(ST_CORTEX_URL, strReq).read()
    myRes = pickle.loads(strRes)
    
    # TODO - handle exceptions (error code later)   
    return 0    
    

def fileIngested(fileId):
    """
    This function is called by the Web server to notify
    jobs which are waiting for this file to be ingested
    
    fileId:    The file that has just been ingested in Fornax
    """
    # to notify all CorrTasks that are waiting for this file
    # reset the "Event" so CorrTasks can all continue
    pass


def closeConn(conn):
    if (conn):
        if (not conn.closed):
            conn.close()
        del conn


if __name__=="__main__":
    testGetFileIds()
    testGetFileLocations()
    closeConn(g_db_conn)
    closeConn(f_db_conn)
    
    
