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

import psycopg2

db_name = 'mwa'
db_user = 'mwa'
db_passwd = 'Qm93VGll\n'
db_host = 'ngas01.ivec.org'
g_db_conn = None

def getMWADBConn():
    global g_db_conn
    if (g_db_conn and (not g_db_conn.closed)):
        return g_db_conn
    try:        
        g_db_conn = psycopg2.connect(database = db_name, user= db_user, 
                            password = db_passwd.decode('base64'), 
                            host = db_host)
        return g_db_conn 
    except Exception, e:
        errStr = 'Cannot create MWA DB Connection: %s' % str(e)
        raise Exception, errStr

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
    try:
        cur = conn.cursor()
        cur.execute(sqlQuery)
        res = cur.fetchall()
    finally:
        if (cur):
            del cur   
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
        self.__svrUrl = svrUrl
        self.__filePath = filePath

def getFileLocation(fileId):
    """
    Return: an instance of FileLocation
    """
    pass

def stageFile(fileIds, corrTask):
    """
    fileIds:    a list of fileIds that needs to be staged from Cortex
    corrTask:   the CorrTask instance that invokes this function
                this corrTask will be used for calling back 
    """
    pass

def fileIngested(fileId):
    """
    fileId:    The file that has just been ingested in Fornax
    """
    # to notify all CorrTasks that are waiting for this file
    pass



if __name__=="__main__":
    testGetFileIds()
    if (not g_db_conn.closed):
        g_db_conn.close()
        del g_db_conn
    
