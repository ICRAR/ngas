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
# "@(#) $Id: ngamsCacheControlThread.py,v 1.10 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  27/03/2008  Created
#

"""
This module contains the code for the Cache Control Thread, which is used
to manage the contents in the cache archive when running the NG/AMS Server
as a cache archive.
"""

import os, time, thread, threading, base64, cPickle, traceback
from Queue import Queue, Empty
try:
    from pysqlite2 import dbapi2 as sqlite
except:
    import sqlite3 as sqlite

from ngamsLib.ngamsCore import info, logFlush, TRACE, rmFile,\
    getMaxLogLevel, iso8601ToSecs, error, warning, notice, genLog, alert,\
    loadPlugInEntryPoint
from ngamsLib import ngamsDbCore, ngamsHighLevelLib, ngamsDbm, ngamsDiskInfo, ngamsCacheEntry, ngamsThreadGroup, ngamsLib

# An internal queue contains files that have been explicitly requested to be removed
explicitDelQueue = Queue()

def _STOP_(srvObj,
           msg):
    """
    Function used for test/debugging purposes.
    """
    info(1, "STOPPING THREAD: %s" % msg)
    logFlush()
    os.system("sync")
    time.sleep(3600)


# Various definitions used within this module.

# Name of DBMS (SQLite) used to hold the information about the contents of the
# file cache.
NGAMS_CACHE_CONTENTS_DBMS    = "CACHE_CONTENTS_DBMS"

# Fields in the NGAS Cache Table (local DBMS).
NGAMS_CACHE_DISK_ID    = 0
NGAMS_CACHE_FILE_ID    = 1
NGAMS_CACHE_FILE_VER   = 2
NGAMS_CACHE_FILENAME   = 3
NGAMS_CACHE_FILE_SIZE  = 4
NGAMS_CACHE_CACHE_DEL  = 5
NGAMS_CACHE_LAST_CHECK = 6
NGAMS_CACHE_CACHE_TIME = 7
NGAMS_CACHE_ENTRY_OBJ  = 8


# DBM used by the ARCHIVE, CLONE, REARCHIVE, REGISTER Command (and other
# commands provided in the form of Command Plug-Ins) which may add/register
# new files on this node.
NGAMS_CACHE_NEW_FILES_DBM    = "CACHE_NEW_FILES_DBM"

# DBM used to schedule files for checking by the Cache Control Plug-In.
NGAMS_CACHE_CTRL_PI_DBM       = "CACHE_CTRL_PI_DBM"
NGAMS_CACHE_CTRL_PI_DEL_DBM   = "CACHE_CTRL_PI_DEL_DBM"
NGAMS_CACHE_CTRL_PI_FILES_DBM = "CACHE_CTRL_PI_FILES_DBM"
NGAMS_CACHE_CTRL_PI_DBM_RD    = "__CACHE_CTRL_PI_DBM_RD__"
NGAMS_CACHE_CTRL_PI_DBM_WR    = "__CACHE_CTRL_PI_DBM_WR__"
NGAMS_CACHE_CTRL_PI_DBM_MAX   = 2147483648
NGAMS_CACHE_CTRL_PI_DBM_SQL   = "SQL_INFO"

# Used as exception message when the thread is stopping execution
# (deliberately).
NGAMS_CACHE_CONTROL_THR_STOP = "_STOP_CACHE_CONTROL_THREAD_"
CACHE_DEL_BIT_MASK = "00000100"
CACHE_DEL_BIT_MASK_INT = int(CACHE_DEL_BIT_MASK, 2)

CHECK_CAN_BE_DELETED = 0 #whether or not check if a file can be deleted (has it been transferred to remote sites?) during file cleaning-up


def startCacheControlThread(srvObj):
    """
    Start the Cache Control Thread.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
    T = TRACE()

    info(1, "Starting the Cache Control Thread ...")

    global CHECK_CAN_BE_DELETED

    try:
        CHECK_CAN_BE_DELETED = int(srvObj.getCfg().getVal("Caching[1].CheckCanBeDeleted"))
    except:
        CHECK_CAN_BE_DELETED = 0

    info(1, "Cache Control - CHECK_CAN_BE_DELETED = %d" % CHECK_CAN_BE_DELETED)

    args = (srvObj, None)
    srvObj._cacheControlThread = threading.Thread(None, cacheControlThread,
                                                  NGAMS_CACHE_CONTROL_THR_STOP,
                                                  args)
    srvObj._cacheControlThread.setDaemon(0)
    srvObj._cacheControlThread.start()
    srvObj.setCacheControlThreadRunning(1)
    info(1, "Cache Control Thread started")


def stopCacheControlThread(srvObj):
    """
    Stop the Cache Control Thread.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
    T = TRACE()

    if (not srvObj.getCacheControlThreadRunning()): return
    info(1, "Stopping the Cache Control Thread ...")
    if (CHECK_CAN_BE_DELETED):
        info(1, "Marking any outstanding files in the delQueue")
        markFileCanBeDeleted(srvObj) # so that files in the queue do not get lost after restart
        info(1, "Done marking")
    srvObj._cacheControlThread = None
    info(1, "Cache Control Thread stopped")


def checkStopCacheControlThread(srvObj,
                                raiseEx = True):
    """
    Used to check if the Cache Control Thread should be stopped and in case
    yes, to stop it.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
    T = TRACE(5)

    if (not srvObj.getThreadRunPermission()):
        srvObj.setCacheControlThreadRunning(0)
        info(2, "Stopping the Cache Control Service")
        if (raiseEx):
            raise Exception, NGAMS_CACHE_CONTROL_THR_STOP
        else:
            thread.exit()


def createCacheDbms(srvObj):
    """
    Creates the internal/local DBMS (SQLite based) used for managing the cache
    contents without constantly accessing the remote NGAS Cache Table in the
    NGAS DB.

    If this DBMS is not existing, it is created and the ngas_cache table
    created as well.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
    T = TRACE()

    hostId = srvObj.getHostId()

    # Create local Cache Contents DBMS.
    cacheContDbmName = "%s/%s_%s.db" %\
                       (ngamsHighLevelLib.getNgasChacheDir(srvObj.getCfg()),
                        NGAMS_CACHE_CONTENTS_DBMS, hostId)
    srvObj._cacheContDbms = sqlite.connect(cacheContDbmName,
                                           check_same_thread = False)
    srvObj._cacheContDbmsCur = srvObj._cacheContDbms.cursor()

    # Check if the ngas_cache table is created in the DBMS. If this is not the
    # case, create it.
    sqlQuery = "SELECT count(*) FROM ngas_cache"
    try:
        srvObj._cacheContDbmsCur.execute(sqlQuery)
    except Exception, e:
        if (str(e) == "no such table: ngas_cache"):
            info(2, "Table ngas_cache not found in local Cache DBMS - " +\
                 "creating")
            # Create table
            sqlQuery = "CREATE TABLE ngas_cache (" +\
                       "disk_id TEXT, " +\
                       "file_id TEXT, " +\
                       "file_version INTEGER, " +\
                       "filename TEXT, " +\
                       "file_size INTEGER, " +\
                       "cache_delete INTEGER, " +\
                       "last_check REAL, " +\
                       "cache_time REAL, " +\
                       "cache_entry_obj TEXT)"
            srvObj._cacheContDbmsCur.execute(sqlQuery)
        else:
            raise Exception, e

    # Create the DBM to hold information about new files that are registered
    # on this node (to be inserted into the Local Cache Contents DBMS).
    newFilesDbmName = "%s/%s_%s" %\
                      (ngamsHighLevelLib.getNgasChacheDir(srvObj.getCfg()),
                       NGAMS_CACHE_NEW_FILES_DBM, hostId)
    rmFile("%s*" % newFilesDbmName)
    srvObj._cacheNewFilesDbm = ngamsDbm.ngamsDbm(newFilesDbmName,
                                                  cleanUpOnDestr = 0,
                                                  writePerm = 1)

    # Create DBMs used by the Cache Control Plug-Ins.
    # - DBM used to schedule files for checking.
    cacheDir = ngamsHighLevelLib.getNgasChacheDir(srvObj.getCfg())
    cacheCtrlPiDbmName = "%s/%s_%s" % (cacheDir, NGAMS_CACHE_CTRL_PI_DBM,
                                       hostId)
    rmFile("%s*" % cacheCtrlPiDbmName)
    srvObj._cacheCtrlPiDbm = ngamsDbm.ngamsDbm(cacheCtrlPiDbmName,
                                                cleanUpOnDestr = 0,
                                                writePerm = 1)
    srvObj._cacheCtrlPiDbm.add(NGAMS_CACHE_CTRL_PI_DBM_WR, 0)
    srvObj._cacheCtrlPiDbm.add(NGAMS_CACHE_CTRL_PI_DBM_RD, 0)
    # - DBM used by the Cache Control Plug-Ins to schedule files for removal.
    cacheCtrlPiDelDbmName = "%s/%s_%s" % (cacheDir,
                                          NGAMS_CACHE_CTRL_PI_DEL_DBM,
                                          hostId)
    rmFile("%s*" % cacheCtrlPiDelDbmName)
    srvObj._cacheCtrlPiDelDbm = ngamsDbm.ngamsDbm(cacheCtrlPiDelDbmName,
                                                   cleanUpOnDestr = 0,
                                                   writePerm = 1)
    # - DBM used by the Cache Control Plug-Ins to schedule files which should
    # stay in the cache.
    cacheCtrlPiFilesDbmName = "%s/%s_%s" % (cacheDir,
                                            NGAMS_CACHE_CTRL_PI_FILES_DBM,
                                            hostId)
    rmFile("%s*" % cacheCtrlPiFilesDbmName)
    srvObj._cacheCtrlPiFilesDbm = ngamsDbm.ngamsDbm(cacheCtrlPiFilesDbmName,
                                                     cleanUpOnDestr = 0,
                                                     writePerm = 1)


def addEntryNewFilesDbm(srvObj,
                        diskId,
                        fileId,
                        fileVersion,
                        filename):
    """
    Add a new entry in the New Files DBM.

    srvObj:       Reference to server object (ngamsServer).

    diskId:       Disk ID for the cached data object (string).

    fileId:       File ID for the cached data object (string).

    fileVersion:  Version of the cached data object (integer).

    filename:     Name of file, relative to the volume root (string).

    Returns:      Void.
    """
    T = TRACE()

    try:
        srvObj._cacheNewFilesDbmSem.acquire()
        fileInfo = (diskId, fileId, fileVersion, filename)
        fileKey = ngamsLib.genFileKey(diskId, fileId, fileVersion)
        srvObj._cacheNewFilesDbm.add(fileKey, fileInfo)
        srvObj._cacheNewFilesDbmSem.release()
    except Exception, e:
        srvObj._cacheNewFilesDbmSem.release()
        msg = "Error accessing %s DBM. Error: %s"
        raise Exception, msg % (NGAMS_CACHE_NEW_FILES_DBM, str(e))


def getEntryNewFilesDbm(srvObj):
    """
    Get an entry from the New File DBM.

    srvObj:       Reference to server object (ngamsServer).

    Returns:      Tuple with (<Disk Id>, <File ID>, <File Version>,
                  <Filename>) or None if there are no entried in the DBM
                  (tuple | None).
    """
    T = TRACE()

    try:
        srvObj._cacheNewFilesDbmSem.acquire()
        srvObj._cacheNewFilesDbm.initKeyPtr()
        key, fileInfo = srvObj._cacheNewFilesDbm.getNext()
        if (key):
            srvObj._cacheNewFilesDbm.rem(key)
        else:
            fileInfo = None
        srvObj._cacheNewFilesDbmSem.release()
        return fileInfo
    except Exception, e:
        srvObj._cacheNewFilesDbmSem.release()
        msg = "Error accessing %s DBM. Error: %s"
        raise Exception, msg % (NGAMS_CACHE_NEW_FILES_DBM, str(e))


def queryCacheDbms(srvObj,
                   sqlQuery):
    """
    Execute an SQL query in the local DBMS and return the result.

    srvObj:       Reference to server object (ngamsServer).

    sqlQuery:     SQL query to return (string).

    Returns:      Result set (tuple with tuples).
    """
    T = TRACE(5)

    try:
        srvObj._cacheContDbmsSem.acquire()
        sqlQuery += ";"
        if (getMaxLogLevel() > 4):
            info(5, "Performing SQL query (Cache DBMS): " + str(sqlQuery))
        srvObj._cacheContDbmsCur.execute(sqlQuery)
        srvObj._cacheContDbms.commit() # TODO: Investigate this.
        res = srvObj._cacheContDbmsCur.fetchall()
        srvObj._cacheContDbmsSem.release()
        if (getMaxLogLevel() > 4):
            info(5, "Result of SQL query  (Cache DBMS) (" +\
                 str(sqlQuery) + "): " + str(res))
        return res
    except Exception, e:
        srvObj._cacheContDbmsSem.release()
        raise e


_ENTRY_IN_CACHE_DBMS_QUERY = "SELECT count(*) FROM ngas_cache WHERE " +\
                             "disk_id = '%s' AND file_id = '%s' AND " +\
                             "file_version = %d"

def entryInCacheDbms(srvObj,
                     diskId,
                     fileId,
                     fileVersion):
    """
    Check if a given entry is found in the local cache DBMS.

    srvObj:       Reference to server object (ngamsServer).

    diskId:       Disk ID for the cached data object (string).

    fileId:       File ID for the cached data object (string).

    fileVersion:  Version of the cached data object (integer).

    Returns:      Flag indicating if the given entry is found in the
                  cache DBMS (boolean).
    """
    T = TRACE()

    sqlQuery = _ENTRY_IN_CACHE_DBMS_QUERY % (diskId, fileId, fileVersion)
    res = queryCacheDbms(srvObj, sqlQuery)
    try:
        if (res[0][0] == 1):
            return True
        else:
            return False
    except:
        return False


_GET_FILENAME_FROM_CACHE_DBMS_QUERY = "SELECT filename FROM ngas_cache " +\
                                      "WHERE disk_id = '%s' AND " +\
                                      "file_id = '%s' AND file_version = %d"

def getFilenameFromCacheDbms(srvObj,
                             diskId,
                             fileId,
                             fileVersion):
    """
    Return the value of the filename column from the NGAS Cache Table.

    srvObj:       Reference to server object (ngamsServer).

    diskId:       Disk ID for the cached data object (string).

    fileId:       File ID for the cached data object (string).

    fileVersion:  Version of the cached data object (integer).

    Returns:      Value of filename column or None if not set (string | None).
    """
    T = TRACE()

    sqlQuery = _GET_FILENAME_FROM_CACHE_DBMS_QUERY %\
               (diskId, fileId, fileVersion)
    res = queryCacheDbms(srvObj, sqlQuery)
    try:
        if (res[0][0][0] == ""):
            return None
        else:
            return res[0][0][0]
    except:
        return None


_ADD_ENTRY_IN_CACHE_DBMS = "INSERT INTO ngas_cache (disk_id, file_id, " +\
                           "file_version, filename, file_size, " +\
                           "cache_delete, last_check, cache_time, " +\
                           "cache_entry_obj) VALUES ('%s', '%s', %d, '%s', " +\
                           "%d, %d, %.6f, %.6f, '%s')"

def addEntryInCacheDbms(srvObj,
                        diskId,
                        fileId,
                        fileVersion,
                        filename,
                        fileSize,
                        delete = False,
                        lastCheck = time.time(),
                        cacheTime = time.time(),
                        cacheEntryObj = "",
                        addInRdbms = True):
    """
    Insert a new cache entry into the NGAS Cache Table (in the local and
    remote DBMS).

    srvObj:         Reference to server object (ngamsServer).

    diskId:         Disk ID for the cached data object (string).

    fileId:         File ID for the cached data object (string).

    fileVersion:    Version of the cached data object (integer).

    filename:       Name of file, relative to the volume root (string).

    fileSize:       Size in bytes of archived file (integer).

    delete:         Mark file for deletion (boolean).

    lastCheck:      Time for last check (float).

    cacheTime:      Time when the file entered the cache (float).

    cacheEntryObj:  Cache Entry Object (ngamsCacheEntry).

    addInRdbms:     Add the entry also in the associated RDBMS (boolean).

    Returns:        Void.
    """
    T = TRACE()

    # Insert entry in the local DBMS (if not already there).
    timeNow = time.time()
    if (not entryInCacheDbms(srvObj, diskId, fileId, fileVersion)):
        if (delete):
            delete = 1
        else:
            delete = 0
        cacheEntryObjPickle = cPickle.dumps(cacheEntryObj)
        # Have to encode the pickled object to be able to write it in the
        # DB table.
        cacheEntryObjPickleEnc = base64.b32encode(cacheEntryObjPickle)
        sqlQuery = _ADD_ENTRY_IN_CACHE_DBMS %\
                   (diskId, fileId, int(fileVersion), filename, fileSize,
                    delete, timeNow, timeNow, cacheEntryObjPickleEnc)
        queryCacheDbms(srvObj, sqlQuery)

    if (addInRdbms):
        # Insert entry in the remote DBMS.
        srvObj.getDb().insertCacheEntry(diskId, fileId, fileVersion, timeNow,
                                        False)


_SET_FILENAME_CACHE_DBMS = "UPDATE ngas_cache SET filename = '%s' WHERE " +\
                           "disk_id = '%s' AND file_id = '%s' AND " +\
                           "file_version = %d"

def setFilenameCacheDbms(srvObj,
                         diskId,
                         fileId,
                         fileVersion,
                         filename):
    """
    Set the filename field for a row in the local Cache Contents DBMS.

    srvObj:         Reference to server object (ngamsServer).

    diskId:         Disk ID for the cached data object (string).

    fileId:         File ID for the cached data object (string).

    fileVersion:    Version of the cached data object (integer).

    filename:       Name of file, relative to the volume root (string).

    Returns:        Void.
    """
    T = TRACE()

    sqlQuery = _SET_FILENAME_CACHE_DBMS %\
               (filename, diskId, fileId, fileVersion)
    queryCacheDbms(srvObj, sqlQuery)


_SET_FILE_SIZE_CACHE_DBMS = "UPDATE ngas_cache SET file_size = %d WHERE " +\
                            "disk_id = '%s' AND file_id = '%s' AND " +\
                            "file_version = %d"

def setFileSizeCacheDbms(srvObj,
                         diskId,
                         fileId,
                         fileVersion,
                         fileSize):
    """
    Set the file size field for a row in the local Cache Contents DBMS.

    srvObj:         Reference to server object (ngamsServer).

    diskId:         Disk ID for the cached data object (string).

    fileId:         File ID for the cached data object (string).

    fileVersion:    Version of the cached data object (integer).

    fileSize:       Size of file (integer).

    Returns:        Void.
    """
    T = TRACE()

    sqlQuery = _SET_FILE_SIZE_CACHE_DBMS % (fileSize, diskId, fileId,
                                            fileVersion)
    queryCacheDbms(srvObj, sqlQuery)


_SET_CACHE_ENTRY_OBJECT_CACHE_DBMS_QUERY = "UPDATE ngas_cache SET " +\
                                           "cache_entry_obj = '%s' " +\
                                           "WHERE disk_id = '%s' AND " +\
                                           "file_id = '%s' AND " +\
                                           "file_version = %d"

def setCacheEntryObjectCacheDbms(srvObj,
                                 cacheEntryObj):
    """
    Set the file size field for a row in the local Cache Contents DBMS.

    srvObj:         Reference to server object (ngamsServer).

    cacheEntryObj:  Cache entry object instance (ngamsCacheEntry).

    Returns:        Void.
    """
    T = TRACE()

    cacheEntryObjPickle = cPickle.dumps(cacheEntryObj)
    cacheEntryObjPickleEnc = base64.b32encode(cacheEntryObjPickle)
    sqlQuery = _SET_CACHE_ENTRY_OBJECT_CACHE_DBMS_QUERY %\
               (cacheEntryObjPickleEnc, cacheEntryObj.getDiskId(),
                cacheEntryObj.getFileId(), cacheEntryObj.getFileVersion())
    queryCacheDbms(srvObj, sqlQuery)


_DEL_ENTRY_FROM_CACHE_DBMS_QUERY = "DELETE FROM ngas_cache WHERE " +\
                                   "disk_id = '%s' AND " +\
                                   "file_id = '%s' AND file_version = %d"

def delEntryFromCacheDbms(srvObj,
                          diskId,
                          fileId,
                          fileVersion):
    """
    Delete an entry from the Cache Contents DBMS'.

    srvObj:       Reference to server object (ngamsServer).

    diskId:       Disk ID for the cached data object (string).

    fileId:       File ID for the cached data object (string).

    fileVersion:  Version of the cached data object (integer).

    Returns:      Void.
    """
    T = TRACE()

    # Remove from the Local Cache Contents DBMS.
    sqlQuery = _DEL_ENTRY_FROM_CACHE_DBMS_QUERY %\
               (diskId, fileId, int(fileVersion))
    queryCacheDbms(srvObj, sqlQuery)

    # Remove from the Remote Cache Contents DBMS.
    srvObj.getDb().deleteCacheEntry(diskId, fileId, fileVersion)


def initCacheArchive(srvObj):
    """
    Initialize the NGAS Cache Archive Service. If there are requests in the
    Cache Table in the DB, these are read out and inserted in the local
    Cache Contents DBM.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
    T = TRACE()

    # Create/open the Cache Contents DBM.
    # Note: This DBMS is kept between sessions for efficiency reasons.
    createCacheDbms(srvObj)

    # Check if all files registered in the RDBMS NGAS Cache Table are
    # registered in the Local Cache DBMS.
    curObj = srvObj.getDb().getCacheContents(srvObj.getHostId())
    while (True):
        fileInfoList = curObj.fetch(10000)
        if (not fileInfoList): break
        for sqlFileInfo in fileInfoList:
            diskId      = sqlFileInfo[0]
            fileId      = sqlFileInfo[1]
            fileVersion = int(sqlFileInfo[2])
            delete      = int(sqlFileInfo[3])
            if (entryInCacheDbms(srvObj, diskId, fileId, fileVersion)):
                continue
            # Set filename, file size and Cache Entry Object later.
            addEntryInCacheDbms(srvObj, diskId, fileId, fileVersion, "", -1,
                                cacheEntryObj = "", addInRdbms = True)

    # Update the local Cache Content DBMS with the information about files
    # online on this node.
    curObj = srvObj.getDb().getFileSummary1(hostId = srvObj.getHostId(),
                                            fileStatus = [], order = False)
    while (True):
        fileInfoList = curObj.fetch(10000)
        if (not fileInfoList): break
        for sqlFileInfo in fileInfoList:
            diskId      = sqlFileInfo[ngamsDbCore.SUM1_DISK_ID]
            fileId      = sqlFileInfo[ngamsDbCore.SUM1_FILE_ID]
            fileVersion = int(sqlFileInfo[ngamsDbCore.SUM1_VERSION])
            filename    = sqlFileInfo[ngamsDbCore.SUM1_FILENAME]
            if (entryInCacheDbms(srvObj, diskId, fileId, fileVersion)):
                # Ensure that filename (and ngamsCacheEntry 0bject) is defined.
                if (getFilenameFromCacheDbms(srvObj, diskId, fileId,
                                             fileVersion)):
                    continue
                else:
                    # If the filename is not defined, this is an entry that
                    # has been recovered from the RDBMS NGAS Cache Table.
                    # We add the filename and the Cache Entry Object to the
                    # entry for that file.
                    setFilenameCacheDbms(srvObj, diskId, fileId,
                                         fileVersion, filename)
                    fileSize = sqlFileInfo[ngamsDbCore.SUM1_FILE_SIZE]
                    setFileSizeCacheDbms(srvObj, diskId, fileId,
                                         fileVersion, fileSize)
                    ingDate8601 = srvObj.getDb().\
                                  getIngDate(diskId, fileId, fileVersion)
                    ingDateSecs = iso8601ToSecs(ingDate8601)
                    cacheEntryObj = ngamsCacheEntry.ngamsCacheEntry().\
                                    unpackSqlInfo(sqlFileInfo).\
                                    setLastCheck(time.time()).\
                                    setCacheTime(ingDateSecs)
                    setCacheEntryObjectCacheDbms(srvObj, cacheEntryObj)
                    continue

            # Add new entry in the DBMS'.
            ingDate8601 = srvObj.getDb().\
                          getIngDate(diskId, fileId, fileVersion)
            ingDateSecs = iso8601ToSecs(ingDate8601)
            lastCheckTime = time.time()
            cacheEntryObject = ngamsCacheEntry.ngamsCacheEntry().\
                               unpackSqlInfo(sqlFileInfo).\
                               setLastCheck(lastCheckTime).\
                               setCacheTime(ingDateSecs)
            fileSize = sqlFileInfo[ngamsDbCore.SUM1_FILE_SIZE]
            addEntryInCacheDbms(srvObj, diskId, fileId, fileVersion,
                                filename, fileSize, lastCheck = lastCheckTime,
                                cacheTime = ingDateSecs,
                                cacheEntryObj = cacheEntryObject)

    # Start the Cache Control Plug-In helper threads if a Cache Control Plug-In
    # is specified.
    cacheControlPi = srvObj.getCfg().getVal("Caching[1].CacheControlPlugIn")
    if (cacheControlPi):
        noOfThreads = srvObj.getCfg().getVal("Caching[1].NumberOfPlugIns")
        if (noOfThreads):
            parameters = [srvObj]
            srvObj._cacheCtrlPiThreadGr = ngamsThreadGroup.\
                                          ngamsThreadGroup(\
                "CACHE-CONTROL-PI-THREAD", _cacheCtrlPlugInThread,
                int(noOfThreads), parameters)
            srvObj._cacheCtrlPiThreadGr.start(wait = False)


def checkNewFilesDbm(srvObj):
    """
    Check if there are new files registered on this node to be inserted into
    the Local Cache Contents DBMS.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
    T = TRACE()

    while (True):
        fileInfo = getEntryNewFilesDbm(srvObj)
        if (not fileInfo): break

        # Get the rest of File Summary 1 info and create an ngamsCacheEntry
        # object.
        sqlFileInfo = srvObj.getDb().\
                      getFileSummary1SingleFile(fileInfo[NGAMS_CACHE_DISK_ID],
                                                fileInfo[NGAMS_CACHE_FILE_ID],
                                                fileInfo[NGAMS_CACHE_FILE_VER])

        if (sqlFileInfo == []):
            msg = "No file found matching: %s/%s/%s" %\
                  (fileInfo[NGAMS_CACHE_DISK_ID],
                   fileInfo[NGAMS_CACHE_FILE_ID],
                   str(fileInfo[NGAMS_CACHE_FILE_VER]))
            error(msg)

        # Add the new entry.
        info(3, "Adding new entry in Cache DBMS: %s/%s/%s" %\
             (fileInfo[NGAMS_CACHE_DISK_ID], fileInfo[NGAMS_CACHE_FILE_ID],
              fileInfo[NGAMS_CACHE_FILE_VER]))
        timeNow = time.time()
        cacheEntryObject = ngamsCacheEntry.ngamsCacheEntry().\
                           unpackSqlInfo(sqlFileInfo).\
                           setLastCheck(timeNow).\
                           setCacheTime(timeNow)
        addEntryInCacheDbms(srvObj,
                            cacheEntryObject.getDiskId(),
                            cacheEntryObject.getFileId(),
                            cacheEntryObject.getFileVersion(),
                            cacheEntryObject.getFilename(),
                            cacheEntryObject.getFileSize(),
                            lastCheck = timeNow,
                            cacheTime = timeNow,
                            cacheEntryObj = cacheEntryObject)


# Template for query to update the last check field for the file in the
# Local Cache Contents DBMS.
_LAST_CHECK_QUERY_TPL = "UPDATE ngas_cache SET last_check = %.6f WHERE " +\
                        "disk_id = '%s' AND file_id = '%s' AND " +\
                        "file_version = %d"

def markFileChecked(srvObj,
                    sqlFileInfo):
    """
    Set the Last Check field for a cache to the current time to indicate
    that it was checked.

    srvObj:       Reference to server object (ngamsServer).

    sqlFileInfo:  Information for one file as queried from the NGAS Cache
                  Table (list).

    Returns:      Void.
    """
    T = TRACE()

    sqlQuery = _LAST_CHECK_QUERY_TPL % (time.time(),
                                        sqlFileInfo[NGAMS_CACHE_DISK_ID],
                                        sqlFileInfo[NGAMS_CACHE_FILE_ID],
                                        int(sqlFileInfo[NGAMS_CACHE_FILE_VER]))
    queryCacheDbms(srvObj, sqlQuery)


# Template to be used to build SQL queries when marking items for deletion
# from the local cache.
_SCHEDULE_DEL_TPL = "UPDATE ngas_cache SET cache_delete = 1 WHERE " +\
                    "disk_id = '%s' AND file_id = '%s' AND file_version = %d"

def requestFileForDeletion(srvObj, sqlFileInfo):
    """
    Explicitly request a file for deletion from the cache.
    This function is always called from outside of the CacheControllerThread

    srvObj:       Reference to server object (ngamsServer).

    sqlFileInfo:  Information for one file as queried from the NGAS Cache
                  Table (list).

    Returns:      Void.
    """
    #T = TRACE()
    explicitDelQueue.put(sqlFileInfo) # this is thread safe



def scheduleFileForDeletion(srvObj,
                            sqlFileInfo):
    """
    Schedule a file for deletion from the cache.

    srvObj:       Reference to server object (ngamsServer).

    sqlFileInfo:  Information for one file as queried from the NGAS Cache
                  Table (list).

    Returns:      Void.
    """
    T = TRACE()

    diskId      = sqlFileInfo[NGAMS_CACHE_DISK_ID]
    fileId      = sqlFileInfo[NGAMS_CACHE_FILE_ID]
    fileVersion = int(sqlFileInfo[NGAMS_CACHE_FILE_VER])
    msg = "Scheduling entry %s/%s/%s for deletion from the " +\
          "NGAS Cache Archive"
    info(2, msg % (diskId, fileId, str(fileVersion)))
    sqlQuery = _SCHEDULE_DEL_TPL % (diskId, fileId, int(fileVersion))
    queryCacheDbms(srvObj, sqlQuery)
    srvObj.getDb().updateCacheEntry(diskId, fileId, fileVersion, 1)


def createTmpDbm(srvObj,
                 id,
                 delOnDestr = True):
    """
    Create a temporary DBM. It will be created in the NGAS Temporary Directory.

    srvObj:       Reference to server object (ngamsServer).

    id:           ID used to build the name of the DBM (string).

    delOnDestr:   Delete (remove) the DBM upon destruction the DBM object
                  (boolean).

    Returns:      DBM object (ngamsDbm2).
    """
    T = TRACE()

    dbmName = os.path.normpath(ngamsHighLevelLib.\
                               getNgasTmpDir(srvObj.getCfg()) +\
                               "/CACHE_%s_DBM" % id)
    rmFile(dbmName + "*")
    dbm = ngamsDbm.ngamsDbm(dbmName, cleanUpOnDestr = delOnDestr,
                             writePerm = 1)
    return dbm


def _addEntryCacheCtrlPlugInDbm(srvObj,
                                cacheEntryObj):
    """
    Add an entry in the  Cache Control Plug-In DBM.

    srvObj:         Reference to server object (ngamsServer).

    cacheEntryObj:  Cache Entry Object to add in the DBM (ngamsCacheEntry).

    Returns:        Void.
    """
    T = TRACE(5)

    try:
        srvObj._cacheCtrlPiThreadGr.takeGenMux()
        writeIdx = srvObj._cacheCtrlPiDbm.get(NGAMS_CACHE_CTRL_PI_DBM_WR)
        srvObj._cacheCtrlPiDbm.add(str(writeIdx), cacheEntryObj)
        writeIdx = ((writeIdx + 1) % NGAMS_CACHE_CTRL_PI_DBM_MAX)
        srvObj._cacheCtrlPiDbm.add(NGAMS_CACHE_CTRL_PI_DBM_WR, writeIdx)
        srvObj._cacheCtrlPiThreadGr.releaseGenMux()
    except Exception, e:
        srvObj._cacheCtrlPiThreadGr.releaseGenMux()
        raise Exception, e


def _getEntryCacheCtrlPlugInDbm(srvObj):
    """
    Get an entry from the Cache Control Plug-In DBM.

    srvObj:       Reference to server object (ngamsServer).

    Returns:      Next Sync. Request Object or None if there are no requests
                  in the DBM (ngamsCacheEntry | None).
    """
    T = TRACE(5)

    try:
        srvObj._cacheCtrlPiThreadGr.takeGenMux()
        readIdx = srvObj._cacheCtrlPiDbm.get(NGAMS_CACHE_CTRL_PI_DBM_RD)
        cacheEntryObj = srvObj._cacheCtrlPiDbm.get(str(readIdx))
        if (cacheEntryObj):
            srvObj._cacheCtrlPiDbm.rem(str(readIdx))
            readIdx = ((readIdx + 1) % NGAMS_CACHE_CTRL_PI_DBM_MAX)
            srvObj._cacheCtrlPiDbm.add(NGAMS_CACHE_CTRL_PI_DBM_RD, readIdx)
        srvObj._cacheCtrlPiThreadGr.releaseGenMux()

        return cacheEntryObj
    except Exception, e:
        srvObj._cacheCtrlPiThreadGr.releaseGenMux()
        raise Exception, e


def _getCountCacheCtrlPlugInDbm(srvObj):
    """
    Get the current count (=number of elements in the

    srvObj:       Reference to server object (ngamsServer).

    Returns:      Next Sync. Request Object or None if there are no requests
                  in the DBM (ngamsCacheEntry | None).
    """
    T = TRACE(5)

    try:
        srvObj._cacheCtrlPiThreadGr.takeGenMux()
        noOfEls = srvObj._cacheCtrlPiDbm.getCount()
        srvObj._cacheCtrlPiThreadGr.releaseGenMux()

        return noOfEls
    except Exception, e:
        srvObj._cacheCtrlPiThreadGr.releaseGenMux()
        raise Exception, e


def _cacheCtrlPlugInThread(threadGrObj):
    """
    Function to run as a thread to check if there are entries in the
    Cache Control Plug-In DBM to check for file validity.

    threadGrObj:  Reference to Thread Group Object to which this thread
                  belongs (ngamsThreadGroup).

    Returns:      Void.
    """
    T = TRACE()

    srvObj      = threadGrObj.getParameters()[0]

    # Load the plug-in module.
    cacheCtrlPlugIn = srvObj.getCfg().getVal("Caching[1].CacheControlPlugIn")
    plugInMethod = loadPlugInEntryPoint(cacheCtrlPlugIn)

    deleteMsg = "CACHE-CRITERIA: Plug-in Selected File for Deletion: %s/%s/%s"

    # Loop until instructed to stop.
    while (True):
        checkStopCacheControlThread(srvObj, raiseEx = False)
        time.sleep(0.500)

        # Get the next Cache Entry Object (if there are any queued).
        while (True):
            checkStopCacheControlThread(srvObj, raiseEx = False)
            cacheEntryObj = _getEntryCacheCtrlPlugInDbm(srvObj)
            if (not cacheEntryObj): break

            # Invoke Cache Control Plug-In on the file.
            try:
                deleteFile = plugInMethod(srvObj, cacheEntryObj)
                if (deleteFile):
                    info(2, deleteMsg % (cacheEntryObj.getDiskId(),
                                         cacheEntryObj.getFileId(),
                                         str(cacheEntryObj.getFileVersion())))
                    srvObj._cacheCtrlPiDelDbm.addIncKey(cacheEntryObj)
                else:
                    srvObj._cacheCtrlPiFilesDbm.addIncKey(cacheEntryObj)
            except Exception, e:
                warning("Error occurred in thread. Error: %s" % str(e))
                # Put the entry in the queue to make it stay in the system
                # still.
                srvObj._cacheCtrlPiFilesDbm.addIncKey(cacheEntryObj)

def markFileCanBeDeleted(srvObj):
    """
    Go through the explicitDel queue to mark file deletion
    using the file_status flag in the remote database
    """
    info(3, 'marking file can be deleted')
    while (1):
        sqlFileInfo = None
        try:
            sqlFileInfo = explicitDelQueue.get_nowait()
        except Empty, e:
            break
        if (sqlFileInfo is None):
            continue
        diskId      = sqlFileInfo[NGAMS_CACHE_DISK_ID]
        fileId      = sqlFileInfo[NGAMS_CACHE_FILE_ID]
        fileVersion = int(sqlFileInfo[NGAMS_CACHE_FILE_VER])
        #TODO - should get the original file_status from the remote db, and then do a bitmask OR operation,
        # maybe too db resource intensive, since this file is about to be deleted, the original value is not that important
        # moreover, it is most likely just ingested (when cache delete is triggered), so we can assume that it is "00000000"
        try:
            info(3, 'Set file_status for file %s' % fileId)
            srvObj.getDb().setFileStatus(fileId, fileVersion, diskId, CACHE_DEL_BIT_MASK) # should be (CACHE_DEL_BIT_MASK | file_status)
        except Exception, err:
            error('Fail to set file status for file %s, Exception: %s' % (fileId, str(err)))
            continue

def checkIfFileCanBeDeleted(srvObj, fileId, fileVersion, diskId):
    """
    Check if the file can be deleted from its file_status flag
    """
    fileStatus = srvObj.getDb().getFileStatus(fileId, fileVersion, diskId)

    re = bin(int(fileStatus, 2) & CACHE_DEL_BIT_MASK_INT)[2:] # logic AND, and remove the '0b', e.g '0b11001' --> '11001'
    re = re.zfill(8) # fill zeroes at the beginning, e.g. '100' --> '00000100'

    return (CACHE_DEL_BIT_MASK == re)


def checkCacheContents(srvObj):
    """
    Go through the contents in the cache and check for each item not already
    marked for deletion, if it can be deleted. If it can be deleted, mark it
    for deletion.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
    T = TRACE()

    # If several methods for checking the cache contents is activated
    # these are applied sequentially. The first rule that matches on the
    # file tested, means that the checking stops and the file is scheduled
    # for removal.
    #
    # The rules are applies in the following sequence:
    #
    # 0. Check the files in the explicitDel queue
    #
    # 1. Check if the files has been in the cache for more than the
    #    specified, maximum time.
    #
    # 2. Check that the volume of the files residing in the cache is not
    #    exceeding the defined limit. If there are more files than this limit,
    #    files are deleted FIFO-wise until reaching the maximum limit -10%.
    #
    # 3. Check if there are more files in the cache than the specified
    #    limit. If there are more files, files are deleted FIFO-wise, until
    #    going below the maximum limit -10%.
    #
    # 4. Check if the minimum specified available space in the cache is
    #    exceeded. If this is the case, remove files FIFO-wise until
    #    reaching the maximum limit -10%.
    #
    # 5. Execute the Cache Control Plug-In (if specified in the
    #    configuration).

    # 0. Go through the explicitDel queue to remove files
    """
    while (1):
        sqlFileInfo = None
        try:
            sqlFileInfo = explicitDelQueue.get_nowait()
        except Empty, e:
            break
        if (sqlFileInfo is None):
            continue
        scheduleFileForDeletion(srvObj, sqlFileInfo)
        markFileChecked(srvObj, sqlFileInfo)
    """
    if (CHECK_CAN_BE_DELETED):
        markFileCanBeDeleted(srvObj)

    # 1. Evaluate if there are files residing in the cache for more than
    #    the specified amount of time.
    if (srvObj.getCfg().getVal("Caching[1].MaxTime")):
        info(4, "Applying criteria: Expired files ...")
        maxCacheTime = int(srvObj.getCfg().getVal("Caching[1].MaxTime"))
        sqlQuery = "SELECT * FROM ngas_cache WHERE cache_time < %.6f" %\
                   (time.time() - maxCacheTime)
        # Dump the results into a temporary DBM.
        delFilesDbm = createTmpDbm(srvObj, "EXP_FILES_INFO")
        # Encapsulate this in a try clause to be able to semaphore protect
        # the interaction with SQLite in case other threads would try to
        # access the DBMS.
        try:
            srvObj._cacheContDbmsSem.acquire()
            srvObj._cacheContDbmsCur.execute(sqlQuery)
            while (True):
                fileInfoList = srvObj._cacheContDbmsCur.fetchmany(10000)
                if (not fileInfoList): break
                for sqlFileInfo in fileInfoList:
                    info(2, "CACHE-CRITERIA: Maximum Time Expired: %s/%s/%s" %\
                         (sqlFileInfo[NGAMS_CACHE_DISK_ID],
                          sqlFileInfo[NGAMS_CACHE_FILE_ID],
                          str(sqlFileInfo[NGAMS_CACHE_FILE_VER])))
                    delFilesDbm.addIncKey(sqlFileInfo)
            srvObj._cacheContDbms.commit()
            srvObj._cacheContDbmsSem.release()
        except Exception, e:
            srvObj._cacheContDbmsSem.release()
            raise Exception, e

        # Now, loop over the selected files and mark them for deletion.
        delFilesDbm.initKeyPtr()
        while (True):
            key, sqlFileInfo = delFilesDbm.getNext()
            if (not key): break
            scheduleFileForDeletion(srvObj, sqlFileInfo)
            markFileChecked(srvObj, sqlFileInfo)

        del delFilesDbm
        delFilesDbm = None
        info(4, "Applied criteria: Expired files")

    # 2. Remove files if there more files (in volume) in the cache than the
    #    specified threshold.
    if (srvObj.getCfg().getVal("Caching[1].MaxCacheSize")):
        info(4, "Applying criteria: Maximum cache size ...")
        maxCacheSize = int(srvObj.getCfg().getVal("Caching[1].MaxCacheSize"))
        # Check if the size of the cache content exceeds the specified limit.
        sqlQuery = "SELECT sum(file_size) FROM ngas_cache"
        cacheSum = queryCacheDbms(srvObj, sqlQuery)[0][0]
        if (not cacheSum):
            cacheSum = 0
        else:
            cacheSum = int(cacheSum)

        msg = "Current size of cache: %.3f GB, " +\
                  "Maximum cache size: %.3f GB"
        info(3, msg % ((float(cacheSum) / 1e9),
                           (float(maxCacheSize) / 1e9)))

        if (cacheSum > maxCacheSize):
            msg = "Current size of cache: %.6f MB exceeding specified " +\
                  "threshold: %.6f MB"
            info(3, msg % ((float(cacheSum) / 1e6),
                           (float(maxCacheSize) / 1e6)))
            # Reduce the size of the cache to 10% below the threshold
            # to avoid having to clean-up constantly due to this rule.
            maxCacheSize *= 0.9

            # Schedule files FIFO-wise for removal from the cache.
            sqlQuery = "SELECT * FROM ngas_cache ORDER BY cache_time"
            # Dump the results into a temporary DBM.
            delFilesDbm = createTmpDbm(srvObj, "MAX_VOL_FILES_INFO")
            # Encapsulate this in a try clause to be able to semaphore protect
            # the interaction with SQLite in case other threads would try to
            # access the DBMS.
            try:
                srvObj._cacheContDbmsSem.acquire()
                srvObj._cacheContDbmsCur.execute(sqlQuery)
                while (cacheSum > maxCacheSize):
                    fileInfoList = srvObj._cacheContDbmsCur.fetchmany(10000)
                    if (not fileInfoList): break
                    for sqlFileInfo in fileInfoList:
                        if (CHECK_CAN_BE_DELETED):
                            try:
                                if (not checkIfFileCanBeDeleted(srvObj,
                                                                sqlFileInfo[NGAMS_CACHE_FILE_ID],
                                                                sqlFileInfo[NGAMS_CACHE_FILE_VER],
                                                                sqlFileInfo[NGAMS_CACHE_DISK_ID])):
                                    info(2, "Cannot delete file from the cache: %s/%s/%s" %\
                                         (str(sqlFileInfo[0]), str(sqlFileInfo[1]), str(sqlFileInfo[2])))
                                    continue
                            except Exception, cee:
                                if (str(cee).find('file not found in ngas db') > -1):
                                    warning("file already gone, still mark for deletion: %s/%s/%s" %\
                                            (str(sqlFileInfo[0]), str(sqlFileInfo[1]), str(sqlFileInfo[2])))
                                else:
                                    raise cee

                        msg = "CACHE-CRITERIA: Maximum Cache Size " +\
                              "Exceeded: %s/%s/%s"
                        info(2, msg %\
                             (sqlFileInfo[NGAMS_CACHE_DISK_ID],
                              sqlFileInfo[NGAMS_CACHE_FILE_ID],
                              str(sqlFileInfo[NGAMS_CACHE_FILE_VER])))
                        delFilesDbm.addIncKey(sqlFileInfo)
                        fileSize = int(sqlFileInfo[NGAMS_CACHE_FILE_SIZE])
                        cacheSum -= fileSize
                        if (cacheSum < maxCacheSize): break
                srvObj._cacheContDbms.commit()
                srvObj._cacheContDbmsSem.release()
            except Exception, e:
                srvObj._cacheContDbmsSem.release()
                raise Exception, e

            # Now, loop over the selected files and mark them for deletion.
            delFilesDbm.initKeyPtr()
            while (True):
                key, sqlFileInfo = delFilesDbm.getNext()
                if (not key): break
                scheduleFileForDeletion(srvObj, sqlFileInfo)
                markFileChecked(srvObj, sqlFileInfo)

            del delFilesDbm
            delFilesDbm = None
        info(4, "Applied criteria: Maximum cache size")

    # 3. Remove files if there are more files in the cache than the
    #    specified threshold.
    if (srvObj.getCfg().getVal("Caching[1].MaxFiles")):
        info(4, "Applying criteria: Maximum number of files ...")
        maxFiles = int(srvObj.getCfg().getVal("Caching[1].MaxFiles"))
        sqlQuery = "SELECT count(*) FROM ngas_cache"
        numberOfFiles = queryCacheDbms(srvObj, sqlQuery)[0][0]
        if (not numberOfFiles):
            numberOfFiles = 0
        else:
            numberOfFiles = int(numberOfFiles)
        if (numberOfFiles > maxFiles):
            # Remove files from the cache, FIFO-wise, until the number of
            # files is 10% below the specified limit.
            # It seems that ROWCOUNT is not implemented for SELECT
            # for SQLite. We do it the brute way ...
            sqlQuery = "SELECT * FROM ngas_cache ORDER BY cache_time"
            noOfFilesToRemove = int(1.10 * float(numberOfFiles - maxFiles))
            count = 0
            # Dump the results into a temporary DBM.
            delFilesDbm = createTmpDbm(srvObj, "MAX_NO_FILES_INFO")
            # Encapsulate this in a try clause to be able to semaphore protect
            # the interaction with SQLite in case other threads would try to
            # access the DBMS.
            try:
                srvObj._cacheContDbmsSem.acquire()
                srvObj._cacheContDbmsCur.execute(sqlQuery)
                while (count < noOfFilesToRemove):
                    fileInfoList = srvObj._cacheContDbmsCur.fetchmany(10000)
                    if (not fileInfoList): break
                    for sqlFileInfo in fileInfoList:
                        msg = "CACHE-CRITERIA: Maximum Number of Files in " +\
                              "Cache Exceeded: %s/%s/%s"
                        info(2, msg %\
                             (sqlFileInfo[NGAMS_CACHE_DISK_ID],
                              sqlFileInfo[NGAMS_CACHE_FILE_ID],
                              str(sqlFileInfo[NGAMS_CACHE_FILE_VER])))
                        delFilesDbm.addIncKey(sqlFileInfo)
                        count += 1
                        if (count >= noOfFilesToRemove): break
                srvObj._cacheContDbms.commit()
                srvObj._cacheContDbmsSem.release()
            except Exception, e:
                srvObj._cacheContDbmsSem.release()
                raise Exception, e

            # Now, marked the selected files for deletion.
            delFilesDbm.initKeyPtr()
            while (True):
                key, sqlFileInfo = delFilesDbm.getNext()
                if (not key): break
                scheduleFileForDeletion(srvObj, sqlFileInfo)
                markFileChecked(srvObj, sqlFileInfo)
            del delFilesDbm
            delFilesDbm = None
        info(4, "Applied criteria: Maximum number of files")

    # 4. Check if the minimum space that should be available in the cache
    #    is exhausted.
    if (srvObj.getCfg().getVal("Caching[1].MinCacheSpace")):
        # This feature is not yet supported.
        # TODO: Make function that derives the space by looking at the
        #       volumes directly. Should look at each file system, note,
        #       in simulation mode, several volumes may be hosted on the same
        #       file system.
        msg = "MINIMUM AVAILABLE CACHE SPACE AS CRITERIA IS NOT YET " +\
              "SUPPORTED"
        notice(msg)
    if (0 and srvObj.getCfg().getVal("Caching[1].MinCacheSpace")):
        info(4, "Applying criteria: Minimum available cache space ...")
        minCacheSpace = int(srvObj.getCfg().getVal("Caching[1].MinCacheSpace"))
        spaceAvailMb = srvObj.getDb().getSpaceAvailForHost(srvObj.getHostId())
        msg = "Space Available=%.6f MB, Min. Cache Space=%.6f MB, " +\
              "Availability till Threshold: %.6f MB"
        info(4, msg % (spaceAvailMb, minCacheSpace,
                       (spaceAvailMb - (1.1 * minCacheSpace))))
        if (spaceAvailMb < minCacheSpace):
            # Reduce the size of the cache to 10% below the threshold
            # to avoid having to clean-up constantly due to this rule.
            minCacheSpace *= 1.10
            # Dump the results into a temporary DBM.
            delFilesDbm = createTmpDbm(srvObj, "MIN_SPACE_FILES_INFO")
            # Schedule files FIFO-wise for removal from the cache.
            sqlQuery = "SELECT * FROM ngas_cache ORDER BY cache_time"
            # Encapsulate this in a try clause to be able to semaphore protect
            # the interaction with SQLite in case other threads would try to
            # access the DBMS.
            try:
                srvObj._cacheContDbmsSem.acquire()
                srvObj._cacheContDbmsCur.execute(sqlQuery)
                while (spaceAvailMb < minCacheSpace):
                    fileInfoList = srvObj._cacheContDbmsCur.fetchmany(10000)
                    if (not fileInfoList): break
                    for sqlFileInfo in fileInfoList:
                        msg = "CACHE-CRITERIA: Minimum Cache Space " +\
                              "Exhausted: %s/%s/%s"
                        info(2, msg %\
                             (sqlFileInfo[NGAMS_CACHE_DISK_ID],
                              sqlFileInfo[NGAMS_CACHE_FILE_ID],
                              str(sqlFileInfo[NGAMS_CACHE_FILE_VER])))
                        delFilesDbm.addIncKey(sqlFileInfo)
                        fileSize = sqlFileInfo[NGAMS_CACHE_FILE_SIZE]
                        spaceAvailMb += fileSize
                    if (spaceAvailMb < minCacheSpace): break
                srvObj._cacheContDbms.commit()
                srvObj._cacheContDbmsSem.release()
            except Exception, e:
                srvObj._cacheContDbmsSem.release()
                raise Exception, e

            # Now, loop over the selected files and mark them for deletion.
            delFilesDbm.initKeyPtr()
            while (True):
                key, sqlFileInfo = delFilesDbm.getNext()
                if (not key): break
                scheduleFileForDeletion(srvObj, sqlFileInfo)
                markFileChecked(srvObj, sqlFileInfo)

            del delFilesDbm
            delFilesDbm = None
        info(4, "Applied criteria: Minimum available cache space")

    # 5. Invoke the Cache Control Plug-In (if specified) on the files.
    if (srvObj.getCfg().getVal("Caching[1].CacheControlPlugIn")):
        info(4, "Applying criteria: Cache Control Plug-In ...")

        # Query the files from the DB and process them.
        sqlQuery = "SELECT * FROM ngas_cache"
        # Encapsulate this in a try clause to be able to semaphore protect
        # the interaction with SQLite in case other threads would try to
        # access the DBMS.
        try:
            srvObj._cacheContDbmsSem.acquire()
            srvObj._cacheContDbmsCur.execute(sqlQuery)
            while (True):
                fileInfoList = srvObj._cacheContDbmsCur.fetchmany(10000)
                if (not fileInfoList): break
                for sqlFileInfo in fileInfoList:
                    cacheEntryObjEnc = sqlFileInfo[NGAMS_CACHE_ENTRY_OBJ]
                    cacheEntryObjPickle = base64.b32decode(cacheEntryObjEnc)
                    cacheEntryObj = cPickle.loads(cacheEntryObjPickle)
                    if (not cacheEntryObj.getPar(NGAMS_CACHE_CTRL_PI_DBM_SQL)):
                        cacheEntryObj.addPar(NGAMS_CACHE_CTRL_PI_DBM_SQL,
                                             sqlFileInfo)
                    _addEntryCacheCtrlPlugInDbm(srvObj, cacheEntryObj)
            srvObj._cacheContDbms.commit()
            srvObj._cacheContDbmsSem.release()
        except Exception, e:
            srvObj._cacheContDbmsSem.release()
            raise Exception, e

        # Wait for all entries in the Cache Control Plug-In DBM to be handled.
        while (True):
            checkStopCacheControlThread(srvObj)
            count = _getCountCacheCtrlPlugInDbm(srvObj)
            if (count == 2): break
            time.sleep(0.250)

        # Mark the files to be deleted for deletion.
        while (True):
            srvObj._cacheCtrlPiDelDbm.initKeyPtr()
            key, cacheEntryObj = srvObj._cacheCtrlPiDelDbm.getNext()
            if (not key): break
            sqlFileInfo = cacheEntryObj.getPar(NGAMS_CACHE_CTRL_PI_DBM_SQL)
            scheduleFileForDeletion(srvObj, sqlFileInfo)
            markFileChecked(srvObj, sqlFileInfo)
            srvObj._cacheCtrlPiDelDbm.rem(key)

        # - mark the rest as checked.
        while (True):
            srvObj._cacheCtrlPiFilesDbm.initKeyPtr()
            key, cacheEntryObj = srvObj._cacheCtrlPiFilesDbm.getNext()
            if (not key): break
            sqlFileInfo = cacheEntryObj.getPar(NGAMS_CACHE_CTRL_PI_DBM_SQL)
            markFileChecked(srvObj, sqlFileInfo)
            setCacheEntryObjectCacheDbms(srvObj, cacheEntryObj)
            srvObj._cacheCtrlPiFilesDbm.rem(key)

        info(4, "Applied criteria: Cache Control Plug-In")


def removeFile(srvObj,
               diskInfoObj,
               fileId,
               fileVersion,
               filename):
    """
    Remove a file, archived on this system. This involves removing the
    information from the DB and removing the local copy.

    diskInfoObj:    Disk info object for the disk, hosting the file
                    (ngamsDiskInfo).

    fileId:         ID for file to remove (string).

    fileVersion:    Version of file to remove (integer).

    filename:       Relative name of file to remove (string).

    Returns:        Void.
    """
    T = TRACE()

    # Remove the entry from the DB. This includes updating the NGAS Disks
    # Table.
    try:
        info(3, "Deleting file information from DB for file: %s/%s/%s" %\
             (diskInfoObj.getDiskId(), fileId, str(fileVersion)))
        srvObj.getDb().deleteFileInfo(srvObj.getHostId(), diskInfoObj.getDiskId(), fileId,
                                      fileVersion)
        msg = "Deleted file from DB: %s/%s/%s" %\
              (diskInfoObj.getDiskId(), fileId, str(fileVersion))
        info(2, msg)
    except Exception, e:
        msg = genLog("NGAMS_ER_DEL_FILE_DB", [diskInfoObj.getDiskId(),
                                              fileId, fileVersion, str(e)])
        error(msg)
    # Remove copy on disk.
    try:
        info(2, "Removing copy on disk, file: %s/%s/%s" %\
             (diskInfoObj.getDiskId(), fileId, str(fileVersion)))
        complFilename = os.path.normpath(diskInfoObj.getMountPoint() + "/" +\
                                         filename)
        msg = "Deleting copy of file: %s/%s/%s: %s"
        info(2, msg % (diskInfoObj.getDiskId(), fileId, str(fileVersion),
                       complFilename))
        rmFile(complFilename)
    except Exception, e:
        msg = "Error removing archived file: %s/%s/%s/%s. Error: %s"
        error(msg % (diskInfoObj.getDiskId(), fileId, str(fileVersion),
                     complFilename), str(e))


def cleanUpCache(srvObj):
    """
    Go through the cache and delete files that have been marked for deletion.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
    T = TRACE()

    # We dump info for all files at once, into a temporary DBM, since during
    # the cleaning up queries will be done in the associated SQLite DBMS.
    sqlQuery = "SELECT disk_id, file_id, file_version, filename " +\
               "FROM ngas_cache WHERE cache_delete = 1"
    cleanUpDbm = createTmpDbm(srvObj, "CLEAN-UP_FILE_INFO")
    try:
        srvObj._cacheContDbmsSem.acquire()
        srvObj._cacheContDbmsCur.execute(sqlQuery)
        while (True):
            fileInfoList = srvObj._cacheContDbmsCur.fetchmany(10000)
            if (not fileInfoList): break
            for sqlFileInfo in fileInfoList:
                cleanUpDbm.addIncKey(sqlFileInfo)
            #res = srvObj._cacheContDbmsCur.fetchone()
            #if (not res): break
            #cleanUpDbm.addIncKey(res)
        srvObj._cacheContDbms.commit()
        srvObj._cacheContDbmsSem.release()
    except Exception, e:
        srvObj._cacheContDbmsSem.release()
        raise Exception, e

    # Now, loop over the entries to delete.
    diskInfoDic = {}
    cleanUpDbm.initKeyPtr()
    while (True):
        key, sqlFileInfo = cleanUpDbm.getNext()
        if (not key): break
        diskId      = sqlFileInfo[0]
        fileId      = sqlFileInfo[1]
        fileVersion = sqlFileInfo[2]
        filename    = sqlFileInfo[3]

        info(2, "Deleting entry from the cache: %s/%s/%s" %\
             (str(sqlFileInfo[0]), str(sqlFileInfo[1]), str(sqlFileInfo[2])))

        # Remove the entry from the cache:
        # - First get the information for the disk hosting the file.
        if (not diskInfoDic.has_key(diskId)):
            diskInfo = srvObj.getDb().getDiskInfoFromDiskId(diskId)
            if (not diskInfo):
                msg = "Illegal Disk ID referenced in Cache Contents " +\
                      "DBMS: %s - ignoring entry"
                warning(msg % diskId)
                delEntryFromCacheDbms(srvObj, diskId, fileId, fileVersion)
                continue
            diskInfoDic[diskId] = ngamsDiskInfo.ngamsDiskInfo().\
                                  unpackSqlResult(diskInfo)
        diskInfoObj = diskInfoDic[diskId]

        #   - Remove from ngas_files (+ update ngas_disks):
        try:
            removeFile(srvObj, diskInfoObj, fileId, fileVersion, filename)
        except Exception, e:
            msg = "Error removing file information from the RDBMS and the " +\
                  "file copy for file %s/%s/%s. Error: %s"
            notice(msg % (diskId, fileId, str(fileVersion), str(e)))

        #   - Remove from Cache Content DBMS's:
        try:
            delEntryFromCacheDbms(srvObj, diskId, fileId, fileVersion)
        except Exception, e:
            msg = "Error removing file information from the Cache Table in " +\
                  "the local DBMS and in the RDBMS for file " +\
                  "%s/%s/%s. Error: %s"
            notice(msg % (diskId, fileId, str(fileVersion), str(e)))

        # TODO: Check if the volume concerned is set to completed and should
        # be marked as uncompleted.


def cacheControlThread(srvObj,
                       dummy):
    """
    The Cache Control Thread runs periodically when the NG/AMS Server is
    Online (if enabled) to synchronize the data holding of the local NGAS
    Cluster against a set of remote NGAS Clusters.

    srvObj:      Reference to server object (ngamsServer).

    dummy:       Needed by the thread handling ...

    Returns:     Void.
    """
    T = TRACE()

    # Don't execute the thread if deactivated in the configuration.
    if (not srvObj.getCachingActive()):
        info(1, "NGAS Cache Service not active - Cache Control Thread " +\
             "terminating with no actions")
        thread.exit()

    # Initialize the Cache Service.
    initCacheArchive(srvObj)

    # Main loop.
    period = srvObj.getCfg().getCachingPeriod()
    while (True):
        startTime = time.time()

        # Incapsulate this whole block to avoid that the thread dies in
        # case a problem occurs, like e.g. a problem with the DB connection.
        try:
            checkStopCacheControlThread(srvObj)
            info(5, "Cache Control Thread starting next iteration ...")

            ###################################################################
            # Business logic of Cache Control Thread
            ###################################################################

            # Check if there are new files to be inserted in the Cache
            # Contents DBMS.
            checkNewFilesDbm(srvObj)

            # Go through local Cache Contents DBMS. Check for each item if it
            # can be deleted.
            checkCacheContents(srvObj)

            # Delete each item, marked for deletion.
            cleanUpCache(srvObj)
            ###################################################################

            ###################################################################
            # Suspend the Cache Control Thread for a while.
            ###################################################################
            suspTime = (period - (time.time() - startTime))
            if (suspTime < 1): suspTime = 1
            info(4, "Cache Control Thread executed - suspending for " +\
                 str(suspTime) + "s ...")
            suspStartTime = time.time()
            while ((time.time() - suspStartTime) < suspTime):
                checkStopCacheControlThread(srvObj)
                time.sleep(0.250)
            ###################################################################

        except Exception, e:
            if (str(e).find(NGAMS_CACHE_CONTROL_THR_STOP) != -1): thread.exit()
            errMsg = "Error occurred during execution of the Cache " +\
                     "Control Thread. Exception: " + str(e)
            alert(errMsg)
            em = traceback.format_exc()
            alert(em)
            # We make a small wait here to avoid that the process tries
            # too often to carry out the tasks that failed.
            time.sleep(5.0)


# EOF