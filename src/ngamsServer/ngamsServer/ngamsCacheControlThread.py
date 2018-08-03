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
import base64
import logging
import os
import time

from six.moves import cPickle # @UnresolvedImport
import sqlite3 as sqlite

from ngamsLib.ngamsCore import rmFile, genLog, loadPlugInEntryPoint
from ngamsLib import ngamsDbCore, ngamsHighLevelLib, ngamsDbm, ngamsDiskInfo, ngamsCacheEntry, ngamsThreadGroup, ngamsLib,\
    utils


logger = logging.getLogger(__name__)

# Various definitions used within this module.

NGAMS_CACHE_CONTROL_THR  = "CACHE-CONTROL-THREAD"

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


class StopCacheControlThreadEx(Exception):
    pass

def checkStopCacheControlThread(stopEvt):
    """
    Used to check if the Cache Control Thread should be stopped and in case
    yes, to stop it.
    """
    if stopEvt.is_set():
        logger.info("Stopping the Cache Control Service")
        raise StopCacheControlThreadEx()

def suspend(stopEvt, t):
    if stopEvt.wait(t):
        raise StopCacheControlThreadEx()

def createCacheDbms(srvObj, ready_evt):
    """
    Creates the internal/local DBMS (SQLite based) used for managing the cache
    contents without constantly accessing the remote NGAS Cache Table in the
    NGAS DB.

    If this DBMS is not existing, it is created and the ngas_cache table
    created as well.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
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
    except Exception as e:
        if (str(e) == "no such table: ngas_cache"):
            logger.info("Table ngas_cache not found in local Cache DBMS - " +\
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

            # Add an index for quicker INSERT/SELECT
            srvObj._cacheContDbmsCur.execute('CREATE INDEX cache_index ON ngas_cache(disk_id, file_id, file_version)')
        else:
            raise

    # Create the DBM to hold information about new files that are registered
    # on this node (to be inserted into the Local Cache Contents DBMS).
    newFilesDbmName = "%s/%s_%s" %\
                      (ngamsHighLevelLib.getNgasChacheDir(srvObj.getCfg()),
                       NGAMS_CACHE_NEW_FILES_DBM, hostId)
    rmFile("%s*" % newFilesDbmName)
    srvObj._cacheNewFilesDbm = ngamsDbm.ngamsDbm(newFilesDbmName,
                                                  cleanUpOnDestr = 0,
                                                  writePerm = 1)
    ready_evt.set()

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
    try:
        srvObj._cacheNewFilesDbmSem.acquire()
        fileInfo = (diskId, fileId, fileVersion, filename)
        fileKey = ngamsLib.genFileKey(diskId, fileId, fileVersion)
        srvObj._cacheNewFilesDbm.add(fileKey, fileInfo)
        srvObj._cacheNewFilesDbmSem.release()
    except Exception as e:
        srvObj._cacheNewFilesDbmSem.release()
        msg = "Error accessing %s DBM. Error: %s"
        raise Exception(msg % (NGAMS_CACHE_NEW_FILES_DBM, str(e)))


def getEntryNewFilesDbm(srvObj):
    """
    Get an entry from the New File DBM.

    srvObj:       Reference to server object (ngamsServer).

    Returns:      Tuple with (<Disk Id>, <File ID>, <File Version>,
                  <Filename>) or None if there are no entried in the DBM
                  (tuple | None).
    """
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
    except Exception as e:
        srvObj._cacheNewFilesDbmSem.release()
        msg = "Error accessing %s DBM. Error: %s"
        raise Exception(msg % (NGAMS_CACHE_NEW_FILES_DBM, str(e)))


def queryCacheDbms(srvObj,
                   sqlQuery):
    """
    Execute an SQL query in the local DBMS and return the result.

    srvObj:       Reference to server object (ngamsServer).

    sqlQuery:     SQL query to return (string).

    Returns:      Result set (tuple with tuples).
    """
    try:
        srvObj._cacheContDbmsSem.acquire()
        sqlQuery += ";"
        logger.debug("Performing SQL query (Cache DBMS): %s", sqlQuery)
        srvObj._cacheContDbmsCur.execute(sqlQuery)
        srvObj._cacheContDbms.commit() # TODO: Investigate this.
        res = srvObj._cacheContDbmsCur.fetchall()
        srvObj._cacheContDbmsSem.release()
        logger.debug("Result of SQL query  (Cache DBMS) (%s): %s", sqlQuery, str(res))
        return res
    except:
        srvObj._cacheContDbmsSem.release()
        raise


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
        cacheEntryObjPickleEnc = utils.b2s(base64.b32encode(cacheEntryObjPickle))
        sqlQuery = _ADD_ENTRY_IN_CACHE_DBMS %\
                   (diskId, fileId, int(fileVersion), filename, fileSize,
                    delete, timeNow, timeNow, cacheEntryObjPickleEnc)
        queryCacheDbms(srvObj, sqlQuery)

    if (addInRdbms):
        # Insert entry in the remote DBMS.
        try:
           srvObj.getDb().insertCacheEntry(diskId, fileId, fileVersion, timeNow, delete)
        except:
           pass


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
    cacheEntryObjPickle = cPickle.dumps(cacheEntryObj)
    cacheEntryObjPickleEnc = utils.b2s(base64.b32encode(cacheEntryObjPickle))
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
    # Remove from the Local Cache Contents DBMS.
    sqlQuery = _DEL_ENTRY_FROM_CACHE_DBMS_QUERY %\
               (diskId, fileId, int(fileVersion))
    queryCacheDbms(srvObj, sqlQuery)

    # Remove from the Remote Cache Contents DBMS.
    srvObj.getDb().deleteCacheEntry(diskId, fileId, fileVersion)


def initCacheArchive(srvObj, stopEvt, ready_evt):
    """
    Initialize the NGAS Cache Archive Service. If there are requests in the
    Cache Table in the DB, these are read out and inserted in the local
    Cache Contents DBM.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
    # Create/open the Cache Contents DBM.
    # Note: This DBMS is kept between sessions for efficiency reasons.
    createCacheDbms(srvObj, ready_evt)

    # Check if all files registered in the RDBMS NGAS Cache Table are
    # registered in the Local Cache DBMS.
    for sqlFileInfo in srvObj.db.getCacheContents(srvObj.getHostId()):
        diskId      = sqlFileInfo[0]
        fileId      = sqlFileInfo[1]
        fileVersion = int(sqlFileInfo[2])
        if (entryInCacheDbms(srvObj, diskId, fileId, fileVersion)):
            continue
        # Set filename, file size and Cache Entry Object later.
        addEntryInCacheDbms(srvObj, diskId, fileId, fileVersion, "", -1,
                            cacheEntryObj = "", addInRdbms = True)

    # Update the local Cache Content DBMS with the information about files
    # online on this node.
    files = srvObj.getDb().getFileSummary1(hostId = srvObj.getHostId(),
                                           fileStatus = [], order = False)
    for sqlFileInfo in files:
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
                ingDateSecs = srvObj.getDb().\
                              getIngDate(diskId, fileId, fileVersion)
                cacheEntryObj = ngamsCacheEntry.ngamsCacheEntry().\
                                unpackSqlInfo(sqlFileInfo).\
                                setLastCheck(time.time()).\
                                setCacheTime(ingDateSecs)
                setCacheEntryObjectCacheDbms(srvObj, cacheEntryObj)
                continue

        # Add new entry in the DBMS'.
        ingDateSecs = srvObj.getDb().\
                      getIngDate(diskId, fileId, fileVersion)
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
            parameters = (srvObj, stopEvt)
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
            logger.error(msg)

        # Add the new entry.
        logger.debug("Adding new entry in Cache DBMS: %s/%s/%s",
                     fileInfo[NGAMS_CACHE_DISK_ID], fileInfo[NGAMS_CACHE_FILE_ID],
                     fileInfo[NGAMS_CACHE_FILE_VER])
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

    try:
        check_can_be_deleted = int(srvObj.getCfg().getVal("Caching[1].CheckCanBeDeleted"))
        if not check_can_be_deleted:
            return
    except:
        return

    diskId, fileId, fileVersion = sqlFileInfo
    srvObj.db.set_available_for_deletion(fileId, fileVersion, diskId)


def scheduleFileForDeletion(srvObj,
                            sqlFileInfo):
    """
    Schedule a file for deletion from the cache.

    srvObj:       Reference to server object (ngamsServer).

    sqlFileInfo:  Information for one file as queried from the NGAS Cache
                  Table (list).

    Returns:      Void.
    """
    diskId      = sqlFileInfo[NGAMS_CACHE_DISK_ID]
    fileId      = sqlFileInfo[NGAMS_CACHE_FILE_ID]
    fileVersion = int(sqlFileInfo[NGAMS_CACHE_FILE_VER])
    msg = "Scheduling entry %s/%s/%s for deletion from the " +\
          "NGAS Cache Archive"
    logger.info(msg, diskId, fileId, str(fileVersion))
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
    try:
        srvObj._cacheCtrlPiThreadGr.takeGenMux()
        writeIdx = srvObj._cacheCtrlPiDbm.get(NGAMS_CACHE_CTRL_PI_DBM_WR)
        srvObj._cacheCtrlPiDbm.add(str(writeIdx), cacheEntryObj)
        writeIdx = ((writeIdx + 1) % NGAMS_CACHE_CTRL_PI_DBM_MAX)
        srvObj._cacheCtrlPiDbm.add(NGAMS_CACHE_CTRL_PI_DBM_WR, writeIdx)
        srvObj._cacheCtrlPiThreadGr.releaseGenMux()
    except:
        srvObj._cacheCtrlPiThreadGr.releaseGenMux()
        raise


def _getEntryCacheCtrlPlugInDbm(srvObj):
    """
    Get an entry from the Cache Control Plug-In DBM.

    srvObj:       Reference to server object (ngamsServer).

    Returns:      Next Sync. Request Object or None if there are no requests
                  in the DBM (ngamsCacheEntry | None).
    """
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
    except:
        srvObj._cacheCtrlPiThreadGr.releaseGenMux()
        raise


def _getCountCacheCtrlPlugInDbm(srvObj):
    """
    Get the current count (=number of elements in the

    srvObj:       Reference to server object (ngamsServer).

    Returns:      Next Sync. Request Object or None if there are no requests
                  in the DBM (ngamsCacheEntry | None).
    """
    try:
        srvObj._cacheCtrlPiThreadGr.takeGenMux()
        noOfEls = srvObj._cacheCtrlPiDbm.getCount()
        srvObj._cacheCtrlPiThreadGr.releaseGenMux()

        return noOfEls
    except:
        srvObj._cacheCtrlPiThreadGr.releaseGenMux()
        raise


def _cacheCtrlPlugInThread(threadGrObj):
    """
    Function to run as a thread to check if there are entries in the
    Cache Control Plug-In DBM to check for file validity.

    threadGrObj:  Reference to Thread Group Object to which this thread
                  belongs (ngamsThreadGroup).

    Returns:      Void.
    """
    srvObj, stopEvt = threadGrObj.getParameters()

    # Load the plug-in module.
    cacheCtrlPlugIn = srvObj.getCfg().getVal("Caching[1].CacheControlPlugIn")
    plugInMethod = loadPlugInEntryPoint(cacheCtrlPlugIn)

    deleteMsg = "CACHE-CRITERIA: Plug-in Selected File for Deletion: %s/%s/%s"

    # Loop until instructed to stop.
    while True:
        try:

            suspend(stopEvt, 0.5)

            # Get the next Cache Entry Object (if there are any queued).
            while True:
                checkStopCacheControlThread(stopEvt)
                cacheEntryObj = _getEntryCacheCtrlPlugInDbm(srvObj)
                if not cacheEntryObj:
                    break

                # Invoke Cache Control Plug-In on the file.
                try:
                    deleteFile = plugInMethod(srvObj, cacheEntryObj)
                    if (deleteFile):
                        logger.info(deleteMsg, cacheEntryObj.getDiskId(),
                                             cacheEntryObj.getFileId(),
                                             str(cacheEntryObj.getFileVersion()))
                        srvObj._cacheCtrlPiDelDbm.addIncKey(cacheEntryObj)
                    else:
                        srvObj._cacheCtrlPiFilesDbm.addIncKey(cacheEntryObj)
                except Exception:
                    logger.exception("Error occurred in thread")
                    # Put the entry in the queue to make it stay in the system
                    # still.
                    srvObj._cacheCtrlPiFilesDbm.addIncKey(cacheEntryObj)
        except StopCacheControlThreadEx:
            break
       

def checkIfFileCanBeDeleted(srvObj, fileId, fileVersion, diskId):
    """
    Check if the file can be deleted from its file_status flag
    """
    fileStatus = srvObj.getDb().getFileStatus(fileId, fileVersion, diskId)

    re = bin(int(fileStatus, 2) & CACHE_DEL_BIT_MASK_INT)[2:] # logic AND, and remove the '0b', e.g '0b11001' --> '11001'
    re = re.zfill(8) # fill zeroes at the beginning, e.g. '100' --> '00000100'

    return (CACHE_DEL_BIT_MASK == re)


def checkCacheContents(srvObj, stopEvt, check_can_be_deleted):
    """
    Go through the contents in the cache and check for each item not already
    marked for deletion, if it can be deleted. If it can be deleted, mark it
    for deletion.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
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
    # 4. Execute the Cache Control Plug-In (if specified in the
    #    configuration).

    # 0. Go through the explicitDel queue to remove files

    # 1. Evaluate if there are files residing in the cache for more than
    #    the specified amount of time.
    if (srvObj.getCfg().getVal("Caching[1].MaxTime")):
        logger.debug("Applying criteria: Expired files ...")
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
                    logger.info("CACHE-CRITERIA: Maximum Time Expired: %s/%s/%s",
                          sqlFileInfo[NGAMS_CACHE_DISK_ID],
                          sqlFileInfo[NGAMS_CACHE_FILE_ID],
                          str(sqlFileInfo[NGAMS_CACHE_FILE_VER]))
                    delFilesDbm.addIncKey(sqlFileInfo)
            srvObj._cacheContDbms.commit()
            srvObj._cacheContDbmsSem.release()
        except:
            srvObj._cacheContDbmsSem.release()
            raise

        # Now, loop over the selected files and mark them for deletion.
        delFilesDbm.initKeyPtr()
        while (True):
            key, sqlFileInfo = delFilesDbm.getNext()
            if (not key): break
            scheduleFileForDeletion(srvObj, sqlFileInfo)
            markFileChecked(srvObj, sqlFileInfo)

        del delFilesDbm
        delFilesDbm = None

    # 2. Remove files if there more files (in volume) in the cache than the
    #    specified threshold.
    if (srvObj.getCfg().getVal("Caching[1].MaxCacheSize")):
        logger.debug("Applying criteria: Maximum cache size ...")
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
        logger.debug(msg, (float(cacheSum) / 1e9),
                           (float(maxCacheSize) / 1e9))

        if (cacheSum > maxCacheSize):
            msg = "Current size of cache: %.6f MB exceeding specified " +\
                  "threshold: %.6f MB"
            logger.debug(msg, (float(cacheSum) / 1e6),
                           (float(maxCacheSize) / 1e6))
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
                        if (check_can_be_deleted):
                            try:
                                if (not checkIfFileCanBeDeleted(srvObj,
                                                                sqlFileInfo[NGAMS_CACHE_FILE_ID],
                                                                sqlFileInfo[NGAMS_CACHE_FILE_VER],
                                                                sqlFileInfo[NGAMS_CACHE_DISK_ID])):
                                    logger.info("Cannot delete file from the cache: %s/%s/%s",
                                          str(sqlFileInfo[0]), str(sqlFileInfo[1]), str(sqlFileInfo[2]))
                                    continue
                            except Exception as cee:
                                if (str(cee).lower().find('file not found in ngas db') > -1):
                                    logger.warning("file already gone, still mark for deletion: %s/%s/%s",
                                            str(sqlFileInfo[0]), str(sqlFileInfo[1]), str(sqlFileInfo[2]))
                                else:
                                    raise

                        msg = "CACHE-CRITERIA: Maximum Cache Size " +\
                              "Exceeded: %s/%s/%s"
                        logger.info(msg,
                              sqlFileInfo[NGAMS_CACHE_DISK_ID],
                              sqlFileInfo[NGAMS_CACHE_FILE_ID],
                              str(sqlFileInfo[NGAMS_CACHE_FILE_VER]))
                        delFilesDbm.addIncKey(sqlFileInfo)
                        fileSize = int(sqlFileInfo[NGAMS_CACHE_FILE_SIZE])
                        cacheSum -= fileSize
                        if (cacheSum < maxCacheSize): break
                srvObj._cacheContDbms.commit()
                srvObj._cacheContDbmsSem.release()
            except:
                srvObj._cacheContDbmsSem.release()
                raise

            # Now, loop over the selected files and mark them for deletion.
            delFilesDbm.initKeyPtr()
            while (True):
                key, sqlFileInfo = delFilesDbm.getNext()
                if (not key): break
                scheduleFileForDeletion(srvObj, sqlFileInfo)
                markFileChecked(srvObj, sqlFileInfo)

            del delFilesDbm
            delFilesDbm = None

    # 3. Remove files if there are more files in the cache than the
    #    specified threshold.
    if (srvObj.getCfg().getVal("Caching[1].MaxFiles")):
        logger.debug("Applying criteria: Maximum number of files ...")
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
                        logger.info(msg,
                              sqlFileInfo[NGAMS_CACHE_DISK_ID],
                              sqlFileInfo[NGAMS_CACHE_FILE_ID],
                              str(sqlFileInfo[NGAMS_CACHE_FILE_VER]))
                        delFilesDbm.addIncKey(sqlFileInfo)
                        count += 1
                        if (count >= noOfFilesToRemove): break
                srvObj._cacheContDbms.commit()
                srvObj._cacheContDbmsSem.release()
            except:
                srvObj._cacheContDbmsSem.release()
                raise

            # Now, marked the selected files for deletion.
            delFilesDbm.initKeyPtr()
            while (True):
                key, sqlFileInfo = delFilesDbm.getNext()
                if (not key): break
                scheduleFileForDeletion(srvObj, sqlFileInfo)
                markFileChecked(srvObj, sqlFileInfo)
            del delFilesDbm
            delFilesDbm = None

    # 4. Invoke the Cache Control Plug-In (if specified) on the files.
    if (srvObj.getCfg().getVal("Caching[1].CacheControlPlugIn")):
        logger.debug("Applying criteria: Cache Control Plug-In ...")

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
        except:
            srvObj._cacheContDbmsSem.release()
            raise

        # Wait for all entries in the Cache Control Plug-In DBM to be handled.
        while (True):
            count = _getCountCacheCtrlPlugInDbm(srvObj)
            if count == 2:
                break
            suspend(stopEvt, 0.25)

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
    # Remove the entry from the DB. This includes updating the NGAS Disks
    # Table.
    try:
        logger.debug("Deleting file information from DB for file: %s/%s/%s",
             diskInfoObj.getDiskId(), fileId, str(fileVersion))
        srvObj.getDb().deleteFileInfo(srvObj.getHostId(), diskInfoObj.getDiskId(), fileId,
                                      fileVersion)
    except Exception as e:
        msg = genLog("NGAMS_ER_DEL_FILE_DB", [diskInfoObj.getDiskId(),
                                              fileId, fileVersion, str(e)])
        logger.error(msg)
    # Remove copy on disk.
    try:
        logger.debug("Removing copy on disk, file: %s/%s/%s",
             diskInfoObj.getDiskId(), fileId, str(fileVersion))
        complFilename = os.path.normpath(diskInfoObj.getMountPoint() + "/" +\
                                         filename)
        msg = "Deleting copy of file: %s/%s/%s: %s"
        logger.debug(msg, diskInfoObj.getDiskId(), fileId, str(fileVersion),
                       complFilename)
        rmFile(complFilename)
    except Exception:
        msg = "Error removing archived file: %s/%s/%s/%s. Error: %s"
        logger.exception(msg, diskInfoObj.getDiskId(), fileId, str(fileVersion),
                     complFilename)


def cleanUpCache(srvObj):
    """
    Go through the cache and delete files that have been marked for deletion.

    srvObj:     Reference to server object (ngamsServer).

    Returns:    Void.
    """
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
    except:
        srvObj._cacheContDbmsSem.release()
        raise

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

        logger.info("Deleting entry from the cache: %s/%s/%s",
             str(sqlFileInfo[0]), str(sqlFileInfo[1]), str(sqlFileInfo[2]))

        # Remove the entry from the cache:
        # - First get the information for the disk hosting the file.
        if diskId not in diskInfoDic:
            diskInfo = srvObj.getDb().getDiskInfoFromDiskId(diskId)
            if (not diskInfo):
                msg = "Illegal Disk ID referenced in Cache Contents " +\
                      "DBMS: %s - ignoring entry"
                logger.warning(msg, diskId)
                delEntryFromCacheDbms(srvObj, diskId, fileId, fileVersion)
                continue
            diskInfoDic[diskId] = ngamsDiskInfo.ngamsDiskInfo().\
                                  unpackSqlResult(diskInfo)
        diskInfoObj = diskInfoDic[diskId]

        #   - Remove from ngas_files (+ update ngas_disks):
        try:
            removeFile(srvObj, diskInfoObj, fileId, fileVersion, filename)
        except:
            msg = "Error removing file information from the RDBMS and the " +\
                  "file copy for file %s/%s/%s"
            logger.exception(msg, diskId, fileId, str(fileVersion))

        #   - Remove from Cache Content DBMS's:
        try:
            delEntryFromCacheDbms(srvObj, diskId, fileId, fileVersion)
        except:
            msg = "Error removing file information from the Cache Table in " +\
                  "the local DBMS and in the RDBMS for file " +\
                  "%s/%s/%s"
            logger.exception(msg, diskId, fileId, str(fileVersion))

        # TODO: Check if the volume concerned is set to completed and should
        # be marked as uncompleted.


def cacheControlThread(srvObj, stopEvt, ready_evt, check_can_be_deleted):
    """
    The Cache Control Thread runs periodically when the NG/AMS Server is
    Online (if enabled) to synchronize the data holding of the local NGAS
    Cluster against a set of remote NGAS Clusters.
    """

    # Initialize the Cache Service.
    try:
        initCacheArchive(srvObj, stopEvt, ready_evt)
    except:
        if not ready_evt.is_set():
            ready_evt.set()
        raise

    # Main loop.
    period = srvObj.getCfg().getCachingPeriod()
    while True:
        startTime = time.time()

        # Encapsulate this whole block to avoid that the thread dies in
        # case a problem occurs, like e.g. a problem with the DB connection.
        try:
            checkStopCacheControlThread(stopEvt)
            logger.debug( "Cache Control Thread starting next iteration ...")

            ###################################################################
            # Business logic of Cache Control Thread
            ###################################################################

            # Check if there are new files to be inserted in the Cache
            # Contents DBMS.
            checkNewFilesDbm(srvObj)

            # Go through local Cache Contents DBMS. Check for each item if it
            # can be deleted.
            checkCacheContents(srvObj, stopEvt, check_can_be_deleted)

            # Delete each item, marked for deletion.
            cleanUpCache(srvObj)
            ###################################################################

            ###################################################################
            # Suspend the Cache Control Thread for a while.
            ###################################################################
            suspTime = (period - (time.time() - startTime))
            if (suspTime < 1): suspTime = 1
            logger.debug("Cache Control Thread executed - suspending for %d s ...", suspTime)
            suspend(stopEvt, suspTime)

        except StopCacheControlThreadEx:
            break
        except Exception:
            errMsg = "Error occurred during execution of the Cache " +\
                     "Control Thread"
            logger.exception(errMsg)
            try:
                suspend(stopEvt, 5)
            except StopCacheControlThreadEx:
                break