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
# "@(#) $Id: ngamsDataCheckThread.py,v 1.13 2010/03/25 14:47:59 jagonzal Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  29/01/2002  Created
#
"""
This module contains the code for the Data Check Thread, which is used
to check the data holding in connection with one NGAS host.
"""

import os, time, thread, threading, random, glob, cPickle

from pccUt import PccUtTime
import ngamsFileUtils
from ngamsLib.ngamsCore import TRACE, info, NGAMS_DATA_CHECK_THR, error, \
    NGAMS_CACHE_DIR, checkCreatePath, isoTime2Secs, iso8601ToSecs, \
    rmFile, genLog, mvFile, NGAMS_DISK_INFO, NGAMS_VOLUME_ID_FILE, \
    NGAMS_VOLUME_INFO_FILE, NGAMS_STAGING_DIR, warning, NGAMS_NOTIF_DATA_CHECK, \
    logFlush
from ngamsLib import ngamsNotification, ngamsDiskInfo
from ngamsLib import ngamsDbCore, ngamsDbm, ngamsHighLevelLib, ngamsLib


class StopDataCheckThreadException(Exception):
    pass

def _finishThread(srvObj):
    info(2,"Stopping Data Check Thread")
    srvObj.updateHostInfo(None, None, None, None, None, None, 0, None)
    raise StopDataCheckThreadException()

def _stopDataCheckThr(srvObj, stopEvt):
    """
    Checks whether the thread should finish or not
    """
    if stopEvt.isSet():
        _finishThread(srvObj)

def suspend(srvObj, stopEvt, t):
    """
    Sleeps for ``t`` seconds, unless the thread is signaled to stop
    """
    if stopEvt.wait(t):
        _finishThread(srvObj)

# TODO: Variables used for the handling of the execution of the DCC within
# this module. Could be made members of the ngamsServer class, but since they
# are only used locally, they are defined as global within this module
# in order not to complicate the ngamsServer class.
# Alternatively, one global dictionary could be used with keys:
# "_diskDic" -> "DiskDic", "_dbmObjDic" -> "DbmObjDic", ...
_diskDic         = {}    # Dictionary with info about disks concerned.
_dbmObjDic       = {}    # Dictionary with Queue/Error DBM objects.
_diskIdList      = []    # List of Disk IDs ordered for the checking.
_diskSchedDic    = {}    # Dictionary used when scheduling disks to be checked.
_fileRefDbm      = {}    # Points to DBM with references to all files on disks.
_reqFileInfoSem  = threading.Semaphore(1)  # Semaphore for critical access.
_runSubThread    = 1     # Flag to signal to sub-threads to run or not.

# Parameters for statistics.
_statCheckSem     = threading.Semaphore(1)  # Semaphore to protect checking.
_statLastDbUpdate = 0        # Time (secs since epoch) for last DB update.
_statCheckStart   = None     # Start time for checking (secs since epoch).
_statCheckRemain  = None     # Remaining time in seconds.
_statCheckRate    = None     # Speed of the data checking (MB/s).
_statCheckMb      = None     # Amount of data to check (MB).
_statCheckedMb    = None     # Amount of data checked (MB).
_statCheckFiles   = None     # Number of files to check.
_statCheckCount   = None     # Number of files checked.


def _getDiskDic():
    """
    Return refrence to the Disk Dictionary with information about the disks
    concerned.

    Returns:    Disk Dictionary (dictionary).
    """
    global _diskDic
    return _diskDic


def _resetDiskDic():
    """
    Reset the Disk Dictionary.

    Returns:   Void.
    """
    global _diskDic
    _diskDic = {}


def _getDbmObjDic():
    """
    Return reference to Queue/Error DBM Objects Dictionary.

    Returns:    Reference to DBM Objects Dictionary (dictionary).
    """
    global _dbmObjDic
    return _dbmObjDic


def _resetDbmObjDic():
    """
    Reset the DBM Object Dictionary.

    Returns:   Void.
    """
    global _dbmObjDic
    _dbmObjDic = {}


def _getDiskIdList():
    """
    Return reference to Queue/Error DBM Disk ID List.

    Returns:    Reference to DBM Slot List (list).
    """
    global _diskIdList
    return _diskIdList


def _resetDiskIdList():
    """
    Reset the Disk ID List.

    Returns:    Void.
    """
    global _diskIdList
    _diskIdList = []


def _getDiskSchedDic():
    """
    Return reference to the Disk Scheduling Dictionary.

    Returns:    Reference to DBM Slot List (list).
    """
    global _diskSchedDic
    return _diskSchedDic


def _resetDiskSchedDic():
    """
    Reset the Disk Scheduling Dictionary.

    Returns:    Void.
    """
    global _diskSchedDic
    _diskSchedDic = {}


def _setFileRefDbm(dbmObj):
    """
    Set the File Reference Object.

    dbmObj:    DBM object (ngamsDbm).

    Returns:   Void.
    """
    global _fileRefDbm
    _fileRefDbm = dbmObj


def _getFileRefDbm():
    """
    Return reference to DBM containing references to all files on the
    storage disks.

    Returns:   Reference to DBM object (ngamsDbm).
    """
    global _fileRefDbm
    return _fileRefDbm


def _resetFileRefDbm():
    """
    Reset the File Reference DBM.

    Returns:   Void.
    """
    global _fileRefDbm
    if (_fileRefDbm): del _fileRefDbm
    _fileRefDbm = {}


def _takeReqFileInfoSem():
    """
    Take the semphore protecting the access to the file info DBMs.

    Returns:    Void.
    """
    global _reqFileInfoSem
    _reqFileInfoSem.acquire()


def _relReqFileInfoSem():
    """
    Release the semphore protecting the access to the file info DBMs.

    Returns:    Void.
    """
    global _reqFileInfoSem
    _reqFileInfoSem.release()


def _initFileCheckStatus(srvObj,
                         amountMb,
                         noOfFiles):
    """
    Initialize the checking parameters.

    srvObj:         Reference to instance of ngamsServer object (ngamsServer).

    amountMb:       Amount of data to check (MB) (float).

    noOfFiles:      Number of files to check (integer).

    Returns:        Void.
    """
    T = TRACE()

    global _statCheckSem, _statLastDbUpdate, _statCheckStart,\
           _statCheckRemain, _statCheckRate, _statCheckMb, _statCheckedMb,\
           _statCheckFiles, _statCheckCount
    try:
        _statCheckSem.acquire()
        _statCheckStart   = time.time()
        _statCheckRemain  = 0
        statEstimTime     = 0
        _statCheckRate    = 0.0
        _statCheckMb      = amountMb
        _statCheckedMb    = 0.0
        _statCheckFiles   = noOfFiles
        _statCheckCount   = 0
        _statLastDbUpdate = 0
        _statCheckSem.release()
    except Exception, e:
        _statCheckSem.release()
        error("Exception caught in Data Checking Thread: " + str(e))
    srvObj.getDb().updateDataCheckStat(srvObj.getHostId(), _statCheckStart,
                                       _statCheckRemain, statEstimTime,
                                       _statCheckRate, _statCheckMb,
                                       _statCheckedMb, _statCheckFiles,
                                       _statCheckCount)


def _updateFileCheckStatus(srvObj,
                           fileSize,
                           diskId,
                           fileId,
                           fileVersion,
                           report,
                           force = 0):
    """
    Update the status of the DCC.

    srvObj:       Reference to instance of ngamsServer object (ngamsServer).

    fileSize:     Size of file (in bytes) that was checked (integer)

    diskId:       ID of disk hosting file checked (string).

    fileId:       ID of file concerned (string).

    fileVersion:  Version of file concered (integer).

    report:       List containing the result of the file checking.
                  Refer to documentation for ngamsFileUtils.checkFile()
                  for futher information (list).

    force:        If set to 1 a DB update will be forced (integer/0|1).

    Returns:      Void.
    """
    T = TRACE(5)

    global _statCheckSem, _statLastDbUpdate, _statCheckStart,\
           _statCheckRemain, _statCheckRate, _statCheckMb, _statCheckedMb,\
           _statCheckFiles, _statCheckCount
    try:
        _statCheckSem.acquire()
        timeNow = time.time()

        # Calculate the new values.
        if (fileId):
            _statCheckedMb  += (float(fileSize) / 1048576.0)
            _statCheckCount += 1

        checkTime        = (timeNow - _statCheckStart)
        _statCheckRate   = (float(_statCheckedMb) / float(checkTime))
        if ( _statCheckRate > 0):
            _statCheckRemain = int((float(_statCheckMb - _statCheckedMb) /
                                    _statCheckRate) + 0.5)
            statEstimTime    = int(float(_statCheckMb / _statCheckRate) + 0.5)
        else:
            _statCheckRemain = 0
            statEstimTime    = 0

        # Update DB only every 10s.
        if (force or ((timeNow - _statLastDbUpdate) >= 10)):
            srvObj.getDb().updateDataCheckStat(srvObj.getHostId(), _statCheckStart,
                                               _statCheckRemain, statEstimTime,
                                               _statCheckRate, _statCheckMb,
                                               _statCheckedMb, _statCheckFiles,
                                               _statCheckCount)
            _statLastDbUpdate = timeNow

        # Update report if an error was found.
        if (diskId and report):
            fileKey = ngamsLib.genFileKey(None, fileId, fileVersion)
            _getDbmObjDic()[diskId][1].add(fileKey, report)

        statFormat = "DCC Status: Time Remaining (s): %d, " +\
                     "Rate (MB/s): %.3f, " +\
                     "Volume/Checked (MB): %.3f/%.3f, Files/Checked: %d/%d"
        info(4,statFormat % (_statCheckRemain, _statCheckRate, _statCheckMb,
             _statCheckedMb, _statCheckFiles, _statCheckCount))
        _statCheckSem.release()
    except Exception, e:
        error("Data Consistency Checking: Encountered error: " + str(e))
        _statCheckSem.release()


def _setRunPermSubThread(perm):
    """
    Set the run permission for the DCC Sub-Thread.

    perm:     Permission: 1 = run, 0 = stop (integer/0|1).

    Returns:  Void.
    """
    global _runSubThread
    _runSubThread = perm


def _getRunPermSubThread():
    """
    Get the run permission for the DCC Sub-Thread.

    Returns:     Permission to tun: 1 = run, 0 = stop (integer/0|1).
    """
    global _runSubThread
    return _runSubThread


def _stopCheckingSubThread():
    """
    Used by the Data Consistency Checking Sub-Threads to check if they are
    allowed to execute.

    Returns:   Void.
    """
    if (not _getRunPermSubThread()):
        info(3,"DCC Sub-Thread exiting ...")
        thread.exit()


def _suspend(srvObj,
             baseTime = 0.010):
    """
    Determine the time the thread should suspend itself according to
    the load, priority etc. + suspend for the given time.

    srvObj:      Reference to instance of ngamsServer object (ngamsServer).

    baseTime:    Time used to calculate the suspension time (float).

    Returns:     Void.
    """
    suspTime = (srvObj.getCfg().getDataCheckPrio() * baseTime)
    time.sleep(suspTime)


def _dumpFileInfo(srvObj, tmpFilePat, stopEvt):
    """
    Function that dumps the information about the files. One DBM is created
    per disk. This is named:

       <Mount Root Point>/cache/DATA-CHECK-THREAD_QUEUE_<Disk ID>.bsddb

    If problems are found for a file, these are stored in DBM files named:

       <Mount Root Point>/cache/DATA-CHECK-THREAD_ERRORS_<Disk ID>.bsddb

    A DBM is also created with references to all files found on the system.
    The name of this is of the form:

       <Mount Root Point>/cache/DATA-CHECK-THREAD_FILES_<Host ID>.bsddb

    Latter is always generated from scratch.


    The function handles the DBM files in the following way:

       1. Get the information about the disks in this system.

       2. Check for each DBM file found, if this disk is still in the system.
          If not, the Queue and Error DBM files are removed.

       3. Go through the list of disks in the system. If they don't have
          the two DBM files listed above, these are initialized. The file
          information for all the files stored on the disk is dumped into
          the Queue DBM file. Only files marked to be ignored are not dumped.

       4. Finally, build up a DBM with references to all files found
          on this system

    srvObj:       Reference to server object (ngamsServer).

    tmpFilePat:   Pattern for temporary files (string).

    Returns:      Void.
    """
    T = TRACE()

    cacheDir = srvObj.getCfg().getRootDirectory() + "/" + NGAMS_CACHE_DIR
    checkCreatePath(os.path.normpath(cacheDir))

    ###########################################################################
    # Get information about all disks in the system. The disks are only
    # added if 'ripe' for checking.
    ###########################################################################
    info(4,"Get information about all disks mounted in this system ...")
    slotIdList = srvObj.getDb().getSlotIdsMountedDisks(srvObj.getHostId())
    diskListRaw = srvObj.getDb().\
                  getDiskInfoForSlotsAndHost(srvObj.getHostId(), slotIdList)
    minCycleTime = isoTime2Secs(srvObj.getCfg().getDataCheckMinCycle())
    _resetDiskDic()
    lastDiskCheckDic = {}
    for diskInfo in diskListRaw:
        _stopDataCheckThr(srvObj, stopEvt)
        tmpDiskInfoObj = ngamsDiskInfo.ngamsDiskInfo().\
                         unpackSqlResult(diskInfo)
        if (not tmpDiskInfoObj.getLastCheck()):
            lastCheck = 0
        else:
            lastCheck = iso8601ToSecs(tmpDiskInfoObj.getLastCheck())
        if ((time.time() - lastCheck) >= minCycleTime):
            if (lastCheck):
                if (not lastDiskCheckDic.has_key(lastCheck)):
                    lastDiskCheckDic[lastCheck] = tmpDiskInfoObj
                else:
                    lastDiskCheckDic[lastCheck + len(lastDiskCheckDic)] =\
                                               tmpDiskInfoObj
            else:
                lastDiskCheckDic[time.time()] = tmpDiskInfoObj
            _getDiskDic()[tmpDiskInfoObj.getDiskId()] = tmpDiskInfoObj
    info(4,"Got information about all disks mounted in this system")
    ###########################################################################

    ###########################################################################
    # Loop over the Queue/Error DBM files found, check if the disk is
    # still in the system/scheduled for checking.
    ###########################################################################
    info(4,"Loop over/check existing Queue/Error DBM Files ...")
    dbmFileList = glob.glob(cacheDir + "/" + NGAMS_DATA_CHECK_THR +\
                            "_QUEUE_*.bsddb")
    for dbmFile in dbmFileList:
        _stopDataCheckThr(srvObj, stopEvt)
        diskId = dbmFile.split("_")[-1].split(".")[0]
        if (not _getDiskDic().has_key(diskId)):
            filePat = "%s/%s*%s.bsddb" % (cacheDir,NGAMS_DATA_CHECK_THR,diskId)
            rmFile(filePat)
        else:
            # Add references to Queue/Error DBM.
            queueDbmFile = "%s/%s_QUEUE_%s.bsddb" %\
                           (cacheDir, NGAMS_DATA_CHECK_THR, diskId)
            queueDbm = ngamsDbm.ngamsDbm(queueDbmFile, 0, 1)
            errorDbmFile = "%s/%s_ERRORS_%s.bsddb" %\
                           (cacheDir, NGAMS_DATA_CHECK_THR, diskId)
            errorDbm = ngamsDbm.ngamsDbm(errorDbmFile, 0, 1)
            _getDbmObjDic()[diskId] = (queueDbm, errorDbm)
    info(4,"Looped over/checked existing Queue/Error DBM Files")
    ###########################################################################

    ###########################################################################
    # Loop over the disks mounted in this system and check if they have a
    # Queue/Error DBM file. In case the DBM files are not available, create
    # these.
    ###########################################################################
    info(4,"Create DBM files for disks to be checked ...")
    startDbFileRd = time.time()
    for diskId in _getDiskDic().keys():
        _stopDataCheckThr(srvObj, stopEvt)
        if (_getDbmObjDic().has_key(diskId)): continue

        # The disk is ripe for checking but still has no Queue/Error DBM
        # DBs allocated.
        queueDbmFile = "%s/%s_QUEUE_%s.bsddb" %\
                       (cacheDir, NGAMS_DATA_CHECK_THR, diskId)
        tmpQueueDbmFile = tmpFilePat + "_" + os.path.basename(queueDbmFile)
        queueDbm = ngamsDbm.ngamsDbm(tmpQueueDbmFile, 0, 1)

        # Get the theoretical number of files.
        noOfFilesList = []
        accuNoOfFiles = 0
        for n in range(3):
            noOfFilesList.append(srvObj.getDb().getNumberOfFiles(diskId,
                                                                 ignore=0))
            accuNoOfFiles += int(noOfFilesList[0])
            time.sleep(0.1)
        if ((3 * int(noOfFilesList[0])) != accuNoOfFiles):
            errMsg = "Problem querying number of files: %d/%d/%d" %\
                     (noOfFilesList[0], noOfFilesList[1], noOfFilesList[2])
            errMsg = genLog("NGAMS_ER_DB_COM", [errMsg])
            raise Exception, errMsg
        expNoOfFiles = noOfFilesList[0]

        # Now, retrieve the files on the given disk, and store the info
        # in the Queue DBM file.
        actNoOfFiles = 0
        # TODO: Use ngamsDb.dumpFileSummary1().
        cursorObj = srvObj.getDb().getFileSummary1(None, [diskId], [],
                                                   ignore=0, fileStatus=[],
                                                   lowLimIngestDate=None,
                                                   order=0)
        fetchSize = 1000
        while (1):
            _stopDataCheckThr(srvObj, stopEvt)
            try:
                fileList = cursorObj.fetch(fetchSize)
            except Exception, e:
                # Assume a problem like e.g. a broken DB connection.
                errMsg = "Problem encountered while dumping file info. " +\
                         "Error: " + str(e)
                error(errMsg)
                del queueDbm
                rmFile(tmpQueueDbmFile)
                raise Exception, genLog("NGAMS_ER_DB_COM", [errMsg])

            if (not fileList): break
            actNoOfFiles += len(fileList)
            for fileInfo in fileList:
                fileId  = fileInfo[ngamsDbCore.SUM1_FILE_ID]
                fileVer = fileInfo[ngamsDbCore.SUM1_VERSION]
                fileKey = ngamsLib.genFileKey(None, fileId, fileVer)
                queueDbm.add(fileKey, fileInfo)
            queueDbm.sync()
            _suspend(srvObj)
        queueDbm.sync()
        del cursorObj

        # Check if the expected number of files is equal to the actual
        # number of files dumped into the DBM.
        if (actNoOfFiles != expNoOfFiles):
            errMsg = "Number of files dumped for disk: %s: %d, differs " +\
                     "from expected number: %d"
            errMsg = errMsg % (diskId, actNoOfFiles, expNoOfFiles)
            error(errMsg)
            rmFile(tmpQueueDbmFile)
            del queueDbm
            raise Exception, genLog("NGAMS_ER_DB_COM", [errMsg])

        # Rename DCC Queue DBM from the temporary to the final name.
        del queueDbm
        mvFile(tmpQueueDbmFile, queueDbmFile)
        queueDbm = ngamsDbm.ngamsDbm(queueDbmFile, 0, 1)

        # Create Error DBM + add these in the DBM Dictionary for the disk.
        errorDbmFile = "%s/%s_ERRORS_%s.bsddb" %\
                       (cacheDir, NGAMS_DATA_CHECK_THR, diskId)
        errorDbm = ngamsDbm.ngamsDbm(errorDbmFile, 0, 1)
        _getDbmObjDic()[diskId] = (queueDbm, errorDbm)

        _stopDataCheckThr(srvObj, stopEvt)
    info(3,"Queried info for files to be checked from DB. Time: %.3fs" %\
         (time.time() - startDbFileRd))
    info(4,"Checked that disks scheduled for checking have DBM files")
    ###########################################################################

    ###########################################################################
    # Create a DBM with references to all files found on this system. The
    # complete path name are the keys in the dictionary. While going through
    # the files registered in the DB, we remove the entries in this dictionary.
    # The remaining entries in the dictionary, are thus files, which are not
    # registered in the DB.
    #
    # We walk recursively from the mount point downwards, ignoring disk info
    # files and hidden directories, as well as the top-level staging directory
    ###########################################################################
    info(4,"Create DBM with references to all files on the storage disks ...")
    fileRefDbm = "%s/%s_FILES_%s.bsddb" %\
                 (cacheDir, NGAMS_DATA_CHECK_THR, srvObj.getHostId())
    rmFile(fileRefDbm)
    _setFileRefDbm(ngamsDbm.ngamsDbm(fileRefDbm, 1, 1))
    for diskId in _getDiskDic().keys():
        _stopDataCheckThr(srvObj, stopEvt)
        diskInfoObj = _getDiskDic()[diskId]

        # Walk over the mount point and collect all files
        mount_pt = diskInfoObj.getMountPoint()
        for dirpath, dirs, files in os.walk(mount_pt):

            _stopDataCheckThr(srvObj, stopEvt)

            # Ignore staging and hidden directories
            dirs[:] = [d for d in dirs if d != NGAMS_STAGING_DIR and not d.startswith('.')]

            # Ignore NGAS Disk Info files
            files[:] = [f for f in files if f not in (NGAMS_DISK_INFO, NGAMS_VOLUME_ID_FILE, NGAMS_VOLUME_INFO_FILE)]

            for filename in files:
                filename = os.path.join(dirpath, filename)
                _getFileRefDbm().add(str(filename), [diskInfoObj.getDiskId()])

        _getFileRefDbm().sync()
        _stopDataCheckThr(srvObj, stopEvt)
    info(4,"Created DBMs with references to all files on the storage disks")
    ###########################################################################

    ###########################################################################
    # Now go through files to be ignored. These are removed if found in
    # the File Reference DBM. This is done, since these files are not
    # taking into account in the checking loop
    ###########################################################################
    info(4,"Retrieve information about files to be ignored ...")
    spuFilesCur = srvObj.getDb().getFileSummarySpuriousFiles1(srvObj.getHostId())
    while (1):
        _stopDataCheckThr(srvObj, stopEvt)
        fileList = spuFilesCur.fetch(1000)
        if (not fileList): break

        # Loop over the files.
        for fileInfo in fileList:
            if (fileInfo[ngamsDbCore.SUM1_FILE_IGNORE]):
                filename = os.path.\
                           normpath(fileInfo[ngamsDbCore.SUM1_MT_PT] + "/" +\
                                    fileInfo[ngamsDbCore.SUM1_FILENAME])
                if (_getFileRefDbm().hasKey(filename)):
                    _getFileRefDbm().rem(filename)
        _suspend(srvObj)
    _getFileRefDbm().sync()
    del spuFilesCur
    info(4,"Retrieved information about files to be ignored")
    ###########################################################################

    # Pack the Disk IDs into the list of disks to check.
    info(4, "Create list with disks to be checked ...")
    _resetDiskIdList()
    for lastCheck in lastDiskCheckDic.keys():
        diskInfoObj = lastDiskCheckDic[lastCheck]
        _getDiskIdList().append(diskInfoObj.getDiskId())
    info(4, "Created list with disks to be checked")

    ###########################################################################
    # Initialize the statistics parameters for the checking.
    ###########################################################################
    info(4,"Initialize the statistics for the checking cycle ...")
    amountMb = 0.0
    noOfFiles = 0
    for diskId in _getDiskIdList():
        queueDbm = _getDbmObjDic()[diskId][0]
        #################################################################################################
        #jagonzal: Replace looping aproach to avoid exceptions coming from the next() method underneath
        #          when iterating at the end of the table that are prone to corrupt the hash table object
        #queueDbm.initKeyPtr()
        #while (1):
        #    fileKey, fileInfo = queueDbm.getNext(0)
        #    if (not fileKey): break
        for fileKey,dbVal in queueDbm.iteritems():
            # jagonzal: We need to reformat the values and skip administrative elements #################
            if (str(fileKey).find("__") != -1): continue
            fileInfo = cPickle.loads(dbVal)
            #############################################################################################
            noOfFiles += 1
            amountMb += float(float(fileInfo[ngamsDbCore.SUM1_FILE_SIZE]) / 1048576.0)
        #################################################################################################
    _initFileCheckStatus(srvObj, amountMb, noOfFiles)
    info(4,"Initialized the statistics for the checking cycle")
    ###########################################################################


def _schedNextFile(srvObj,
                   threadId):
    """
    Function that returns the information about the next file to be checked
    according to the checking scheme.

    The function keeps track of which disk has been allocated to which
    Checking Sub-Thread and allocates only files from the given disk to that
    thread.

    srvObj:       Reference to server object (ngamsServer).

    threadId:     ID of the sub-thread (string).

    Returns:      List with information (format see
                  ngamsDbCore.getFileSummary1()) for file to be checked. If
                  there are no more files to check, None is returned
                  (list/None).
    """
    T = TRACE()

    try:
        _takeReqFileInfoSem()
        while (1):
            fileInfo = None
            if (not _getDiskSchedDic().has_key(threadId)):
                if (_getDiskIdList()):
                    idx = random.randint(0, (len(_getDiskIdList()) - 1))
                    diskId = _getDiskIdList()[idx]
                    # Check if that disk is already being checked by other
                    # threads. If yes, we don't initialize the key pointer.
                    beingChecked = 0
                    for thrId in _getDiskSchedDic().keys():
                        if (_getDiskSchedDic()[thrId] == diskId):
                            beingChecked = 1
                            break
                    if (not beingChecked):
                        _getDbmObjDic()[diskId][0].initKeyPtr()
                    fileKey, fileInfo = _getDbmObjDic()[diskId][0].getNext(0)
                    _getDiskSchedDic()[threadId] = diskId
                else:
                    break
            else:
                diskId = _getDiskSchedDic()[threadId]
                fileKey, fileInfo = _getDbmObjDic()[diskId][0].getNext(0)

            if (fileInfo):
                # We got a file key + file info list, return the info.
                break
            else:
                # There are no more file info lists for the given Disk ID,
                # remove that Disk ID from the list and try to switch to
                # another disk. Also set, the time for the last check of that
                # disk.
                _getDbmObjDic()[diskId][0].cleanUp()
                rmFile(_getDbmObjDic()[diskId][0].getDbmName())
                srvObj.getDb().setLastCheckDisk(diskId, time.time())
                if (ngamsLib.elInList(_getDiskIdList(), diskId)):
                    idx = _getDiskIdList().index(diskId)
                    del _getDiskIdList()[idx]
                for thrId in _getDiskSchedDic().keys():
                    if (_getDiskSchedDic()[thrId] == diskId):
                        del _getDiskSchedDic()[thrId]
                if (_getDiskIdList()):
                    idx = random.randint(0, (len(_getDiskIdList()) - 1))
                    nextDiskId = _getDiskIdList()[idx]
                    _getDiskSchedDic()[threadId] = nextDiskId
                else:
                    break

        _relReqFileInfoSem()
        return fileInfo
    except Exception, e:
        warning("Exception in _schedNextFile(): %s" % str(e))
        _relReqFileInfoSem()
        raise e


def _dataCheckSubThread(srvObj,
                        threadId,
                        dummy):
    """
    Sub-thread scheduled to carry out the actual checking. This makes
    it possible to do the checking in several threads simultaneously if
    the NGAS Host has multiple CPUs.

    srvObj:       Reference to server object (ngamsServer).

    threadId:     ID allocated to this thread (string).

    Returns:      Void.
    """
    T = TRACE()

    dataCheckPrio = srvObj.getCfg().getDataCheckPrio()
    while (1):
        _stopCheckingSubThread()

        try:
            # Get the info for the next file to check + check it.
            fileInfo = _schedNextFile(srvObj, threadId)
            if (not fileInfo):
                info(4,"No more files in queue to check - exiting")
                _updateFileCheckStatus(srvObj, None, None, None, None, [], 1)
                raise Exception, "_STOP_DATA_CHECK_SUB_THREAD_"

            # Remove the entry for this file in the File Reference DBM to
            # indicate that the file is registered in the DB.
            filename = os.path.normpath(fileInfo[ngamsDbCore.SUM1_MT_PT]+"/" +\
                                        fileInfo[ngamsDbCore.SUM1_FILENAME])
            filename = str(filename)
            if (_getFileRefDbm().hasKey(filename)):
                _getFileRefDbm().rem(filename).sync()

            # Update the overall status of the checking.
            tmpReport = []
            ngamsFileUtils.checkFile(srvObj, fileInfo, tmpReport,
                                     srvObj.getCfg().getDataCheckScan())
            if (not tmpReport): tmpReport = [[]]
            _updateFileCheckStatus(srvObj,
                                   fileInfo[ngamsDbCore.SUM1_FILE_SIZE],
                                   fileInfo[ngamsDbCore.SUM1_DISK_ID],
                                   fileInfo[ngamsDbCore.SUM1_FILE_ID],
                                   fileInfo[ngamsDbCore.SUM1_VERSION],
                                   tmpReport[0])

            # If the server is handling a command, the sub-thread will suspend
            # itself until the server is idle again.
            if (srvObj.getHandlingCmd()):
                while (srvObj.getHandlingCmd()):
                    _suspend(srvObj, 0.200)
        except Exception, e:
            if (str(e).find("_STOP_DATA_CHECK_SUB_THREAD_") != -1):
                thread.exit()
            else:
                error("Exception encountered in Data Check Sub-Thread: %s" %\
                      str(e))
                time.sleep(2)


def _genReport(srvObj):
    """
    Generate the DCC Check Report according to the problems found.

    srvObj:     Reference to instance of ngamsServer object (ngamsServer).

    Returns:    Void.
    """
    # Find out how many inconsistencies were found.
    noOfProbs = 0
    # Errors found.
    for diskId in _getDiskDic().keys():
        noOfProbs += _getDbmObjDic()[diskId][1].getCount()
    # Spurious files on disk.
    unRegFiles = _getFileRefDbm().getCount()

    # Generate the report.
    global _statCheckStart, _statCheckRate, _statCheckFiles, _statCheckedMb,\
           _statCheckCount
    checkTime = (time.time() - _statCheckStart)
    if ((noOfProbs + unRegFiles) or srvObj.getCfg().getDataCheckForceNotif()):

        report    = ""
        hdrForm   = "%-20s %s\n"
        format    = "%-60s %-32s %-9s %s\n"
        separator = 130 * "-" + "\n"

        # Build up the report.
        report =  "DATA CHECKING REPORT:\n\n"
        report += hdrForm % ("Date", PccUtTime.TimeStamp().getTimeStamp())
        report += hdrForm % ("NGAS Host ID", srvObj.getHostId())
        report += hdrForm % ("Start Time", PccUtTime.TimeStamp().\
                             initFromSecsSinceEpoch(_statCheckStart).\
                             getTimeStamp())
        report += hdrForm % ("Total Time (s)", "%.3f" % checkTime)
        report += hdrForm % ("Total Time (hours)", "%.3f" % (checkTime / 3600))
        report += hdrForm % ("Rate (MB/s)", "%.3f" % _statCheckRate)
        report += hdrForm % ("Files Checked", _statCheckCount)
        report += hdrForm % ("Data Checked (MB)", "%.5f" % _statCheckedMb)
        report += hdrForm % ("Inconsistencies",  str(noOfProbs + unRegFiles))
        report += separator

        # Any discrepancies found?
        if ((not noOfProbs) and (not unRegFiles)):
            report += "No discrepancies found!"

        # Inconsistencies found?
        if (noOfProbs):
            report += "INCONSISTENT FILES FOUND:\n\n"
            report += format % ("Problem Description", "File ID",
                                "Version", "Slot ID:Disk ID")
            report += separator
            for diskId in _getDiskDic().keys():
                errDbm = _getDbmObjDic()[diskId][1].initKeyPtr()
                #################################################################################################
                #jagonzal: Replace looping aproach to avoid exceptions coming from the next() method underneath
                #          when iterating at the end of the table that are prone to corrupt the hash table object
                #while (1):
                #    fileKey, errInfo = errDbm.getNext(0)
                #    if (not fileKey): break
                for fileKey,dbVal in errDbm.iteritems():
                    # jagonzal: We need to reformat the values and skip administrative elements #################
                    if (str(fileKey).find("__") != -1): continue
                    errInfo = cPickle.loads(dbVal)
                    #############################################################################################
                    slotDiskId = errInfo[3] + ":" + errInfo[4]
                    report += format % (errInfo[0], errInfo[1], errInfo[2],slotDiskId)
                #################################################################################################
            report += separator

        # Not registered files found?
        if (unRegFiles):
            repFormat = "%-32s %s\n"
            report += "NOT REGISTERED FILES FOUND ON STORAGE DISKS:\n\n"
            report += repFormat % ("Disk ID:", "Filename:")
            report += separator
            unregFileList = _getFileRefDbm().keys()
            for unregFile in unregFileList:
                fileInfo = _getFileRefDbm().get(unregFile)
                if (not unregFile): break
                if (fileInfo): report += repFormat % (fileInfo[0], unregFile)
            del unregFileList
            report += separator

        # Send Notification Message if needed (only if disks where checked).
        if (len(_getDiskDic().keys())):
            ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(), NGAMS_NOTIF_DATA_CHECK,
                                     "DATA CHECK REPORT", report, [], 1)

    # Give out the statistics for the checking.
    if (srvObj.getCfg().getDataCheckLogSummary()):
        msg = genLog("NGAMS_INFO_DATA_CHK_STAT",
                     [_statCheckCount,unRegFiles,noOfProbs,
                      _statCheckedMb,_statCheckRate,checkTime])
        info(0,msg)

    # Remove the various DBMs allocated.
    for diskId in _getDiskDic().keys():
        _getDbmObjDic()[diskId][1].cleanUp()
        del _getDbmObjDic()[diskId]
    _resetDbmObjDic()
    _getFileRefDbm().cleanUp()
    _resetFileRefDbm()


def _crossCheckNonRegFiles(srvObj):
    """
    This function checks if non-registered files were found during the checking
    if these still are not available. This is necessary if requests are
    handled during the initialization of the DCC, in particular if files are
    archived between the file information is dumped from the DB and the list
    of files actually found on the data volumes is generated. I.e., the
    sequence might be:

    - Dump DB File Info for this node.
    - Some ARCHIVE Commands are handled.
    - Generate list of files on the volumes in this node.

    The files that were archived in between would be detected as not
    registered.

    srvObj:    Reference to server object (ngamsServer).

    Returns:   None.
    """
    T = TRACE()

    tmpFilePat = ngamsHighLevelLib.genTmpFilename(srvObj.getCfg(),
                                                  NGAMS_DATA_CHECK_THR)
    crossCheckDbmName = None
    crossCheckDbm = None
    try:
        crossCheckDbmName = tmpFilePat + "_DCC_CROSS_CHECK_DB"
        rmFile(crossCheckDbmName)
        crossCheckDbm = ngamsDbm.ngamsDbm(crossCheckDbmName, cleanUpOnDestr=1,
                                          writePerm=1)

        #################################################################################################
        #jagonzal: Replace looping aproach to avoid exceptions coming from the next() method underneath
        #          when iterating at the end of the table that are prone to corrupt the hash table object
        #_getFileRefDbm().initKeyPtr()
        #while (1):
        #    filename, fileInfo = _getFileRefDbm().getNext(0)
        #    if (not filename): break
        for filename,dbVal in _getFileRefDbm().iteritems():
            # jagonzal: We need to reformat the values and skip administrative elements #################
            if (str(filename).find("__") != -1): continue
            fileInfo = cPickle.loads(dbVal)
            #############################################################################################
            diskId = fileInfo[0]
            if (not _getDiskDic().has_key(diskId)):
                warning("Unknown Disk ID: %s encountered" % diskId)
                break
            mtPt = _getDiskDic()[diskId].getMountPoint()
            ngasFilename = filename[(len(mtPt) + 1):]
            fileInfo = srvObj.getDb().\
                       getFileInfoFromDiskIdFilename(diskId, ngasFilename)
            if (fileInfo != None):
                msg = "File: %s detected as not registered was found in the "+\
                      "NGAS DB while cross-checking discrepancy. Disk ID: " +\
                      "%s/File Id: %s/File Version: %s"
                crossCheckDbm.add(filename, "")
                info(3,msg % (filename, diskId, fileInfo.getFileId(),
                              fileInfo.getFileVersion()))
            else:
                msg = "File: %s detected as not registered was not found " +\
                      "in the NGAS DB while cross-checking discrepancy. " +\
                      "Disk ID: %s"
                info(3,msg % (filename, diskId))
        #################################################################################################

        # If files during cross-checking were found to be OK, we remove these
        # from the File Registration DBM.

        #################################################################################################
        #jagonzal: Replace looping aproach to avoid exceptions coming from the next() method underneath
        #          when iterating at the end of the table that are prone to corrupt the hash table object
        #crossCheckDbm.initKeyPtr()
        #while (1):
        #    key, val = crossCheckDbm.getNext(0)
        #    if (not key): break
        for key,dbVal in crossCheckDbm.iteritems():
            # jagonzal: We need to reformat the values and skip administrative elements #################
            if (str(key).find("__") != -1): continue
            val = cPickle.loads(dbVal)
            #############################################################################################
            _getFileRefDbm().rem(key)
        #################################################################################################

        _getFileRefDbm().sync()
        del crossCheckDbm
        rmFile(crossCheckDbmName)
    except Exception, e:
        if (crossCheckDbm): del crossCheckDbm
        if (crossCheckDbmName): rmFile(crossCheckDbmName)
        msg = "Error encountered in _crossCheckNonRegFiles(). Error: %s" %\
              str(e)
        error(msg)
        raise msg


def dataCheckThread(srvObj, stopEvt):
    """
    The Data Check Thread is executed to run a periodic check of the
    consistency of the data files contained in an NG/AMS system. The periodic
    check is performed only when NG/AMS is Online.

    srvObj:       Reference to server object (ngamsServer).

    Returns:      Void.
    """
    minCycleTime = isoTime2Secs(srvObj.getCfg().getDataCheckMinCycle())

    while True:

        # Encapsulate this whole block to avoid that the thread dies in
        # case a problem occurs, like e.g. a problem with the DB connection.
        try:
            # Wait until we're sure that the Janitor Thread has executed
            # at least once, to ensure that the check is carried out in a
            # clean environment.
            while (not srvObj.getJanitorThreadRunCount()):
                suspend(srvObj, stopEvt, 0.5)

            if (srvObj.getCfg().getDataCheckLogSummary()):
                info(0,"Data Check Thread starting iteration ...")

            srvObj.updateHostInfo(None, None, None, None, None, None, 1, None)

            # It was intended to implement a scheme for preserving the info
            # about files already checked between two sessions if the DCC
            # should be interrupted if the server is shut down. This attempt
            # has been given up since if the disk configuration changed
            # between the two sessions, false DCC problems might be reported.
            #
            # These temporary DCC DBM's are kept in the NG/AMS Cache Dir
            # since if put in the NG/AMS Temp. Dir. they might be deleted
            # if not accessed for the specified expiration time in the temp.
            # dir - a DCC run may run for a longer period of time.
            rmFile("%s/%s/%s*" % (srvObj.getCfg().getRootDirectory(),
                                  NGAMS_CACHE_DIR, NGAMS_DATA_CHECK_THR))

            # Get the information about the files to check.
            tmpFilePat = ngamsHighLevelLib.\
                         genTmpFilename(srvObj.getCfg(),
                                        NGAMS_DATA_CHECK_THR)
            try:
                _dumpFileInfo(srvObj, tmpFilePat, stopEvt)
            finally:
                rmFile(tmpFilePat + "*")

            # According to the number of disks to be checked, a sub-thread
            # is allocated for each up to the limit defined in the
            # configuration.
            #
            # Afterwards the main DCC Thread monitors the execution of
            # these + update the status information.
            thrHandleDic = {}
            noOfSubThreads = len(_getDiskIdList())
            if (noOfSubThreads > srvObj.getCfg().getDataCheckMaxProcs()):
                noOfSubThreads = srvObj.getCfg().getDataCheckMaxProcs()
            _resetDiskSchedDic()
            for n in range(1, (noOfSubThreads + 1)):
                threadId = NGAMS_DATA_CHECK_THR + "-" + str(n)
                args = (srvObj, threadId, None)
                info(3,"Starting Data Check Sub-Thread: %s" % threadId)
                thrHandleDic[n] = threading.Thread(None, _dataCheckSubThread,
                                                   threadId, args)
                thrHandleDic[n].setDaemon(0)
                thrHandleDic[n].start()

            while True:
                time.sleep(0.500)

                # Check if the DCC Thread is allowed to run, else stop all
                # activities.
                if stopEvt.isSet():
                    # Stop the sub-threads.
                    _setRunPermSubThread(0)
                    startTime = time.time()
                    noOfSubThr = len(thrHandleDic.keys())
                    while ((time.time() - startTime) < 10):
                        thrCount = 0
                        for thrId in thrHandleDic.keys():
                            if (not thrHandleDic[thrId].isAlive()):
                                del thrHandleDic[thrId]
                                thrCount += 1
                        if (thrCount == noOfSubThr): break
                        time.sleep(0.050)
                    # Kill the possible remaining sub-threads.
                    for thrId in thrHandleDic.keys():
                        del thrHandleDic[thrId]
                    _stopDataCheckThr(srvObj, stopEvt)
                else:
                    # Check if all the sub-threads are still running
                    # or if the check cycle is completed.
                    for thrId in thrHandleDic.keys():
                        if (not thrHandleDic[thrId].isAlive()):
                            del thrHandleDic[thrId]
                    if (not len(thrHandleDic)):
                        lastCheckTime = time.time()
                        break

            # Check again for non-registered files.
            global _statCheckCount, _statCheckFiles
            if (_statCheckCount): _crossCheckNonRegFiles(srvObj)

            # Send out check report if any discrepancies found + send
            # out notification message according to configuration.
            _genReport(srvObj)

            # Set the last check for all disks to the same value
            for diskId in _getDiskDic().keys():
                srvObj.getDb().setLastCheckDisk(diskId, lastCheckTime)

            # FLush the log; otherwise we might not notice that this has
            # finished until it's too late
            logFlush()

            srvObj.updateHostInfo(None, None, None, None, None, None, 0, None)
            ###################################################################

            ###################################################################
            # Check if we should wait for a while for the Minimum Cycle
            # Time to elapse.
            ###################################################################
            lastOldestCheck = srvObj.getDb().getMinLastDiskCheck(srvObj.getHostId())
            waitTime = 0
            timeNow = time.time()
            if (lastOldestCheck):
                if ((timeNow - lastOldestCheck) < minCycleTime):
                    waitTime = (minCycleTime - (timeNow - lastOldestCheck))
            else:
                execTime = (timeNow - _statCheckStart)
                if (execTime < minCycleTime):
                    waitTime = (minCycleTime - execTime)
            if (waitTime):
                waitTime += 1
                info(3,"Suspending Data Checking Thread for: " +\
                     str(waitTime)+"s.")
                nextAbsCheckTime = int(time.time() + waitTime + 0.5)
                info(3,"Next Data Checking scheduled for: " +\
                     PccUtTime.TimeStamp().\
                     initFromSecsSinceEpoch(nextAbsCheckTime).\
                     getTimeStamp() + "/" + str(nextAbsCheckTime))
                srvObj.setNextDataCheckTime(nextAbsCheckTime)

                suspend(srvObj, stopEvt, waitTime)
            ###################################################################

        except StopDataCheckThreadException:
            return
        except Exception, e:

            waitTime = 1
            if "NGAMS_ER_DB_COM" in str(e):
                errMsg = "Problem communicating with DB. " +\
                        "Suspending DCC %ds. Error : %s" % (waitTime, str(e))
                waitTime = 3600
            else:
                errMsg = "Error occurred during execution of the " +\
                         "Data Check Thread. Exception: " + str(e)
            error(errMsg)

            try:
                suspend(srvObj, stopEvt, 1)
            except StopDataCheckThreadException:
                return


# EOF
