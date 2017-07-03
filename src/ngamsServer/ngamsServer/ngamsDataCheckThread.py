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

import cPickle
import glob
import logging
import os
import random
import time
import threading

import ngamsFileUtils
from ngamsLib.ngamsCore import TRACE, NGAMS_DATA_CHECK_THR, \
    NGAMS_CACHE_DIR, checkCreatePath, isoTime2Secs, \
    rmFile, genLog, mvFile, NGAMS_DISK_INFO, NGAMS_VOLUME_ID_FILE, \
    NGAMS_VOLUME_INFO_FILE, NGAMS_STAGING_DIR, NGAMS_NOTIF_DATA_CHECK, toiso8601
from ngamsLib import ngamsNotification, ngamsDiskInfo
from ngamsLib import ngamsDbCore, ngamsDbm, ngamsHighLevelLib, ngamsLib


logger = logging.getLogger(__name__)

class StopDataCheckThreadException(Exception):
    pass

def _finishThread():
    logger.info("Stopping Data Check Thread")
    raise StopDataCheckThreadException()

def _stopDataCheckThr(stopEvt):
    """
    Checks whether the thread should finish or not
    """
    if stopEvt.isSet():
        _finishThread()

def suspend(stopEvt, t):
    """
    Sleeps for ``t`` seconds, unless the thread is signaled to stop
    """
    if stopEvt.wait(t):
        _finishThread()

# Parameters for statistics.
class Stats(object):
    def __init__(self, mbs, files):
        self.lock = threading.Lock()
        self.last_db_update = 0
        self.time_start = time.time()
        self.time_remaining = 0
        self.check_rate = 0.0
        self.mbs = mbs
        self.mbs_checked = 0
        self.files = files
        self.files_checked = 0


def _initFileCheckStatus(srvObj, amountMb, noOfFiles):
    """
    Initialize the checking parameters.

    srvObj:         Reference to instance of ngamsServer object (ngamsServer).

    amountMb:       Amount of data to check (MB) (float).

    noOfFiles:      Number of files to check (integer).

    Returns:        Void.
    """

    stats = Stats(mbs=amountMb, files=noOfFiles)

    srvObj.getDb().updateDataCheckStat(srvObj.getHostId(), stats.time_start,
                                       stats.time_remaining, 0,
                                       stats.check_rate, stats.mbs,
                                       stats.mbs_checked, stats.files,
                                       stats.files_checked)

    logger.debug("Initialized the statistics for the checking cycle")
    return stats

def _updateFileCheckStatus(srvObj,
                           fileSize,
                           diskId,
                           fileId,
                           fileVersion,
                           report,
                           stats,
                           dbmObjDic,
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

    now = time.time()
    with stats.lock:

        # Calculate the new values.
        if (fileId):
            stats.mbs_checked += float(fileSize) / 1048576.0
            stats.files_checked += 1

        checkTime = now - stats.time_start
        stats.check_rate = stats.mbs_checked / checkTime
        if stats.check_rate > 0:
            stats.remainding_time = (stats.mbs - stats.mbs_checked) / stats.check_rate
            statEstimTime = stats.mbs / stats.check_rate
        else:
            stats.remainding_time = 0
            statEstimTime    = 0

        # Update DB only every 10s.
        if force or now - stats.last_db_update >= 10:
            srvObj.getDb().updateDataCheckStat(srvObj.getHostId(), stats.time_start,
                                               stats.time_remaining, statEstimTime,
                                               stats.check_rate, stats.mbs,
                                               stats.mbs_checked, stats.files,
                                               stats.files_checked)
            stats.last_db_update = now

        # Update report if an error was found.
        if (diskId and report):
            fileKey = ngamsLib.genFileKey(None, fileId, fileVersion)
            dbmObjDic[diskId][1].add(fileKey, report)

        statFormat = "DCC Status: Time Remaining (s): %d, " +\
                     "Rate (MB/s): %.3f, " +\
                     "Volume/Checked (MB): %.3f/%.3f, Files/Checked: %d/%d"
        logger.debug(statFormat, stats.time_remaining, stats.check_rate, stats.mbs_checked,
             stats.mbs, stats.files, stats.files_checked)


def suspend_with_priority(srvObj, stopEvt, baseTime = 0.010):
    """
    Determine the time the thread should suspend itself according to
    the load, priority etc. + suspend for the given time.

    srvObj:      Reference to instance of ngamsServer object (ngamsServer).

    baseTime:    Time used to calculate the suspension time (float).

    Returns:     Void.
    """
    suspTime = (srvObj.getCfg().getDataCheckPrio() * baseTime)
    suspend(stopEvt, suspTime)


def collect_files_on_disk(stopEvt, disks):

    all_files = {}

    # Loop over disks
    for diskId, diskInfoObj in disks.items():

        # Walk over the mount point and collect all files
        mount_pt = diskInfoObj.getMountPoint()
        for dirpath, dirs, files in os.walk(mount_pt):
            _stopDataCheckThr(stopEvt)

            # Ignore staging and hidden directories
            dirs[:] = [d for d in dirs if d != NGAMS_STAGING_DIR and not d.startswith('.')]

            # Ignore NGAS Disk Info files
            files[:] = [f for f in files if f not in (NGAMS_DISK_INFO, NGAMS_VOLUME_ID_FILE, NGAMS_VOLUME_INFO_FILE)]

            for filename in files:
                filename = os.path.join(dirpath, filename)
                all_files[filename] = diskId

    return all_files

def _dumpFileInfo(srvObj, disks_to_check, tmpFilePat, stopEvt):
    """
    Function that dumps the information about the files. One DBM is created
    per disk. This is named:

       <Mount Root Point>/cache/DATA-CHECK-THREAD_QUEUE_<Disk ID>.bsddb

    If problems are found for a file, these are stored in DBM files named:

       <Mount Root Point>/cache/DATA-CHECK-THREAD_ERRORS_<Disk ID>.bsddb

    The function handles the DBM files in the following way:

       1. Check for each DBM file found, if this disk is still in the system.
          If not, the Queue and Error DBM files are removed.

       2. Go through the list of disks in the system. If they don't have
          the two DBM files listed above, these are initialized. The file
          information for all the files stored on the disk is dumped into
          the Queue DBM file. Only files marked to be ignored are not dumped.

       3. Finally, build up a DBM with references to all files found
          on this system

    srvObj:       Reference to server object (ngamsServer).

    tmpFilePat:   Pattern for temporary files (string).

    Returns:      Void.
    """
    T = TRACE()

    cacheDir = srvObj.getCfg().getRootDirectory() + "/" + NGAMS_CACHE_DIR
    checkCreatePath(os.path.normpath(cacheDir))

    ###########################################################################
    # Loop over the Queue/Error DBM files found, check if the disk is
    # still in the system/scheduled for checking.
    ###########################################################################
    logger.debug("Loop over/check existing Queue/Error DBM Files ...")
    dbmFileList = glob.glob(cacheDir + "/" + NGAMS_DATA_CHECK_THR +\
                            "_QUEUE_*.bsddb")
    dbmObjDic = {}
    for dbmFile in dbmFileList:
        _stopDataCheckThr(stopEvt)
        diskId = dbmFile.split("_")[-1].split(".")[0]
        if diskId not in disks_to_check:
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
            dbmObjDic[diskId] = (queueDbm, errorDbm)
    logger.debug("Looped over/checked existing Queue/Error DBM Files")
    ###########################################################################

    ###########################################################################
    # Loop over the disks mounted in this system and check if they have a
    # Queue/Error DBM file. In case the DBM files are not available, create
    # these.
    ###########################################################################
    logger.debug("Create DBM files for disks to be checked ...")
    startDbFileRd = time.time()
    for diskId in disks_to_check.keys():
        _stopDataCheckThr(stopEvt)
        if (dbmObjDic.has_key(diskId)): continue

        # The disk is ripe for checking but still has no Queue/Error DBM
        # DBs allocated.
        queueDbmFile = "%s/%s_QUEUE_%s.bsddb" %\
                       (cacheDir, NGAMS_DATA_CHECK_THR, diskId)
        tmpQueueDbmFile = tmpFilePat + "_" + os.path.basename(queueDbmFile)
        queueDbm = ngamsDbm.ngamsDbm(tmpQueueDbmFile, 0, 1)

        # Now, retrieve the files on the given disk, and store the info
        # in the Queue DBM file.
        # TODO: Use ngamsDb.dumpFileSummary1().
        cursorObj = srvObj.getDb().getFileSummary1(diskIds=[diskId],
                                                   ignore=0, fileStatus=[],
                                                   lowLimIngestDate=None,
                                                   order=0)
        while (1):

            fileList = cursorObj.fetch(1000)
            if not fileList:
                break

            for fileInfo in fileList:
                fileId  = fileInfo[ngamsDbCore.SUM1_FILE_ID]
                fileVer = fileInfo[ngamsDbCore.SUM1_VERSION]
                fileKey = ngamsLib.genFileKey(None, fileId, fileVer)
                queueDbm.add(fileKey, fileInfo)
            queueDbm.sync()
            suspend_with_priority(srvObj, stopEvt)
        del cursorObj

        # Rename DCC Queue DBM from the temporary to the final name.
        mvFile(tmpQueueDbmFile, queueDbmFile)
        queueDbm = ngamsDbm.ngamsDbm(queueDbmFile, 0, 1)

        # Create Error DBM + add these in the DBM Dictionary for the disk.
        errorDbmFile = "%s/%s_ERRORS_%s.bsddb" %\
                       (cacheDir, NGAMS_DATA_CHECK_THR, diskId)
        errorDbm = ngamsDbm.ngamsDbm(errorDbmFile, 0, 1)
        dbmObjDic[diskId] = (queueDbm, errorDbm)

        _stopDataCheckThr(stopEvt)
    logger.debug("Queried info for files to be checked from DB. Time: %.3fs",
         time.time() - startDbFileRd)
    logger.debug("Checked that disks scheduled for checking have DBM files")
    ###########################################################################

    # These are all files recursively found on the disks
    # Later on we check whether they are registered or not, and check them (or not)
    start = time.time()
    files_on_disk = collect_files_on_disk(stopEvt, disks_to_check)
    end = time.time()
    logger.debug("Collected references to %d files on disks in %.3f [s]",
                 len(files_on_disk), end - start)

    # Don't take these into account
    logger.debug("Retrieving information about files to be ignored ...")
    spuFilesCur = srvObj.getDb().getFileSummarySpuriousFiles1(srvObj.getHostId())
    while (1):
        fileList = spuFilesCur.fetch(1000)
        if (not fileList): break

        # Loop over the files.
        for fileInfo in fileList:
            if (fileInfo[ngamsDbCore.SUM1_FILE_IGNORE]):
                filename = os.path.\
                           normpath(fileInfo[ngamsDbCore.SUM1_MT_PT] + "/" +\
                                    fileInfo[ngamsDbCore.SUM1_FILENAME])
                if filename in files_on_disk:
                    del files_on_disk[filename]
        suspend_with_priority(srvObj, stopEvt)
    del spuFilesCur
    logger.debug("Retrieved information about files to be ignored")
    ###########################################################################

    ###########################################################################
    # Initialize the statistics parameters for the checking.
    ###########################################################################
    logger.debug("Initialize the statistics for the checking cycle ...")
    amountMb = 0.0
    noOfFiles = 0
    for diskId in disks_to_check.keys():
        queueDbm = dbmObjDic[diskId][0]
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
            amountMb += float(fileInfo[ngamsDbCore.SUM1_FILE_SIZE] / 1048576.0)
        #################################################################################################

    stats = _initFileCheckStatus(srvObj, amountMb, noOfFiles)
    ###########################################################################

    return files_on_disk, dbmObjDic, stats

def _schedNextFile(srvObj,
                   threadId, disk_ids, diskSchedDic, dbmObjDic, reqFileInfoSem):
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

    with reqFileInfoSem:
        while True:
            fileInfo = None
            if (not diskSchedDic.has_key(threadId)):
                if (disk_ids):
                    idx = random.randint(0, (len(disk_ids) - 1))
                    diskId = disk_ids[idx]
                    # Check if that disk is already being checked by other
                    # threads. If yes, we don't initialize the key pointer.
                    beingChecked = 0
                    for thrId in diskSchedDic.keys():
                        if (diskSchedDic[thrId] == diskId):
                            beingChecked = 1
                            break
                    if (not beingChecked):
                        dbmObjDic[diskId][0].initKeyPtr()
                    fileKey, fileInfo = dbmObjDic[diskId][0].getNext(0)
                    diskSchedDic[threadId] = diskId
                else:
                    break
            else:
                diskId = diskSchedDic[threadId]
                fileKey, fileInfo = dbmObjDic[diskId][0].getNext(0)

            if (fileInfo):
                # We got a file key + file info list, return the info.
                break
            else:
                # There are no more file info lists for the given Disk ID,
                # remove that Disk ID from the list and try to switch to
                # another disk. Also set, the time for the last check of that
                # disk.
                dbmObjDic[diskId][0].cleanUp()
                rmFile(dbmObjDic[diskId][0].getDbmName())
                srvObj.getDb().setLastCheckDisk(diskId, time.time())
                if diskId in disk_ids:
                    idx = disk_ids.index(diskId)
                    del disk_ids[idx]
                for thrId in diskSchedDic.keys():
                    if (diskSchedDic[thrId] == diskId):
                        del diskSchedDic[thrId]
                if (disk_ids):
                    idx = random.randint(0, (len(disk_ids) - 1))
                    nextDiskId = disk_ids[idx]
                    diskSchedDic[threadId] = nextDiskId
                else:
                    break

        return fileInfo


def _dataCheckSubThread(srvObj,
                        threadId,
                        stopEvt,
                        all_files,
                        disk_ids,
                        diskSchedDic,
                        dbmObjDic,
                        reqFileInfoSem,
                        stats):
    """
    Sub-thread scheduled to carry out the actual checking. This makes
    it possible to do the checking in several threads simultaneously if
    the NGAS Host has multiple CPUs.

    srvObj:       Reference to server object (ngamsServer).

    threadId:     ID allocated to this thread (string).

    Returns:      Void.
    """
    T = TRACE()

    while (1):

        try:
            _stopDataCheckThr(stopEvt)

            # Get the info for the next file to check + check it.
            fileInfo = _schedNextFile(srvObj, threadId, disk_ids, diskSchedDic, dbmObjDic, reqFileInfoSem)
            if (not fileInfo):
                logger.debug("No more files in queue to check - exiting")
                _updateFileCheckStatus(srvObj, None, None, None, None, [], stats, dbmObjDic, 1)
                return

            # Remove the entry for this file in the File Reference DBM to
            # indicate that the file is registered in the DB.
            filename = os.path.normpath(fileInfo[ngamsDbCore.SUM1_MT_PT]+"/" +\
                                        fileInfo[ngamsDbCore.SUM1_FILENAME])
            filename = str(filename)

            if filename in all_files:
                del all_files[filename]

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
                                   tmpReport[0],
                                   stats,
                                   dbmObjDic)

            # If the server is handling a command, the sub-thread will suspend
            # itself until the server is idle again.
            if (srvObj.getHandlingCmd()):
                while (srvObj.getHandlingCmd()):
                    suspend_with_priority(srvObj, stopEvt, 0.200)
        except StopDataCheckThreadException:
            raise
        except Exception:
            logger.exception("Exception encountered in Data Check Sub-Thread")
            suspend(stopEvt, 2)


def _genReport(srvObj, unregistered, diskDic, dbmObjDic, stats):
    """
    Generate the DCC Check Report according to the problems found.

    srvObj:     Reference to instance of ngamsServer object (ngamsServer).

    Returns:    Void.
    """
    # Find out how many inconsistencies were found.
    noOfProbs = 0
    # Errors found.
    for diskId in diskDic.keys():
        noOfProbs += dbmObjDic[diskId][1].getCount()
    # Spurious files on disk.
    unRegFiles = len(unregistered)

    # Generate the report.
    checkTime = time.time() - stats.time_start
    if ((noOfProbs + unRegFiles) or srvObj.getCfg().getDataCheckForceNotif()):

        report    = ""
        hdrForm   = "%-20s %s\n"
        format    = "%-60s %-32s %-9s %s\n"
        separator = 130 * "-" + "\n"

        # Build up the report.
        report =  "DATA CHECKING REPORT:\n\n"
        report += hdrForm % ("Date", toiso8601())
        report += hdrForm % ("NGAS Host ID", srvObj.getHostId())
        report += hdrForm % ("Start Time", toiso8601(stats.time_start))
        report += hdrForm % ("Total Time (s)", "%.3f" % checkTime)
        report += hdrForm % ("Total Time (hours)", "%.3f" % (checkTime / 3600))
        report += hdrForm % ("Rate (MB/s)", "%.3f" % stats.check_rate)
        report += hdrForm % ("Files Checked", stats.files_checked)
        report += hdrForm % ("Data Checked (MB)", "%.5f" % stats.mbs_checked)
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
            for diskId in diskDic.keys():
                errDbm = dbmObjDic[diskId][1].initKeyPtr()
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
            for filename, disk_id in unregistered.items():
                report += repFormat % (disk_id, filename)
            report += separator

        # Send Notification Message if needed (only if disks where checked).
        if (len(diskDic.keys())):
            ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(), NGAMS_NOTIF_DATA_CHECK,
                                     "DATA CHECK REPORT", report, [], 1)

    # Give out the statistics for the checking.
    if (srvObj.getCfg().getDataCheckLogSummary()):
        msg = genLog("NGAMS_INFO_DATA_CHK_STAT",
                     [stats.files_checked, unRegFiles, noOfProbs,
                      stats.mbs_checked, stats.check_rate, checkTime])
        logger.info(msg)

    # Remove the various DBMs allocated.
    for diskId in diskDic.keys():
        dbmObjDic[diskId][1].cleanUp()
        del dbmObjDic[diskId]


def _crossCheckNonRegFiles(srvObj, unchecked_files, diskDic):
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

    unregistered = {}
    for filename, diskId in unchecked_files.items():

        if diskId not in diskDic:
            logger.warning("Unknown Disk ID: %s encountered", diskId)
            break

        mtPt = diskDic[diskId].getMountPoint()
        ngasFilename = filename[(len(mtPt) + 1):]
        fileInfo = srvObj.getDb().\
                   getFileInfoFromDiskIdFilename(diskId, ngasFilename)
        if fileInfo is not None:
            msg = "File: %s detected as not registered was found in the "+\
                  "NGAS DB while cross-checking discrepancy. Disk ID: " +\
                  "%s/File Id: %s/File Version: %s"
            logger.debug(msg, filename, diskId, fileInfo.getFileId(),
                          fileInfo.getFileVersion())
        else:
            msg = "File: %s detected as not registered was not found " +\
                  "in the NGAS DB while cross-checking discrepancy. " +\
                  "Disk ID: %s"
            logger.debug(msg, filename, diskId)
            unregistered[filename] = diskId

    # Unregistered files returned
    return unregistered


def get_disks_to_check(srvObj):

    # Get mounted disks
    slotIdList = srvObj.getDb().getSlotIdsMountedDisks(srvObj.getHostId())
    disks_to_check = srvObj.getDb().\
                  getDiskInfoForSlotsAndHost(srvObj.getHostId(), slotIdList)

    # Turn from simply SQL results into objects indexed by disk_id
    disks_to_check = [ngamsDiskInfo.ngamsDiskInfo().unpackSqlResult(x) for x in disks_to_check]
    disks_to_check = {x.getDiskId(): x for x in disks_to_check}

    # Filter out those that don't need a check
    now = time.time()
    check_period = isoTime2Secs(srvObj.getCfg().getDataCheckMinCycle())
    def needs_check(x):
        last_check = x.getLastCheck() or 0
        return check_period + last_check < now
    disks_to_check = {k: v for k, v in disks_to_check.items() if needs_check(v)}

    logger.info("Will check %d disks that are mounted in this system", len(disks_to_check))
    return disks_to_check


def dataCheckThread(srvObj, stopEvt):
    """
    The Data Check Thread is executed to run a periodic check of the
    consistency of the data files contained in an NG/AMS system. The periodic
    check is performed only when NG/AMS is Online.

    srvObj:       Reference to server object (ngamsServer).

    Returns:      Void.
    """
    minCycleTime = isoTime2Secs(srvObj.getCfg().getDataCheckMinCycle())
    logger.info("Data checker thread period is %f", minCycleTime)

    while True:

        # Encapsulate this whole block to avoid that the thread dies in
        # case a problem occurs, like e.g. a problem with the DB connection.
        try:
            # Wait until we're sure that the Janitor Thread has executed
            # at least once, to ensure that the check is carried out in a
            # clean environment.
            while (not srvObj.getJanitorThreadRunCount()):
                suspend(stopEvt, 0.5)

            if (srvObj.getCfg().getDataCheckLogSummary()):
                logger.info("Data Check Thread starting iteration ...")

            srvObj.updateHostInfo(None, None, None, None, None, None, 1, None)

            # Get list of disks that need checking
            disks_to_check = get_disks_to_check(srvObj)

            # Get the information about the files in those disks that we should check.
            tmpFilePat = ngamsHighLevelLib.\
                         genTmpFilename(srvObj.getCfg(),
                                        NGAMS_DATA_CHECK_THR)
            try:
                all_files, dbmObjDic, stats = _dumpFileInfo(srvObj, disks_to_check, tmpFilePat, stopEvt)
            finally:
                rmFile(tmpFilePat + "*")

            # According to the number of disks to be checked, a sub-thread
            # is allocated for each up to the limit defined in the
            # configuration.
            #
            # Afterwards the main DCC Thread monitors the execution of
            # these + update the status information.
            noOfSubThreads = len(disks_to_check)
            if (noOfSubThreads > srvObj.getCfg().getDataCheckMaxProcs()):
                noOfSubThreads = srvObj.getCfg().getDataCheckMaxProcs()

            diskSchedDic = {}

            reqFileInfoSem = threading.Lock()
            disk_ids = list(disks_to_check.keys())
            thrHandleDic = {}
            for n in range(noOfSubThreads):
                threadId = NGAMS_DATA_CHECK_THR + "-" + str(n)
                args = (srvObj, threadId, stopEvt, all_files, disk_ids, diskSchedDic, dbmObjDic, reqFileInfoSem, stats)
                logger.debug("Starting Data Check Sub-Thread: %s", threadId)
                thrHandleDic[n] = threading.Thread(None, _dataCheckSubThread,
                                                   threadId, args)
                thrHandleDic[n].setDaemon(0)
                thrHandleDic[n].start()

            while True:

                try:
                    suspend(stopEvt, 0.500)
                except StopDataCheckThreadException:

                    # Be nice and join sub-threads
                    for t in thrHandleDic.values():
                        t.join(10)
                        if t.isAlive():
                            logger.warning("Thread %r didn't cleanly shut down within 10 seconds", t)

                    # Let's stop ourselves now
                    raise

                else:
                    # Check if all the sub-threads are still running
                    # or if the check cycle is completed.
                    for thrId in thrHandleDic.keys():
                        if not thrHandleDic[thrId].isAlive():
                            del thrHandleDic[thrId]
                    if not len(thrHandleDic):
                        lastCheckTime = time.time()
                        break

            # Check again for non-registered files.
            # The sub-threads remove individual items from all_files after they
            # check each file, so any files left there were not checked
            unregistered = _crossCheckNonRegFiles(srvObj, all_files, disks_to_check)

            # Send out check report if any discrepancies found + send
            # out notification message according to configuration.
            _genReport(srvObj, unregistered, disks_to_check, dbmObjDic, stats)

            # Set the last check for all disks to the same value
            for diskId in disks_to_check.keys():
                srvObj.getDb().setLastCheckDisk(diskId, lastCheckTime)

            # FLush the log; otherwise we might not notice that this has
            # finished until it's too late
            #logFlush()

            srvObj.updateHostInfo(None, None, None, None, None, None, 0, None)
            ###################################################################

            ###################################################################
            # Check if we should wait for a while for the Minimum Cycle
            # Time to elapse.
            ###################################################################
            lastOldestCheck = srvObj.getDb().getMinLastDiskCheck(srvObj.getHostId())

            time_to_compare = lastOldestCheck or stats.time_start
            execTime = time.time() - time_to_compare
            if execTime < minCycleTime:
                waitTime = minCycleTime - execTime
                logger.info("Suspending Data Checking Thread for %.3f [s]", waitTime)
                nextAbsCheckTime = int(time.time() + waitTime)
                logger.info("Next Data Checking scheduled for %s", toiso8601(nextAbsCheckTime))
                srvObj.setNextDataCheckTime(nextAbsCheckTime)

                suspend(stopEvt, waitTime)
            ###################################################################

        except StopDataCheckThreadException:
            return
        except Exception:
            errMsg = "Error occurred during execution of the Data Check Thread"
            logger.exception(errMsg)

            try:
                suspend(stopEvt, 1)
            except StopDataCheckThreadException:
                return

# EOF