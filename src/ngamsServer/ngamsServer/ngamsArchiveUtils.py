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
# "@(#) $Id: ngamsArchiveUtils.py,v 1.10 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  14/11/2001  Created
#

"""
Contains utility functions used in connection with the handling of
the file archiving.
"""
import contextlib
import cPickle
import glob
import logging
import os
import random
import time

from ngamsLib.ngamsCore import NGAMS_FAILURE, getFileCreationTime,\
    NGAMS_FILE_STATUS_OK, TRACE, NGAMS_NOTIF_DISK_SPACE,\
    getDiskSpaceAvail, NGAMS_XML_MT, NGAMS_NOTIF_DISK_CHANGE, genLog,\
    NGAMS_HTTP_GET, NGAMS_ARCHIVE_CMD, NGAMS_HTTP_FILE_URL, cpFile,\
    NGAMS_NOTIF_NO_DISKS, mvFile, NGAMS_PICKLE_FILE_EXT,\
    rmFile, NGAMS_SUCCESS, NGAMS_BACK_LOG_TMP_PREFIX, NGAMS_BACK_LOG_DIR,\
    getHostName, loadPlugInEntryPoint
from ngamsLib import ngamsHighLevelLib, ngamsNotification, ngamsPlugInApi, ngamsLib,\
    ngamsHttpUtils
from ngamsLib import ngamsReqProps, ngamsFileInfo, ngamsDiskInfo, ngamsStatus, ngamsDiskUtils
import ngamsFileUtils
import ngamsCacheControlThread


# Dictionary to keep track of disk space warnings issued.
_diskSpaceWarningDic = {}

logger = logging.getLogger(__name__)

def updateFileInfoDb(srvObj,
                     piStat,
                     checksum,
                     checksumPlugIn):
    """
    Update the information for the file in the NGAS DB.

    srvObj:           Server object (ngamsServer).

    piStat:           Status object returned by Data Archiving Plug-In.
                      (ngamsDapiStatus).

    checksum:         Checksum value for file (string).

    checksumPlugIn:   Checksum Plug-In (string).

    Returns:          Void.
    """
    logger.debug("Updating file info in NGAS DB for file with ID: %s", piStat.getFileId())

    # Check that the file is really contained in the final location as
    # indicated by the information in the File Info Object.
    try:
        ngamsFileUtils.syncCachesCheckFiles(srvObj,
                                            [piStat.getCompleteFilename()])
    except Exception, e:
        errMsg = "Severe error occurred! Cannot update information in " +\
                 "NGAS DB (ngas_files table) about file with File ID: " +\
                 piStat.getFileId() + " and File Version: " +\
                 str(piStat.getFileVersion()) + ", since file is not found " +\
                 "in the indicated, final storage location! Check system! " +\
                 "Error: " + str(e)
        raise Exception, errMsg

    if (piStat.getStatus() == NGAMS_FAILURE): return
    now = time.time()
    creDate = getFileCreationTime(piStat.getCompleteFilename())
    fileInfo = ngamsFileInfo.ngamsFileInfo().\
               setDiskId(piStat.getDiskId()).\
               setFilename(piStat.getRelFilename()).\
               setFileId(piStat.getFileId()).\
               setFileVersion(piStat.getFileVersion()).\
               setFormat(piStat.getFormat()).\
               setFileSize(piStat.getFileSize()).\
               setUncompressedFileSize(piStat.getUncomprSize()).\
               setCompression(piStat.getCompression()).\
               setIngestionDate(now).\
               setChecksum(checksum).setChecksumPlugIn(checksumPlugIn).\
               setFileStatus(NGAMS_FILE_STATUS_OK).\
               setCreationDate(creDate).\
               setIoTime(piStat.getIoTime())
    fileInfo.write(srvObj.getHostId(), srvObj.getDb())
    logger.debug("Updated file info in NGAS DB for file with ID: %s", piStat.getFileId())


def replicateFile(dbConObj,
                  ngamsCfgObj,
                  diskDic,
                  piStat):
    """
    Replicate a main file stored in this system.

    dbConObj:         DB connection object (ngamsDb).

    ngamsCfgObj:      NG/AMS ConfigurationObject (ngamsConfig).

    piStat:           Status returned by Data Archiving Plug-In
                      (ngamsDapiStatus).

    Returns:          Data Archiving Plug-In Status for Replication File
                      or None if no Replication Disk is configured for the
                      Main Disk (ngamsDapiStatus).
    """
    T = TRACE()

    if (ngamsCfgObj.getAssocSlotId(piStat.getSlotId()) == ""):
        logger.debug("No Replication Disk is configured for the Main Disk in Slot "+\
             "with ID: %s - no replication performed", piStat.getSlotId())
        return None
    else:
        logger.debug("Replicating file: %s", piStat.getRelFilename())

    # Get the ID for the Replication Disk.
    setObj = ngamsCfgObj.getStorageSetFromSlotId(piStat.getSlotId())
    if (not diskDic.has_key(setObj.getRepDiskSlotId())):
        raise Exception, "Error handling Archive Request - no Replication " +\
              "Disk found according to configuration. Replication Disk " +\
              "Slot ID: " + str(setObj.getRepDiskSlotId())
    repDiskId    = diskDic[setObj.getRepDiskSlotId()].getDiskId()
    mainDiskMtPt = diskDic[setObj.getMainDiskSlotId()].getMountPoint()
    repDiskMtPt  = diskDic[setObj.getRepDiskSlotId()].getMountPoint()
    srcFilename  = os.path.normpath(mainDiskMtPt + "/"+piStat.getRelFilename())
    trgFilename  = os.path.normpath(repDiskMtPt + "/"+piStat.getRelFilename())
    fileExists   = ngamsHighLevelLib.\
                   checkIfFileExists(dbConObj, piStat.getFileId(), repDiskId,
                                     piStat.getFileVersion(), trgFilename)
    if (ngamsCfgObj.getReplication()):
        logger.debug("Replicating Main File: %s to Replication File: %s - " + \
                     "on Disk with Disk ID: %s",
                     srcFilename, trgFilename, repDiskId)
        ioTime = ngamsHighLevelLib.copyFile(ngamsCfgObj,
                                            setObj.getMainDiskSlotId(),
                                            setObj.getRepDiskSlotId(),
                                            srcFilename, trgFilename)[0]
        ngamsLib.makeFileReadOnly(trgFilename)
    else:
        logger.info("Note: Replication is not done by NG/AMS!!")
        ioTime = 0
    logger.debug("File: %s replicated", piStat.getRelFilename())

    # Generate the plug-in status object.
    piStat2 = ngamsPlugInApi.genDapiSuccessStat(repDiskId,
                                                piStat.getRelFilename(),
                                                piStat.getFileId(),
                                                piStat.getFileVersion(),
                                                piStat.getFormat(),
                                                piStat.getFileSize(),
                                                piStat.getUncomprSize(),
                                                piStat.getCompression(),
                                                piStat.getRelPath(),
                                                piStat.getSlotId(),
                                                piStat.getFileExists(),
                                                trgFilename)
    piStat2.setIoTime(ioTime)
    return piStat2


def resetDiskSpaceWarnings():
    """
    Reset the dictionary keeping track of the disk space warnings issued.

    Returns:     Void.
    """
    global _diskSpaceWarningDic
    for diskId in _diskSpaceWarningDic.keys():
        _diskSpaceWarningDic[diskId] = 0


def issueDiskSpaceWarning(srvObj,
                          diskId):
    """
    Check if the amount of free disk space on the disk with the given ID is
    below the limit specified in the configuration file. If yes, a warning
    log/Notification Message is issued.

    dbConObj:      NG/AMS DB Connection object (ngamsDb).

    ngamsCfgObj:   NG/AMS Configuration object (ngamsConfig).

    diskId:        Disk ID (string).

    Returns:       Void.
    """
    T = TRACE()

    global _diskSpaceWarningDic
    if (_diskSpaceWarningDic.has_key(diskId) == 0):
        _diskSpaceWarningDic[diskId] = 0
    if (_diskSpaceWarningDic[diskId] == 0):
        diskInfo = ngamsDiskInfo.ngamsDiskInfo()
        diskInfo.read(srvObj.getDb(), diskId)
        msg = "Disk with ID: " + diskId + " - Name: " +\
              diskInfo.getLogicalName() + " - Slot No.: " +\
              str(diskInfo.getSlotId()) + " - running low in "+\
              "available space (" + str(diskInfo.getAvailableMb())+" MB)!"
        logger.warning(msg)
        _diskSpaceWarningDic[diskId] = 1
        ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(), NGAMS_NOTIF_DISK_SPACE,
                                 "NOTICE: DISK SPACE RUNNING LOW",
                                 msg + "\n\nNote: This is just a notice. " +\
                                 "A message will be send when the disk " +\
                                 "should be changed.", [], 1)


def checkDiskSpace(srvObj,
                   mainDiskId):
    """
    Check the amount of disk space on the disk with the given ID. Both the
    Main Disk and the Replication Disk are taken into account.

    If this amount is below the threshold for changing disk from the
    configuration file (Monitor.FreeSpaceDiskChangeMb), the disk will be
    marked as 'completed'.

    srvObj:         Reference to instance of the NG/AMS Server class
                    (ngamsServer).

    mainDiskId:     Disk ID for disk to check (string).

    Returns:        Void.
    """
    logger.debug("Checking disk space for disk with ID: %s", mainDiskId)

    # Get info Main Disk
    mainDiskInfo = ngamsDiskInfo.ngamsDiskInfo()
    mainDiskInfo.read(srvObj.getDb(), mainDiskId)
    availSpaceMbMain = getDiskSpaceAvail(mainDiskInfo.getMountPoint(),
                                         smart=False)

    # Get the Replication Disk Slot ID. If no Replication Disk is
    # configured for the Main Disk, this will be ''.
    repDiskSlotId = srvObj.getCfg().getAssocSlotId(mainDiskInfo.getSlotId())

    # Get info about the Replication Disk
    if (repDiskSlotId != ""):
        repDiskInfo = ngamsDiskInfo.ngamsDiskInfo()
        repDiskId = srvObj.getDiskDic()[repDiskSlotId].getDiskId()
        repDiskInfo.read(srvObj.getDb(), repDiskId)
        availSpaceMbRep = getDiskSpaceAvail(repDiskInfo.getMountPoint(),
                                            smart=False)
    else:
        availSpaceMbRep = -1

    # Get reference to the Storage Set definition from the configuration file.
    stoSetObj = srvObj.getCfg().\
                getStorageSetFromSlotId(mainDiskInfo.getSlotId())

    # Check if we should issue a "Disk Running Full Notice".
    if ((availSpaceMbMain < srvObj.getCfg().getMinFreeSpaceWarningMb()) or \
        ((availSpaceMbRep < srvObj.getCfg().getMinFreeSpaceWarningMb()) and \
         (repDiskSlotId != ""))):
        issueDiskSpaceWarning(srvObj, mainDiskId)

    # Check if we should change Disk Set/mark disks as completed.
    freeDiskSpaceLim = srvObj.getCfg().getFreeSpaceDiskChangeMb()
    if ((availSpaceMbMain < freeDiskSpaceLim) or \
        (stoSetObj.getSynchronize() and \
         (availSpaceMbRep < freeDiskSpaceLim) and (repDiskSlotId != ""))):
        mainDiskCompl = 1
    else:
        mainDiskCompl = 0
    if (((availSpaceMbRep < freeDiskSpaceLim) and (repDiskSlotId != "")) or \
        (stoSetObj.getSynchronize() and \
         (availSpaceMbMain < freeDiskSpaceLim) and (repDiskSlotId != ""))):
        repDiskCompl  = 1
    else:
        repDiskCompl  = 0
    if (mainDiskCompl or repDiskCompl):
        complDate = time.time()

    # Mark Main Disk as completed if required.
    if (mainDiskCompl):
        # The amount of space available is below the specified limit.
        # - Mark Main Disk as Completed.
        mainDiskInfo.setCompleted(1).setCompletionDate(complDate)
        mainDiskInfo.write(srvObj.getDb())
        logger.warning("Marked Main Disk with ID: " + mainDiskId + " - Name: " +\
               mainDiskInfo.getLogicalName() + " - Slot No.: " +\
               str(mainDiskInfo.getSlotId()) + " - as 'completed' " +\
               "- PLEASE CHANGE!")
        # - Update NGAS Disk Info accordingly
        ngasDiskInfoFile = ngamsDiskUtils.\
                           dumpDiskInfo(srvObj.getHostId(),
                                        srvObj.getDb(), srvObj.getCfg(),
                                        mainDiskInfo.getDiskId(),
                                        mainDiskInfo.getMountPoint())
        srvObj.getDb().addDiskHistEntry(srvObj.getHostId(),
                                        mainDiskInfo.getDiskId(),
                                        "Disk Completed", NGAMS_XML_MT,
                                        ngasDiskInfoFile)

    # Mark Replication Disk as completed if required.
    if (repDiskCompl):
        repDiskInfo.setCompleted(1).setCompletionDate(complDate)
        repDiskInfo.write(srvObj.getDb())
        logger.warning("Marked Replication Disk with ID: " + repDiskInfo.getDiskId() +\
               " - Name: " + repDiskInfo.getLogicalName() +\
               " - Slot No.: " + str(repDiskInfo.getSlotId()) +\
               " - as 'completed' - PLEASE CHANGE!")
        # - Update NGAS Disk Info accordingly
        ngasDiskInfoFile = ngamsDiskUtils.\
                           dumpDiskInfo(srvObj.getHostId(),
                                        srvObj.getDb(), srvObj.getCfg(),
                                        repDiskInfo.getDiskId(),
                                        repDiskInfo.getMountPoint())
        srvObj.getDb().addDiskHistEntry(srvObj.getHostId(),
                                        repDiskInfo.getDiskId(),
                                        "Disk Completed", NGAMS_XML_MT,
                                        ngasDiskInfoFile)

    # Generate/send Notification Message(s).
    if (mainDiskCompl or repDiskCompl):
        msg = "PLEASE CHANGE DISK(S):\n\n"
        if (mainDiskCompl):
            msg += "Main Disk:\n" +\
                   "- Logical Name: " + mainDiskInfo.getLogicalName() + "\n" +\
                   "- Slot ID:      " + mainDiskInfo.getSlotId() + "\n\n"
        if (repDiskCompl):
            msg += "Replication Disk:\n" +\
                   "- Logical Name: " + repDiskInfo.getLogicalName() + "\n" +\
                   "- Slot ID:      " + repDiskInfo.getSlotId() + "\n\n"
        ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(), NGAMS_NOTIF_DISK_CHANGE,
                                 "CHANGE DISK(S)", msg, [], 1)


def postFileRecepHandling(srvObj,
                          reqPropsObj,
                          resultPlugIn,
                          cksum = None):
    """
    The function carries out the action needed after a file has been received
    for archiving. This consists of updating the information about the
    file in the DB, and to replicate the file if requested.

    srvObj:         Reference to instance of the NG/AMS Server class
                    (ngamsServer).

    reqPropsObj:    NG/AMS Request Properties Object (ngamsReqProps).

    resultPlugIn:   Result returned from DAPI (ngamsDapiStatus).

    cksum:          Tuple containing checksum string value and algorithm

    Returns:        Disk info object containing the information about
                    the Main File (ngasDiskInfo).
    """
    logger.debug("Data returned from Data Archiving Plug-In: %s", resultPlugIn.toString())

    # if checksum is already supplied then do not calculate it from the plugin
    if (cksum == None):
        # Calculate checksum (if plug-in specified).
        checksumPlugIn = srvObj.getCfg().getChecksumPlugIn()
        if (checksumPlugIn != ""):
            logger.debug("Invoking Checksum Plug-In: %s to handle file: %s",
                         checksumPlugIn, resultPlugIn.getCompleteFilename())
            plugInMethod = loadPlugInEntryPoint(checksumPlugIn)
            checksum = plugInMethod(srvObj, resultPlugIn.getCompleteFilename(), 0)
            logger.debug("Result: %s", checksum)
        else:
            checksum = ''
            checksumPlugIn = ''
    else:
        checksum, checksumPlugIn = cksum

    # Update information for Main File/Disk in DB.
    updateFileInfoDb(srvObj, resultPlugIn, checksum, checksumPlugIn)
    mainDiskInfo = ngamsDiskUtils.updateDiskStatusDb(srvObj.getDb(),
                                                     resultPlugIn)
    ngamsLib.makeFileReadOnly(resultPlugIn.getCompleteFilename())

    # If running as a cache archive, update the Cache New Files DBM
    # with the information about the new file.
    if (srvObj.getCachingActive()):
        fileVersion = resultPlugIn.getFileVersion()
        filename = resultPlugIn.getRelFilename()
        ngamsCacheControlThread.addEntryNewFilesDbm(srvObj,
                                                    resultPlugIn.getDiskId(),
                                                    resultPlugIn.getFileId(),
                                                    fileVersion, filename)

    # Log a message if a file with the File ID of the new file already existed.
    if (resultPlugIn.getFileExists()):
        msg = genLog("NGAMS_NOTICE_FILE_REINGESTED",
                     [reqPropsObj.getSafeFileUri()])
        logger.warning(msg)

    # Now handle the Replication Disk - if there is a corresponding Replication
    # Disk for the Main Disk and if not replication was disabled by the DAPI.
    if (srvObj.getCfg().getReplication()):
        srvObj.test_BeforeRepFile()
        assocSlotId = srvObj.getCfg().getAssocSlotId(resultPlugIn.getSlotId())
        if ((not reqPropsObj.getNoReplication()) and (assocSlotId != "")):
            resRep = replicateFile(srvObj.getDb(), srvObj.getCfg(),
                                   srvObj.getDiskDic(), resultPlugIn)
            srvObj.test_BeforeDbUpdateRepFile()
            updateFileInfoDb(srvObj, resRep, checksum, checksumPlugIn)
            ngamsDiskUtils.updateDiskStatusDb(srvObj.getDb(), resRep)

        # Inform the caching service about the new file.
        if (srvObj.getCachingActive()):
            diskId      = resRep.getDiskId()
            fileId      = resRep.getFileId()
            fileVersion = resRep.getFileVersion()
            filename    = resRep.getRelFilename()
            ngamsCacheControlThread.addEntryNewFilesDbm(srvObj, diskId, fileId,
                                                        fileVersion, filename)

    # Check if we should change to next disk.
    checkDiskSpace(srvObj, resultPlugIn.getDiskId())

    # Generate a File Info Object for the file stored.
    fileInfo = ngamsFileInfo.ngamsFileInfo()
    fileInfo.read(srvObj.getHostId(),
                  srvObj.getDb(), resultPlugIn.getFileId(),
                  resultPlugIn.getFileVersion())
    mainDiskInfo.addFileObj(fileInfo)

    # Trigger the Data Susbcription Thread to make it check if there are
    # files to deliver to the new Subscriber.
    srvObj.addSubscriptionInfo([(resultPlugIn.getFileId(),
                                 resultPlugIn.getFileVersion())], [])

    logger.debug("Handled file with URI: %s successfully", reqPropsObj.getSafeFileUri())
    return mainDiskInfo


def archiveFromFile(srvObj,
                    filename,
                    noReplication = 0,
                    mimeType = None,
                    reqPropsObj = None):
    """
    Archive a file directly from a file as source.

    srvObj:          Reference to NG/AMS Server Object (ngamsServer).

    filename:        Name of file to archive (string).

    noReplication:   Flag to enable/disable replication (integer).

    reqPropsObj:     Request Property object to keep track of actions done
                     during the request handling (ngamsReqProps).

    Returns:         Execution status (NGAMS_SUCCESS|NGAMS_FAILURE).
    """
    logger.info("Archiving file: %s", filename)
    if (reqPropsObj):
        logger.debug("Request Properties Object given - using this")
        reqPropsObjLoc = reqPropsObj
    else:
        logger.debug("No Request Properties Object given - creating one")
        reqPropsObjLoc = ngamsReqProps.ngamsReqProps()
    stagingFile = filename
    try:
        if (mimeType == None):
            mimeType = ngamsHighLevelLib.determineMimeType(srvObj.getCfg(),
                                                           filename)

        archiving_start = time.time()

        # Prepare dummy ngamsReqProps object (if an object was not given).
        if (not reqPropsObj):
            reqPropsObjLoc.setMimeType(mimeType)
            reqPropsObjLoc.setStagingFilename(filename)
            reqPropsObjLoc.setHttpMethod(NGAMS_HTTP_GET)
            reqPropsObjLoc.setCmd(NGAMS_ARCHIVE_CMD)
            reqPropsObjLoc.setSize(os.path.getsize(filename))
            reqPropsObjLoc.setFileUri(NGAMS_HTTP_FILE_URL + filename)
            reqPropsObjLoc.setNoReplication(noReplication)

        # If no target disk is defined, find one suitable disk.
        if (not reqPropsObjLoc.getTargDiskInfo()):
            try:
                trgDiskInfo = ngamsDiskUtils.\
                              findTargetDisk(srvObj.getHostId(),
                                             srvObj.getDb(), srvObj.getCfg(),
                                             mimeType, 0,
                                             reqSpace=reqPropsObjLoc.getSize())
                reqPropsObjLoc.setTargDiskInfo(trgDiskInfo)
                # copy the file to the staging area of the target disk
                stagingFile = trgDiskInfo.getMountPoint()+ '/staging/' + os.path.basename(filename)
                cpFile(filename, stagingFile)
                reqPropsObjLoc.setStagingFilename(stagingFile)
            except Exception, e:
                errMsg = str(e) + ". Attempting to archive local file: " +\
                         filename
                ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(),
                                         NGAMS_NOTIF_NO_DISKS,
                                         "NO DISKS AVAILABLE", errMsg)
                raise Exception, errMsg

        # Set the log cache to 1 during the handling of the file.
        plugIn = srvObj.getMimeTypeDic()[mimeType]
        logger.info("Invoking DAPI: %s", plugIn)
        plugInMethod = loadPlugInEntryPoint(plugIn)
        resMain = plugInMethod(srvObj, reqPropsObjLoc)
        # Move the file to final destination.
        mvFile(reqPropsObjLoc.getStagingFilename(),
               resMain.getCompleteFilename())

        postFileRecepHandling(srvObj, reqPropsObjLoc, resMain)
    except Exception, e:
        # If another error occurrs, than one qualifying for Back-Log
        # Buffering the file, we have to log an error.
        if (ngamsHighLevelLib.performBackLogBuffering(srvObj.getCfg(),
                                                      reqPropsObjLoc, e)):
            logger.warning("Tried to archive local file: " + filename +\
                   ". Attempt failed with following error: " + str(e) +\
                   ". Keeping original file.")
            return NGAMS_FAILURE
        else:
            logger.exception("Tried to archive local file: %s" +\
                  ". Attempt failed with following error", filename)
            logger.warning("Moving local file: " +\
                   filename + " to Bad Files Directory -- cannot be handled.")
            ngamsHighLevelLib.moveFile2BadDir(srvObj.getCfg(), filename,
                                              filename)
            # Remove pickle file if available.
            pickleObjFile = filename + "." + NGAMS_PICKLE_FILE_EXT
            if (os.path.exists(pickleObjFile)):
                logger.info("Removing Back-Log Buffer Pickle File: %s", pickleObjFile)
                rmFile(pickleObjFile)
            return NGAMS_FAILURE

    # If the file was handled successfully, we remove it from the
    # Back-Log Buffer Directory unless the local file was a log-file
    # in which case we leave the cleanup to the Janitor-Thread.
    if filename.find('LOG-ROTATE') > -1:
        logger.info("Successfully archived local file: %s", filename)
    else:
        logger.info("Successfully archived local file: %s. Removing original file", filename)
        rmFile(filename)
        rmFile(filename + "." + NGAMS_PICKLE_FILE_EXT)

    logger.debug("Archived local file: %s. Time (s): %.3f", filename, time.time() - archiving_start)
    return NGAMS_SUCCESS


def backLogBufferFiles(srvObj,
                       stagingFile,
                       reqPropsFile):
    """
    Back-Log Buffer Original Staging File and the corresponding Request
    Properties File. This is done in a safe manner such that first a
    Temporary Back-Log Buffer File is created, this is then renamed. Then
    the original file is removed after reassuring that the files have arrived
    in the Back-Log Buffer.

    srvObj:           Reference to NG/AMS server class object (ngamsServer).

    stagingFile:      Name of Staging File (string).

    reqPropsFile:     Name of Request Properties File (string).

    Returns:          Void.
    """
    T = TRACE()

    try:
        # We can back-log buffer the two files.
        tmpMsg = "Back-Log Buffering Staging File: %s. " +\
                 "Corresponding Request Properties file: %s ..."
        logger.info(tmpMsg, stagingFile, reqPropsFile)
        backLogDir = srvObj.getCfg().getBackLogDir()
        backLogBufFile = os.path.normpath("%s/%s" %\
                                          (backLogDir,
                                           os.path.basename(stagingFile)))
        tmpBackLogBufFile = "%s/%s%s" % (backLogDir, NGAMS_BACK_LOG_TMP_PREFIX,
                                         os.path.basename(stagingFile))
        cpFile(stagingFile, tmpBackLogBufFile)
        backLogBufReqFile = "%s/%s" % (backLogDir,
                                       os.path.basename(reqPropsFile))
        tmpBackLogBufReqFile = "%s/%s%s" %\
                               (backLogDir, NGAMS_BACK_LOG_TMP_PREFIX,
                                os.path.basename(reqPropsFile))
        # Have to change the name of the Staging File in the Req. Prop.
        # Object = name of Back-Log Buffering File.
        with open(reqPropsFile, "r") as fo:
            tmpReqPropObj = cPickle.load(fo).setStagingFilename(backLogBufFile)

        with open(tmpBackLogBufReqFile, "w") as fo:
            cPickle.dump(tmpReqPropObj, fo)

        mvFile(tmpBackLogBufFile, backLogBufFile)
        mvFile(tmpBackLogBufReqFile, backLogBufReqFile)
        ngamsFileUtils.syncCachesCheckFiles(srvObj, [backLogBufFile,
                                                     backLogBufReqFile])
        rmFile(stagingFile)
        rmFile(reqPropsFile)
    except Exception, e:
        errMsg = genLog("NGAMS_ER_PROB_BACK_LOG_BUF",
                        [ngamsLib.hidePassword(stagingFile),backLogDir,str(e)])
        logger.exception(errMsg)
        raise


def checkBackLogBuffer(srvObj):
    """
    Method to check if there is any data in the NG/AMS Back-Log Directory
    and to archive this if there is.

    srvObj:         Reference to NG/AMS Server object (ngamsServer).

    Returns:        Void.
    """
    logger.debug("Checking if data available in Back-Log Buffer Directory ...")

    # Generate Back Log Buffering Directory
    backLogDir = os.path.join(srvObj.getCfg().getBackLogBufferDirectory(), NGAMS_BACK_LOG_DIR)

    # Get file list. Take only files which do not have the
    # NGAMS_BACK_LOG_TMP_PREFIX as prefix.
    fileList = glob.glob(backLogDir + "/*")
    for file in fileList:
        if ((file[0] == "/") and (file[1] == "/")): file = file[1:]
        if ((file.find(NGAMS_BACK_LOG_TMP_PREFIX) == -1) and
            (file.find("." + NGAMS_PICKLE_FILE_EXT) == -1)):
            logger.info("Archiving Back-Log Buffered file: %s", file)
            # Check if a pickled Request Object File is available.
            try:
                pickleObjFile = file + "." + NGAMS_PICKLE_FILE_EXT
                reqPropsObj = ngamsLib.loadObjPickleFile(pickleObjFile)
            except Exception, e:
                errMsg = "Error encountered trying to load pickled " +\
                         "Request Properties Object from file: " +\
                         pickleObjFile + ".  Error: " + str(e)
                if (str(e).find("[Errno 2]") != -1):
                    logger.warning(errMsg)
                    reqPropsObj = None
                else:
                    raise Exception, errMsg
            if (archiveFromFile(srvObj, file, 0, None,
                                reqPropsObj) == NGAMS_SUCCESS):
                rmFile(pickleObjFile)


def cleanUpStagingArea(srvObj,
                       reqPropsObj,
                       tmpStagingFilename,
                       stagingFilename,
                       tmpReqPropsFilename,
                       reqPropsFilename):
    """
    The function cleans up the Staging Area for the files involved
    in the Archive Request.

    If the client requested wait=0, the Original Staging File +
    Request Properties File are moved to the Bad Files Area. Possible, other
    Staging Files are removed.

    If the client requested wait=1, all files are deleted.

    srvObj:                Reference to NG/AMS server class object
                           (ngamsServer).

    reqPropsObj:           Request Property object to keep track of actions
                           done during the request handling (ngamsReqProps).

    tmpStagingFilename:    Temporary Staging File (string).

    stagingFilename:       Staging File (string).

    tmpReqPropsFilename:   Temporary Request Properties File (string).

    reqPropsFilename:      Request Properties File (string).

    Returns:               Void.
    """
    T = TRACE()

    # All Staging Files can be deleted.
    stgFiles = [tmpStagingFilename, stagingFilename, tmpReqPropsFilename,
                reqPropsFilename]
    for stgFile in stgFiles:
        if (stgFile):
            logger.warning("Removing Staging File: %s", stgFile)
            rmFile(stgFile)


def dataHandler(srvObj,
                reqPropsObj,
                httpRef):
    """
    Data handler that takes care of the handling in connection
    with archiving a data file.

    srvObj:        Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:   Request Property object to keep track of actions done
                   during the request handling (ngamsReqProps).

    httpRef:       Reference to the HTTP request handler
                   object (ngamsHttpRequestHandler).

    Returns:       Disk info object with status for Main Disk
                   where data file was stored (ngamsDiskInfo).
    """
    T = TRACE()

    logger.info(genLog("NGAMS_INFO_ARCHIVING_FILE", [reqPropsObj.getFileUri()]), extra={'to_syslog': True})

    if reqPropsObj.getSize() <= 0:
        raise Exception('Content-Length is 0')

    baseName = os.path.basename(reqPropsObj.getFileUri())
    mimeType = reqPropsObj.getMimeType()
    archiving_start = time.time()

    logger.info("Archiving file: %s with mime-type: %s",
                reqPropsObj.getSafeFileUri(), mimeType)
    tmpStagingFilename = stagingFilename = tmpReqPropsFilename =\
                         reqPropsFilename = None
    try:

        # Generate target filename. Remember to set this in the Request Object.
        try:
            trgDiskInfo = ngamsDiskUtils.\
                          findTargetDisk(srvObj.getHostId(),
                                         srvObj.getDb(), srvObj.getCfg(),
                                         mimeType, 0, caching=0,
                                         reqSpace=reqPropsObj.getSize())
            reqPropsObj.setTargDiskInfo(trgDiskInfo)
        except Exception, e:
            errMsg = str(e) + ". Attempting to archive file: " +\
                      reqPropsObj.getSafeFileUri()
            ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(), NGAMS_NOTIF_NO_DISKS,
                                      "NO DISKS AVAILABLE", errMsg)
            raise Exception, errMsg

        # Generate Staging Filename + Temp Staging File + save data in this
        # file. Also Org. Staging Filename is created, Processing Staging
        # Filename and the Temp. Req. Props. File and Req. Props. File.
        storageSetId = trgDiskInfo.getStorageSetId()
        tmpStagingFilename, stagingFilename,\
                            tmpReqPropsFilename,\
                            reqPropsFilename = ngamsHighLevelLib.\
                            genStagingFilename(srvObj.getCfg(), reqPropsObj,
                                               srvObj.getDiskDic(),
                                               storageSetId,
                                               reqPropsObj.getFileUri(),
                                               genTmpFiles=1)
        # Save the data into the Temp. Staging File.
        ioTime = ngamsHighLevelLib.saveInStagingFile(srvObj.getCfg(),
                                                     reqPropsObj,
                                                     tmpStagingFilename,
                                                     trgDiskInfo)
        srvObj.test_AfterSaveInStagingFile()
        logger.debug("Iotime returned from saveInStagingFile: %6.2f", ioTime)
        reqPropsObj.incIoTime(ioTime)
        logger.debug("Create Temporary Request Properties File: %s", tmpReqPropsFilename)
        tmpReqPropsObj = reqPropsObj.clone().setReadFd(None).setWriteFd(None).\
                         setTargDiskInfo(None)
        ngamsLib.createObjPickleFile(tmpReqPropsFilename, tmpReqPropsObj)
        srvObj.test_AfterCreateTmpPropFile()
        logger.debug("Move Temporary Staging File to Processing Staging File: %s -> %s",
                     tmpStagingFilename, stagingFilename)
        mvFile(tmpStagingFilename, stagingFilename)
        logger.debug("Move Temporary Request Properties File to Request " + \
                     "Properties File: %s -> %s",
                     tmpReqPropsFilename, reqPropsFilename)
        mvFile(tmpReqPropsFilename, reqPropsFilename)

        # Synchronize the file caches to ensure the files have been stored
        # on the disk and check that the files are accessible.
        # This sync is only relevant if back-log buffering is on.
        if (srvObj.getCfg().getBackLogBuffering()):
            ngamsFileUtils.syncCachesCheckFiles(srvObj, [stagingFilename,
                                                         reqPropsFilename])

        # Invoke the Data Archiving Plug-In.
        plugIn = srvObj.getMimeTypeDic()[mimeType]
        plugInMethod = loadPlugInEntryPoint(plugIn)

        logger.info("Invoking DAPI: %s to handle data for file with URI: %s",
                    plugIn, os.path.basename(reqPropsObj.getFileUri()))
        srvObj.test_BeforeDapiInvocation()
        timeBeforeDapi = time.time()
        resMain = plugInMethod(srvObj, reqPropsObj)
        srvObj.test_AfterDapiInvocation()
        logger.debug("Invoked DAPI: %s. Time: %.3fs.",plugIn, (time.time() - timeBeforeDapi))

        # Move the file to final destination.
        ioTime = mvFile(reqPropsObj.getStagingFilename(),
                        resMain.getCompleteFilename())
        reqPropsObj.incIoTime(ioTime)
        srvObj.test_AfterMovingStagingFile()

        # Remember to set the final IO time in the plug-in status object.
        resMain.setIoTime(reqPropsObj.getIoTime())

    except Exception, e:
        if (str(e).find("NGAMS_ER_DAPI_BAD_FILE") != -1):
            errMsg = "Problems during archiving! URI: " +\
                     reqPropsObj.getFileUri() + ". Exception: " + str(e)
            cleanUpStagingArea(srvObj, reqPropsObj, tmpStagingFilename,
                               stagingFilename, tmpReqPropsFilename,
                               reqPropsFilename)
        elif (str(e).find("NGAMS_ER_DAPI_RM") != -1):
            errMsg = "DAPI: " + plugIn + " encountered problem handling " +\
                     "the file with URI: " + reqPropsObj.getFileUri() +\
                     ". Removal of Staging Files requested by DAPI."
            logger.warning(errMsg)
            stgFiles = [tmpStagingFilename, stagingFilename,
                        tmpReqPropsFilename, reqPropsFilename]
            for stgFile in stgFiles:
                logger.warning("Removing Staging File: %s", stgFile)
                rmFile(stgFile)
            errMsg += " Error from DAPI: " + str(e)
        elif (ngamsHighLevelLib.performBackLogBuffering(srvObj.getCfg(),
                                                        reqPropsObj, e)):
            backLogBufferFiles(srvObj, stagingFilename, reqPropsFilename)
            errMsg = genLog("NGAMS_WA_BUF_DATA",
                            [reqPropsObj.getFileUri(), str(e)])
            logger.error(errMsg)
            stgFiles = [tmpStagingFilename, stagingFilename,
                        tmpReqPropsFilename]
            for stgFile in stgFiles:
                logger.warning("Removing Staging File: %s", stgFile)
                rmFile(stgFile)
        else:
            # Another error ocurred.
            errMsg = "Error encountered handling file: " + str(e)
            logger.error(errMsg)
            cleanUpStagingArea(srvObj, reqPropsObj, tmpStagingFilename,
                               stagingFilename, tmpReqPropsFilename,
                               reqPropsFilename)
        raise Exception, errMsg

    diskInfo = postFileRecepHandling(srvObj, reqPropsObj, resMain)
    msg = genLog("NGAMS_INFO_FILE_ARCHIVED", [reqPropsObj.getSafeFileUri()])
    msg = msg + ". Time: %.3fs" % (time.time() - archiving_start)
    logger.info(msg, extra={'to_syslog': True})

    # Remove back-up files (Original Staging File + Request Properties File.
    srvObj.test_BeforeArchCleanUp()
    logger.debug("Removing Request Properties File: %s", reqPropsFilename)
    if (reqPropsFilename): rmFile(reqPropsFilename)

    return diskInfo


def findTargetNode(srvObj, mimeType):
    """
    Finds the NGAS server that should handle the archiving of a file of type
    `mimeType`. The node to archive onto is determined randomly, and only if
    it can handle the request for the given file type.

    If no nodes are available, an exception is raised (NGAMS_AL_NO_STO_SETS).

    In case of success, this method returns a tuple with:
     * The hostId of the NGAS server that will be contacted
     * The IP that should be used to contact the server
     * The port that should be used to contact the server
    """

    ngamsCfgObj = srvObj.getCfg()
    dbConObj = srvObj.getDb()

    hostIds = list(ngamsCfgObj.getStreamFromMimeType(mimeType).getHostIdList())

    # If there are Storage Sets defined for the mime-type, also the local
    # host is a candidate.
    locStoSetList = ngamsCfgObj.getStreamFromMimeType(mimeType).\
                    getStorageSetIdList()
    if locStoSetList:
        hostIds.append(srvObj.getHostId())

    # Shuffle and find
    random.shuffle(hostIds)
    for hostId in hostIds:

        host, port = srvObj.get_remote_server_endpoint(hostId)

        # This is us!
        if hostId == srvObj.getHostId():

            # This is basically what ARCHIVE?probe=1 (the request we send to
            # other servers) do.
            try:
                ngamsDiskUtils.findTargetDisk(hostId, dbConObj, ngamsCfgObj, mimeType,
                                            sendNotification=0)
                return hostId, host, port
            except Exception, e:
                if (str(e).find("NGAMS_AL_NO_STO_SETS") != -1):
                    logMsg = "Local node: %s cannot handle Archive " +\
                             "Request for data file with mime-type: %s"
                    logger.debug(logMsg, getHostName(), mimeType)
                    continue
                raise Exception, e

        # Now, issue the request to the node to probe.
        logMsg = "Probing remote Archiving Unit: %s:%s for handling of " +\
                 "data file with mime-type: %s ..."
        logger.debug(logMsg, host, str(port), mimeType)
        try:
            pars = [("probe", "1"), ("mime_type", mimeType)]
            resp =  ngamsHttpUtils.httpGet(host, port, NGAMS_ARCHIVE_CMD,
                                     pars=pars, timeout=10)
            with contextlib.closing(resp):
                data = resp.read()

            # OK, request was successfully handled. We assume that
            # the data is an NG/AMS XML Status Document.
            statObj = ngamsStatus.ngamsStatus().unpackXmlDoc(data)
            if "NGAMS_INFO_ARCH_REQ_OK" in statObj.getMessage():
                logMsg = "Found remote Archiving Unit: %s:%d to handle " +\
                         "Archive Request for data file with mime-type: %s"
                logger.debug(logMsg, host, port, mimeType)
                return hostId, host, port

            logMsg = "Remote Archiving Unit: %s:%d rejected to/" +\
                     "could not handle Archive Request for data " +\
                     "file with mime-type: %s"
            logger.debug(logMsg, host, port, mimeType)
            continue

        except Exception, e:
            # The request handling failed for some reason, give up this
            # host for now.
            logMsg = "Problem contacting remote Archiving Unit: %s:%d: %s. " +\
                     "Skipping node."
            logger.warning(logMsg, host, port, str(e))
            continue

    errMsg = genLog("NGAMS_AL_NO_STO_SETS", [mimeType])
    raise Exception(errMsg)


# EOF
