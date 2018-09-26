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
import collections
import contextlib
import glob
import logging
import os
import random
import time

from six.moves.urllib import parse as urlparse # @UnresolvedImport
from six.moves.urllib import request as urlrequest # @UnresolvedImport
from six.moves import cPickle # @UnresolvedImport

from ngamsLib.ngamsCore import NGAMS_FAILURE, getFileCreationTime,\
    NGAMS_FILE_STATUS_OK, NGAMS_NOTIF_DISK_SPACE,\
    getDiskSpaceAvail, NGAMS_XML_MT, NGAMS_NOTIF_DISK_CHANGE, genLog,\
    NGAMS_HTTP_GET, NGAMS_ARCHIVE_CMD, NGAMS_HTTP_FILE_URL, cpFile,\
    NGAMS_NOTIF_NO_DISKS, mvFile, NGAMS_PICKLE_FILE_EXT,\
    rmFile, NGAMS_SUCCESS, NGAMS_BACK_LOG_TMP_PREFIX, NGAMS_BACK_LOG_DIR,\
    getHostName, loadPlugInEntryPoint, checkCreatePath, NGAMS_HTTP_HDR_CHECKSUM,\
    NGAMS_ONLINE_STATE, NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE,\
    NGAMS_NOTIF_ERROR
from ngamsLib import ngamsHighLevelLib, ngamsNotification, ngamsPlugInApi, ngamsLib,\
    ngamsHttpUtils
from ngamsLib import ngamsReqProps, ngamsFileInfo, ngamsDiskInfo, ngamsStatus, ngamsDiskUtils
from . import ngamsFileUtils
from . import ngamsCacheControlThread


logger = logging.getLogger(__name__)

# Dictionary to keep track of disk space warnings issued.
_diskSpaceWarningDic = {}

VOLUME_STRATEGY_RANDOM = 0
VOLUME_STRATEGY_STREAMS = 1

class PluginNotFoundError(Exception):
    """Raised when a configured plug-in cannot be loaded"""
    pass

def _random_target_volume(srv):

    # Get a random volume from the list of available volumes
    # This should balance load the disk utilization
    res = srv.getDb().getAvailableVolumes(srv.getHostId())
    if not res:
        return None

    # Shuffle the results.
    res = list(res)
    random.shuffle(res)
    return ngamsDiskInfo.ngamsDiskInfo().unpackSqlResult(res[0])

def _stream_target_volume(srvObj, mimeType, file_uri, size):
    try:
        return ngamsDiskUtils.findTargetDisk(srvObj.getHostId(),
                                             srvObj.getDb(), srvObj.getCfg(),
                                             mimeType, 0, caching=0,
                                             reqSpace=size)
    except Exception as e:
        errMsg = str(e) + ". Attempting to archive file: %s" % file_uri
        ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(), NGAMS_NOTIF_NO_DISKS,
                                  "NO DISKS AVAILABLE", errMsg)
        raise


class eof_found(Exception):
    """Throw by archive_contents if the EOF is found while reading the incoming data"""
    pass

class too_much_data(Exception):
    """Throw by archive_contents if the EOF is found while reading the incoming data"""
    pass

archiving_results = collections.namedtuple('archiving_results',
                                           'size rtime wtime crctime totaltime crcname crc')

def archive_contents(out_fname, fin, fsize, block_size, crc_name, skip_crc=False):
    """
    Archives the contents read from `fin` (a file-like object with .read()
    support) and writes it to file `out_fname`, which is opened in write mode
    and truncated. While reading the data its checksum is calculated using the
    checksum method indicated by `crc_variant`.

    This method returns an archiving_results tuple populated with all the
    corresponding fields.
    """

    # Get the CRC method to be used and initialize CRC value
    crc_info = None
    crc_m = None
    crc = None
    if not skip_crc:
        crc_info = ngamsFileUtils.get_checksum_info(crc_name)
        if crc_info:
            crc_m = crc_info.method
            crc = crc_info.init

    crctime = 0
    rtime = 0
    wtime = 0
    readin = 0

    logger.debug("Saving data in file: %s", out_fname)

    start = time.time()
    with open(out_fname, 'wb') as fout:
        while readin < fsize:

            left = fsize - readin

            # Read
            rstart = time.time()
            buff = fin.read(block_size if left >= block_size else left)
            rtime += time.time() - rstart
            readin += len(buff)

            if not buff:
                raise eof_found("Only read %d out of %d bytes (%d bytes missing)"
                                % (readin, fsize, fsize - readin))

            # Write
            wstart = time.time()
            fout.write(buff)
            wtime += time.time() - wstart

            # CRC
            if crc_m:
                crcstart = time.time()
                crc = crc_m(buff, crc)
                crctime += time.time() - crcstart

    if crc_info:
        crc = crc_info.final(crc)

        # rtobar, 31 Aug 2017
        #
        # I've added these two lines here to ensure that the contents of the
        # file are actually safe on disk at this stage, but it's commented out
        # for the moment because I'm not fully sure this is the best place to do
        # this. It probably is because any problem that occurs later without
        # doing an explicit fsync() here would mean that not *all* the data
        # might have been saved to disk.
        #
        # On the other hand, the NGAS code already invokes a disk-sync plug-in
        # after each file archival (which syncs *the whole disk*!) if the
        # backlog buffering feature is on (which seems to be the case, at least
        # in our MWA setups). This later full-disk sync seems unnecessary
        # (backlog buffering or not), and it looks to me that an unconditional
        # fsync() is really what we want. Still, I prefer not to rush and try
        # to understand the reasoning behind the current approach (performance
        # is the only thing I can think of) before doing any changes.
        #
        # All the above means also that if I enable these two lines I'll simply
        # remove the full disk sync logic.

#        # Make sure all the content has been flushed from user-space caches...
#        fout.flush()
#        # ... and hits the disk
#        os.fsync(fout.fileno())

    total_time = time.time() - start

    if readin > fsize:
        raise too_much_data("Read %d bytes of data, but advertised size was %d (%d bytes too many)"
                            % (readin, fsize, readin - fsize))

    # Avoid divide by zeros later on, let's say it took us 1 [us] to do this
    if total_time == 0.0:
        total_time = 0.000001

    return archiving_results(readin, rtime, wtime, crctime, total_time, crc_name, crc)


def archive_contents_from_request(out_fname, cfg, req, rfile, skip_crc=False, transfer=None):
    """
    Inspects the given configuration and request objects, and calls
    archive_contents with the required arguments.
    """

    checkCreatePath(os.path.dirname(out_fname))

    # The CRC variant is configured in the server, but can be overridden
    # in a per-request basis
    if 'crc_variant' in req:
        variant = req['crc_variant']
    else:
        variant = cfg.getCRCVariant()
    crc_name = ngamsFileUtils.get_checksum_name(variant)

    def http_transfer(req, out_fname, crc_name, skip_crc):
        block_size = cfg.getBlockSize()
        size = req.getSize()
        return archive_contents(out_fname, rfile, size, block_size, crc_name, skip_crc)

    transfer = transfer or http_transfer
    result = transfer(req, out_fname, crc_name, skip_crc=skip_crc)

    req.incIoTime(result.rtime + result.wtime)
    req.setBytesReceived(result.size)
    ingestRate = result.size / result.totaltime / 1024. / 1024.

    # Compare checksum if required
    checksum = req.getHttpHdr(NGAMS_HTTP_HDR_CHECKSUM)
    checksum_info = ngamsFileUtils.get_checksum_info(variant)
    if checksum and result.crc is not None:
        if not checksum_info.equals(checksum, result.crc):
            msg = 'Checksum error for file %s, local crc = %s, but remote crc = %s' % (req.getFileUri(), str(result.crc), checksum)
            raise Exception(msg)
        else:
            logger.info("%s CRC checked, OK!", req.getFileUri())

    logger.debug('File size: %d; Transfer time: %.4f s; CRC time: %.4f s; write time %.4f s',
                 result.size, result.totaltime, result.crctime, result.wtime)
    logger.info('Saved data in file: %s. Bytes received: %d. Time: %.4f s. Rate: %.2f MB/s. Checksum (%s): %s',
                out_fname, result.size, result.totaltime, ingestRate, str(result.crcname), str(result.crc))

    return result

def updateFileInfoDb(srvObj,
                     piStat,
                     checksum,
                     checksumPlugIn, sync_disk=True, ingestion_rate=None):
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
    if sync_disk:
        try:
            ngamsFileUtils.syncCachesCheckFiles(srvObj,
                                                [piStat.getCompleteFilename()])
        except Exception as e:
            errMsg = "Severe error occurred! Cannot update information in " +\
                     "NGAS DB (ngas_files table) about file with File ID: " +\
                     piStat.getFileId() + " and File Version: " +\
                     str(piStat.getFileVersion()) + ", since file is not found " +\
                     "in the indicated, final storage location! Check system! " +\
                     "Error: " + str(e)
            raise Exception(errMsg)

    if (piStat.getStatus() == NGAMS_FAILURE):
        return

    # If there was a previous version of this file, and it had a container associated with it
    # associate the new version with the container too
    containerId = None
    file_version = piStat.getFileVersion()
    if file_version > 1:
        fileInfo = ngamsFileInfo.ngamsFileInfo()
        fileInfo.read(srvObj.getHostId(),srvObj.getDb(), piStat.getFileId(),
                      fileVersion=(file_version - 1))
        containerId = fileInfo.getContainerId()
        prevSize = fileInfo.getUncompressedFileSize()

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
               setIoTime(piStat.getIoTime()).\
               setIgnore(0)
    if ingestion_rate is not None:
        fileInfo.setIngestionRate(ingestion_rate)

    fileInfo.write(srvObj.getHostId(), srvObj.getDb())
    logger.debug("Updated file info in NGAS DB for file with ID: %s", piStat.getFileId())

    # Update the container size with the new size
    if containerId:
        newSize = fileInfo.getUncompressedFileSize()
        srvObj.getDb().addFileToContainer(containerId, piStat.getFileId(), True)
        srvObj.getDb().addToContainerSize(containerId, (newSize - prevSize))

    return fileInfo

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
    if (ngamsCfgObj.getAssocSlotId(piStat.getSlotId()) == ""):
        logger.debug("No Replication Disk is configured for the Main Disk in Slot "+\
             "with ID: %s - no replication performed", piStat.getSlotId())
        return None
    else:
        logger.debug("Replicating file: %s", piStat.getRelFilename())

    # Get the ID for the Replication Disk.
    setObj = ngamsCfgObj.getStorageSetFromSlotId(piStat.getSlotId())
    if setObj.getRepDiskSlotId() not in diskDic:
        raise Exception("Error handling Archive Request - no Replication " +\
              "Disk found according to configuration. Replication Disk " +\
              "Slot ID: " + str(setObj.getRepDiskSlotId()))
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
    global _diskSpaceWarningDic
    if diskId not in _diskSpaceWarningDic:
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
                   mainDiskId,
                   mainDiskInfo=None):
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
    if mainDiskInfo is None:
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
                          tgtDiskInfo,
                          cksum=None, sync_disk=True, ingestion_rate=None,
                          do_replication=True):
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

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Data returned from Data Archiving Plug-In: %r", resultPlugIn)

    # if checksum is already supplied then do not calculate it from the plugin
    if (cksum == None):
        # Calculate checksum (if plug-in specified).
        checksumPlugIn = srvObj.getCfg().getChecksumPlugIn()
        if (checksumPlugIn != ""):
            logger.info("Invoking Checksum Plug-In: %s to handle file: %s",
                         checksumPlugIn, resultPlugIn.getCompleteFilename())
            plugInMethod = loadPlugInEntryPoint(checksumPlugIn)
            checksum = plugInMethod(srvObj, resultPlugIn.getCompleteFilename(), 0)
            logger.info("Result: %s", checksum)
        else:
            checksum = ''
            checksumPlugIn = ''
    else:
        checksum, checksumPlugIn = cksum

    # Update information for File in DB.
    fileInfo = updateFileInfoDb(srvObj, resultPlugIn, checksum, checksumPlugIn,
                     sync_disk=sync_disk, ingestion_rate=ingestion_rate)
    ngamsLib.makeFileReadOnly(resultPlugIn.getCompleteFilename())

    # Update information about main disk
    # TODO: This doesn't handle (yet) the fact that we might be overwriting
    #       an existing file (and therefore the number of files shouldn't go up),
    #       the new amount of available space, and the accumulated amount of
    #       I/O spent on the disk (who cares?). We are only updated these values
    #       on the disk object in memory, while on the database we perform only
    #       certain updates.
    #       The routine commented below did all this, but it reads from the DB
    #       and then writes back again, which is not exactly great either.
    if not resultPlugIn.getFileExists():
        tgtDiskInfo.setNumberOfFiles(tgtDiskInfo.getNumberOfFiles() + 1)
    tgtDiskInfo.setBytesStored(tgtDiskInfo.getBytesStored() + resultPlugIn.getFileSize())
    tgtDiskInfo.setTotalDiskWriteTime(tgtDiskInfo.getTotalDiskWriteTime() + resultPlugIn.getIoTime())
    srvObj.getDb().updateDiskInfo(resultPlugIn.getFileSize(), resultPlugIn.getDiskId())
#     mainDiskInfo = ngamsDiskUtils.updateDiskStatusDb(srvObj.getDb(),
#                                                      resultPlugIn)

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
    if do_replication and srvObj.getCfg().getReplication():
        assocSlotId = srvObj.getCfg().getAssocSlotId(resultPlugIn.getSlotId())
        if ((not reqPropsObj.getNoReplication()) and (assocSlotId != "")):
            resRep = replicateFile(srvObj.getDb(), srvObj.getCfg(),
                                   srvObj.getDiskDic(), resultPlugIn)
            updateFileInfoDb(srvObj, resRep, checksum, checksumPlugIn,
                             sync_disk=sync_disk)
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
    checkDiskSpace(srvObj, resultPlugIn.getDiskId(), tgtDiskInfo)

    # Return these to the user in a status document
    tgtDiskInfo.addFileObj(fileInfo)
    return tgtDiskInfo


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
            except Exception as e:
                errMsg = str(e) + ". Attempting to archive local file: " +\
                         filename
                ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(),
                                         NGAMS_NOTIF_NO_DISKS,
                                         "NO DISKS AVAILABLE", errMsg)
                raise Exception(errMsg)

        # Set the log cache to 1 during the handling of the file.
        plugIn = srvObj.getMimeTypeDic()[mimeType]
        logger.info("Invoking DAPI: %s", plugIn)
        plugInMethod = loadPlugInEntryPoint(plugIn)
        resMain = plugInMethod(srvObj, reqPropsObjLoc)
        # Move the file to final destination.
        mvFile(reqPropsObjLoc.getStagingFilename(),
               resMain.getCompleteFilename())

        postFileRecepHandling(srvObj, reqPropsObjLoc, resMain, trgDiskInfo)
    except Exception as e:
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
            ngamsHighLevelLib.moveFile2BadDir(srvObj.getCfg(), filename)
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
        with open(reqPropsFile, "rb") as fo:
            tmpReqPropObj = cPickle.load(fo).setStagingFilename(backLogBufFile)

        with open(tmpBackLogBufReqFile, "wb") as fo:
            cPickle.dump(tmpReqPropObj, fo)

        mvFile(tmpBackLogBufFile, backLogBufFile)
        mvFile(tmpBackLogBufReqFile, backLogBufReqFile)
        ngamsFileUtils.syncCachesCheckFiles(srvObj, [backLogBufFile,
                                                     backLogBufReqFile])
        rmFile(stagingFile)
        rmFile(reqPropsFile)
    except Exception as e:
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
            except Exception as e:
                errMsg = "Error encountered trying to load pickled " +\
                         "Request Properties Object from file: " +\
                         pickleObjFile + ".  Error: " + str(e)
                if (str(e).find("[Errno 2]") != -1):
                    logger.warning(errMsg)
                    reqPropsObj = None
                else:
                    raise Exception(errMsg)
            if (archiveFromFile(srvObj, file, 0, None,
                                reqPropsObj) == NGAMS_SUCCESS):
                rmFile(pickleObjFile)


def cleanUpStagingArea(tmpStagingFilename,
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

    tmpStagingFilename:    Temporary Staging File (string).

    stagingFilename:       Staging File (string).

    tmpReqPropsFilename:   Temporary Request Properties File (string).

    reqPropsFilename:      Request Properties File (string).

    Returns:               Void.
    """

    # All Staging Files can be deleted.
    stgFiles = [tmpStagingFilename, stagingFilename, tmpReqPropsFilename,
                reqPropsFilename]
    for stgFile in stgFiles:
        if (stgFile):
            logger.warning("Removing Staging File: %s", stgFile)
            rmFile(stgFile)


def archiveInitHandling(srvObj, reqPropsObj, httpRef, do_probe=False, try_to_proxy=False):
    """
    Handle the initialization of the ARCHIVE Command.

    For a description of the signature: Check handleCmdArchive().

    Returns:   Mime-type of the request or None if the request has been
               handled and reply sent back (string|None).
    """

    # Is this NG/AMS permitted to handle Archive Requests?
    if (not srvObj.getCfg().getAllowArchiveReq()):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Archive"])
        raise Exception(errMsg)
    srvObj.checkSetState("Archive Request", [NGAMS_ONLINE_STATE],
                         [NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE],
                         NGAMS_ONLINE_STATE, NGAMS_BUSY_SUBSTATE,
                         updateDb=False)

    # Ensure we have the mime-type.
    mimeType = reqPropsObj.getMimeType()
    if not mimeType:
        mimeType = ngamsHighLevelLib.\
                   determineMimeType(srvObj.getCfg(), reqPropsObj.getFileUri())
        reqPropsObj.setMimeType(mimeType)

    # This is a request probing for capability of handling the request.
    if do_probe:
        try:
            ngamsDiskUtils.findTargetDisk(srvObj.getHostId(),
                                          srvObj.getDb(), srvObj.getCfg(),
                                          mimeType, sendNotification=0)
            msg = genLog("NGAMS_INFO_ARCH_REQ_OK",
                         [mimeType, getHostName()])
        except Exception:
            msg = genLog("NGAMS_ER_ARCH_REQ_NOK",
                         [mimeType, getHostName()])

        httpRef.send_status(msg)
        return

    # Check if the URI is correctly set.
    if not reqPropsObj.getFileUri():
        errMsg = genLog("NGAMS_ER_MISSING_URI")
        raise Exception(errMsg)

    # Act possibly as proxy for the Achive Request?
    # TODO: Support maybe HTTP redirection also for Archive Requests.
    if (try_to_proxy and
        srvObj.getCfg().getStreamFromMimeType(mimeType).getHostIdList()):
        host_id, host, port = findTargetNode(srvObj, mimeType)
        if host_id != srvObj.getHostId():
            httpRef.proxy_request(host_id, host, port)
            return None

    return mimeType

def dataHandler(srv, request, httpRef, volume_strategy=VOLUME_STRATEGY_STREAMS,
                pickle_request=True, sync_disk=True, do_replication=True,
                transfer=None):

    # Choose the method to select which volume will host the incoming data
    if volume_strategy == VOLUME_STRATEGY_RANDOM:
        find_target_disk = _random_target_volume
    elif volume_strategy == VOLUME_STRATEGY_STREAMS:
        def find_target_disk(srv):
            mimeType = request.getMimeType()
            file_uri = request.getFileUri()
            size = request.getSize()
            return _stream_target_volume(srv, mimeType, file_uri, size)
    else:
        raise Exception("Unknown volume selection strategy: %d", volume_strategy)

    # Thin wrapper around _dataHandler to send notifications, just in case
    try:
        _dataHandler(srv, request, httpRef, find_target_disk,
                     pickle_request=pickle_request, sync_disk=sync_disk,
                     do_replication=do_replication, transfer=transfer)
    except PluginNotFoundError as e:
        srv.setSubState(NGAMS_IDLE_SUBSTATE)
        httpRef.send_status('No module named %s' % e.args[0], status=NGAMS_FAILURE)
        return
    except Exception as e:
        try:
            errMsg = genLog("NGAMS_ER_ARCHIVE_PUSH_REQ",
                            [request.getSafeFileUri(), str(e)])
            ngamsNotification.notify(srv.getHostId(), srv.getCfg(), NGAMS_NOTIF_ERROR,
                                     "PROBLEM ARCHIVE HANDLING", errMsg)
        except:
            logger.exception('Unexpected error while trying to notify archiving error')
        raise

def _dataHandler(srvObj, reqPropsObj, httpRef, find_target_disk,
                pickle_request, sync_disk, do_replication, transfer):

    cfg = srvObj.getCfg()

    # GET means pull, POST is push
    if (reqPropsObj.getHttpMethod() == NGAMS_HTTP_GET):
        logger.info("Handling archive pull request")

        # Default to absolute path file:// scheme if url has no schema
        url = reqPropsObj.getFileUri()
        if not urlparse.urlparse(url).scheme:
            url = 'file://' + os.path.abspath(os.path.normpath(url))

        handle = urlrequest.urlopen(url)
        # urllib.urlopen will attempt to get the content-length based on the URI
        # i.e. file, ftp, http
        reqPropsObj.setSize(handle.info()['Content-Length'])
        rfile = handle
    else:
        logger.info("Handling archive push request")
        rfile = httpRef.rfile

    logger.info(genLog("NGAMS_INFO_ARCHIVING_FILE", [reqPropsObj.getFileUri()]), extra={'to_syslog': True})

    if reqPropsObj.getSize() <= 0:
        raise Exception('Content-Length is 0')

    mimeType = reqPropsObj.getMimeType()
    archiving_start = time.time()

    logger.info("Archiving file: %s with mime-type: %s",
                reqPropsObj.getSafeFileUri(), mimeType)
    tmpStagingFilename = stagingFilename = tmpReqPropsFilename =\
                         reqPropsFilename = None
    try:

        # Generate target filename. Remember to set this in the Request Object.
        trgDiskInfo = find_target_disk(srvObj)
        reqPropsObj.setTargDiskInfo(trgDiskInfo)
        if trgDiskInfo is None:
            errMsg = "No disk volumes are available for ingesting any files."
            raise Exception(errMsg)

        # Generate Staging Filename + Temp Staging File + save data in this
        # file. Also Org. Staging Filename is created, Processing Staging
        # Filename and the Temp. Req. Props. File and Req. Props. File.
        tmpStagingFilename, stagingFilename,\
                            tmpReqPropsFilename,\
                            reqPropsFilename = ngamsHighLevelLib.\
                            genStagingFilename(cfg, reqPropsObj,
                                               trgDiskInfo, reqPropsObj.getFileUri(),
                                               genTmpFiles=1)

        # Check if we can directly perform checksum calculation at reception time;
        # otherwise we must postpone it until the data archiving plug-in is executed,
        # since it can potentially change the data.
        # This method is optional, in which case it is assumed the data is changed
        skip_crc = True
        plugIn = srvObj.getMimeTypeDic()[mimeType]
        try:
            modifies = loadPlugInEntryPoint(plugIn,
                                            entryPointMethodName='modifies_content',
                                            returnNone=True)
            if modifies:
                skip_crc = modifies(srvObj, reqPropsObj)
        except ImportError:
            raise PluginNotFoundError(plugIn)

        try:
            ngamsHighLevelLib.acquireDiskResource(cfg, trgDiskInfo.getSlotId())
            archive_result = archive_contents_from_request(tmpStagingFilename, cfg, reqPropsObj,
                                                           rfile, skip_crc=skip_crc, transfer=transfer)
        finally:
            ngamsHighLevelLib.releaseDiskResource(cfg, trgDiskInfo.getSlotId())

        logger.debug("Move Temporary Staging File to Processing Staging File: %s -> %s",
                     tmpStagingFilename, stagingFilename)
        mvFile(tmpStagingFilename, stagingFilename)

        # Pickle the request object if necessary
        if pickle_request:
            logger.debug("Create Temporary Request Properties File: %s", tmpReqPropsFilename)
            tmpReqPropsObj = reqPropsObj.clone().setTargDiskInfo(None)
            ngamsLib.createObjPickleFile(tmpReqPropsFilename, tmpReqPropsObj)
            logger.debug("Move Temporary Request Properties File to Request " + \
                         "Properties File: %s -> %s",
                         tmpReqPropsFilename, reqPropsFilename)
            mvFile(tmpReqPropsFilename, reqPropsFilename)

        # Synchronize the file caches to ensure the files have been stored
        # on the disk and check that the files are accessible.
        # This sync is only relevant if back-log buffering is on.
        if sync_disk and cfg.getBackLogBuffering():
            ngamsFileUtils.syncCachesCheckFiles(srvObj, [stagingFilename,
                                                         reqPropsFilename])

        # Invoke the Data Archiving Plug-In.
        # In case the plug-in modifies data, it can return the checksum of this
        # modified data (only if it makes sense from a performance point of
        # view, the default final option of re-reading the final file may be
        # the only way). In that case they need to know the checksum method to
        # use, which we pass down via the 'crc_name' parameter (which we later
        # remove).
        try:
            plugInMethod = loadPlugInEntryPoint(plugIn)
        except (ImportError, AttributeError):
            raise PluginNotFoundError(plugIn)

        logger.info("Invoking DAPI: %s to handle data for file with URI: %s",
                    plugIn, os.path.basename(reqPropsObj.getFileUri()))

        timeBeforeDapi = time.time()
        reqPropsObj.addHttpPar('crc_name', archive_result.crcname)
        plugin_result = plugInMethod(srvObj, reqPropsObj)
        del reqPropsObj.getHttpParsDic()['crc_name']
        logger.debug("Invoked DAPI: %s. Time: %.3fs.", plugIn, (time.time() - timeBeforeDapi))

        # Move the file to final destination.
        ioTime = mvFile(reqPropsObj.getStagingFilename(),
                        plugin_result.getCompleteFilename())
        reqPropsObj.incIoTime(ioTime)

        # Remember to set the final IO time in the plug-in status object.
        plugin_result.setIoTime(reqPropsObj.getIoTime())

    except Exception as e:
        if (str(e).find("NGAMS_ER_DAPI_BAD_FILE") != -1):
            errMsg = "Problems during archiving! URI: " +\
                     reqPropsObj.getFileUri() + ". Exception: " + str(e)
            cleanUpStagingArea(tmpStagingFilename,
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
        elif (ngamsHighLevelLib.performBackLogBuffering(cfg, reqPropsObj, e)):
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
            cleanUpStagingArea(tmpStagingFilename,
                               stagingFilename, tmpReqPropsFilename,
                               reqPropsFilename)
        raise

    # Backwards compatibility for the ARCHIVE command based on plug-in'ed CRCs
    crc_name = archive_result.crcname
    if reqPropsObj.getCmd() == 'ARCHIVE' and crc_name == 'crc32':
        crc_name = 'ngamsGenCrc32'

    # Checksum could have been calculated during archiving or by the DAPI
    # Worst case scenario: we calculate it now at the very end by re-reading
    # the stating file
    cksum = None
    if archive_result.crc is not None:
        cksum = (archive_result.crc, crc_name)
    elif plugin_result.crc is not None:
        cksum = (plugin_result.crc, crc_name)
    elif crc_name is None:
        cksum = (None, None)

    intestion_rate = archive_result.totaltime / reqPropsObj.getSize()
    diskInfo = postFileRecepHandling(srvObj, reqPropsObj, plugin_result,
                                     reqPropsObj.getTargDiskInfo(), cksum=cksum,
                                     sync_disk=sync_disk, ingestion_rate=intestion_rate,
                                     do_replication=do_replication)
    msg = genLog("NGAMS_INFO_FILE_ARCHIVED", [reqPropsObj.getSafeFileUri()])
    msg = msg + ". Time: %.3fs" % (time.time() - archiving_start)
    logger.info(msg, extra={'to_syslog': True})

    # Remove back-up files (Original Staging File + Request Properties File.
    srvObj.test_BeforeArchCleanUp()
    logger.debug("Removing Request Properties File: %s", reqPropsFilename)
    if (reqPropsFilename): rmFile(reqPropsFilename)

    # Request after-math
    srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
    msg = "Successfully handled Archive %s Request for data file with URI: %s"
    msg = msg % ('Pull' if reqPropsObj.is_GET() else 'Push', reqPropsObj.getSafeFileUri())
    logger.info(msg)

    httpRef.send_ingest_status(msg, diskInfo)

    # After a successful archiving we notify the archive event subscribers
    srvObj.fire_archive_event(plugin_result.getFileId(), plugin_result.getFileVersion())

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
            except Exception as e:
                if (str(e).find("NGAMS_AL_NO_STO_SETS") != -1):
                    logMsg = "Local node: %s cannot handle Archive " +\
                             "Request for data file with mime-type: %s"
                    logger.debug(logMsg, getHostName(), mimeType)
                    continue
                raise

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
                logger.info(logMsg, host, port, mimeType)
                return hostId, host, port

            logMsg = "Remote Archiving Unit: %s:%d rejected to/" +\
                     "could not handle Archive Request for data " +\
                     "file with mime-type: %s"
            logger.debug(logMsg, host, port, mimeType)
            continue

        except Exception as e:
            # The request handling failed for some reason, give up this
            # host for now.
            logMsg = "Problem contacting remote Archiving Unit: %s:%d: %s. " +\
                     "Skipping node."
            logger.warning(logMsg, host, port, str(e))
            continue

    errMsg = genLog("NGAMS_AL_NO_STO_SETS", [mimeType])
    raise Exception(errMsg)


# EOF
