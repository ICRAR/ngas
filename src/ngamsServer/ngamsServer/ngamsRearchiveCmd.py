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
# "@(#) $Id: ngamsRearchiveCmd.py,v 1.9 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  24/04/03    Created
#

"""
Contains the functions to handle the REARCHIVE command.

It is possible to re-archive files via push or pull technique; when using
former, the data of the file is provided in the request, using the second,
the file is pulled via a provided URL from the remote, host node.
"""

import logging
import os
import time, base64

import ngamsArchiveCmd, ngamsFileUtils, ngamsCacheControlThread
from ngamsLib import ngamsStatus, ngamsLib
from ngamsLib import ngamsFileInfo
from ngamsLib import ngamsHighLevelLib, ngamsDiskUtils
from ngamsLib.ngamsCore import TRACE, NGAMS_HTTP_HDR_FILE_INFO, NGAMS_HTTP_GET, \
    NGAMS_HTTP_SUCCESS, mvFile, getDiskSpaceAvail, genLog, \
    NGAMS_IDLE_SUBSTATE, NGAMS_SUCCESS


logger = logging.getLogger(__name__)

def receiveData(srvObj,
                reqPropsObj,
                httpRef):
    """
    Receive the data in connection with the Rearchive Request.

    For a description of the parameters: Check handleCmdRearchive().

    Returns:   Tuple with File Info Object for the file to be rearchived and
               Disk Info Object for the selected target disk
               (tuple/(fileInfo, ngamsDiskInfo)).
    """
    T = TRACE()

    # Note, this algorithm does not implement support for back-log buffering
    # for speed optimization reasons.

    # Unpack the file information contained in the HTTP header
    # (NGAS-File-Info).
    encFileInfo = reqPropsObj.getHttpHdr(NGAMS_HTTP_HDR_FILE_INFO)
    if (not encFileInfo):
        msg = "Error. Must provide NGAS File Information " +\
              "(RFC 3548 encoded) in HTTP header NGAS-File-Info"
        raise Exception, msg
    fileInfoXml = base64.b64decode(encFileInfo)
    fileInfoObj = ngamsFileInfo.ngamsFileInfo().unpackXmlDoc(fileInfoXml)

    # Find the most suitable target Disk Set.
    trgDiskInfoObj = None
    diskExList = []
    while (True):
        try:
            tmpTrgDiskInfo = ngamsDiskUtils.\
                             findTargetDisk(srvObj.getHostId(),
                                            srvObj.getDb(), srvObj.getCfg(),
                                            reqPropsObj.getMimeType(),
                                            sendNotification = 0,
                                            diskExemptList = diskExList,
                                            reqSpace = reqPropsObj.getSize())
            if (srvObj.getDb().fileInDb(tmpTrgDiskInfo.getDiskId(),
                                        fileInfoObj.getFileId(),
                                        fileInfoObj.getFileVersion())):
                diskExList.append(tmpTrgDiskInfo.getDiskId())
            else:
                trgDiskInfoObj = tmpTrgDiskInfo
                break
        except Exception, e:
            msg = "Error locating target disk for REARCHIVE Command. Error: %s"
            msg = msg % str(e)
            raise Exception, msg

    # Generate Staging Filename + save file into this.
    storageSetId = trgDiskInfoObj.getStorageSetId()
    stagingFilename = ngamsHighLevelLib.\
                      genStagingFilename(srvObj.getCfg(), reqPropsObj,
                                         srvObj.getDiskDic(), storageSetId,
                                         reqPropsObj.getFileUri(),
                                         genTmpFiles = 0)
    reqPropsObj.setStagingFilename(stagingFilename)

    # Save the data into the Staging File.
    # If it is an Rearchive Pull Request, open the URL.
    if (reqPropsObj.getHttpMethod() == NGAMS_HTTP_GET):
        # urllib.urlopen will attempt to get the content-length based on the URI
        # i.e. file, ftp, http
        handle = ngamsHighLevelLib.openCheckUri(reqPropsObj.getFileUri())
        reqPropsObj.setSize(handle.info()['Content-Length'])
        reqPropsObj.setReadFd(handle)
        ioTime = ngamsHighLevelLib.saveInStagingFile(srvObj.getCfg(),
                                                     reqPropsObj,
                                                     stagingFilename,
                                                     trgDiskInfoObj)
        reqPropsObj.incIoTime(ioTime)
    else:
        try:
            reqPropsObj.setSize(fileInfoObj.getFileSize())
            ioTime = ngamsHighLevelLib.saveInStagingFile(srvObj.getCfg(),
                                                         reqPropsObj,
                                                         stagingFilename,
                                                         trgDiskInfoObj)
            reqPropsObj.incIoTime(ioTime)
        except Exception, e:
            reqPropsObj.setSize(0)
            raise e

    # Synchronize the file caches to ensure the files have been stored
    # on the disk and check that the files are accessible.
    # This sync is only relevant if back-log buffering is on.
    if (srvObj.getCfg().getBackLogBuffering()):
        ngamsFileUtils.syncCachesCheckFiles(srvObj, [stagingFilename])

    return (fileInfoObj, trgDiskInfoObj)


def processRequest(srvObj,
                   reqPropsObj,
                   httpRef,
                   fileInfoObj,
                   trgDiskInfoObj):
    """
    Process the Rearchive Request.

    For a description of the parameters: Check handleCmdRearchive().

    fileInfoObj:     File information for file to be restored (ngamsFileInfo).

    trgDiskInfoObj:  Disk Info Object for target disk (ngamsDiskInfo).

    Returns:         Void.
    """
    T = TRACE()

    # Check the consistency of the staging file via the provided DCPI and
    # checksum value.
    ngamsFileUtils.check_checksum(srvObj, fileInfoObj, reqPropsObj.getStagingFilename())

    # Generate the DB File Information.
    newFileInfoObj = fileInfoObj.clone().\
                     setDiskId(trgDiskInfoObj.getDiskId()).\
                     setCreationDate(time.time())

    # Generate the final storage location and move the file there.
    targetFilename = os.path.normpath("%s/%s" %\
                                      (trgDiskInfoObj.getMountPoint(),
                                       newFileInfoObj.getFilename()))
    logger.debug("Move Restore Staging File to final destination: %s->%s ...",
                 reqPropsObj.getStagingFilename(), targetFilename)

    io_start = time.time()
    mvFile(reqPropsObj.getStagingFilename(), targetFilename)
    ioTime = time.time() - io_start
    reqPropsObj.incIoTime(ioTime)
    logger.debug("Moved Restore Staging File to final destination: %s->%s",
                 reqPropsObj.getStagingFilename(), targetFilename)

    # Update the DB with the information about the new file.
    # Update information for Main File/Disk in DB.
    newFileInfoObj.write(srvObj.getHostId(), srvObj.getDb())
    diskSpace = getDiskSpaceAvail(trgDiskInfoObj.getMountPoint())
    newSize = (trgDiskInfoObj.getBytesStored() + newFileInfoObj.getFileSize())
    ioTime = (trgDiskInfoObj.getTotalDiskWriteTime() + reqPropsObj.getIoTime())
    trgDiskInfoObj.\
                     setNumberOfFiles(trgDiskInfoObj.getNumberOfFiles() + 1).\
                     setAvailableMb(diskSpace).setBytesStored(newSize).\
                     setTotalDiskWriteTime(ioTime).write(srvObj.getDb())
    # Ensure that the restored file is readonly.
    ngamsLib.makeFileReadOnly(targetFilename)
    # Add the file info object for the target file in the disk info object.
    trgDiskInfoObj.addFileObj(newFileInfoObj)


def handleCmdRearchive(srvObj,
                       reqPropsObj,
                       httpRef):
    """
    Handle REARCHIVE command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler object
                    (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    T = TRACE()

    archive_start = time.time()

    # Execute the init procedure for the ARCHIVE Command.
    mimeType = ngamsArchiveCmd.archiveInitHandling(srvObj, reqPropsObj,
                                                   httpRef)
    # If mime-type is None, the request has been handled, i.e., it might have
    # been a probe request or the server acting as proxy.
    if (not mimeType): return
    logger.debug("Archiving file: %s with mime-type: %s",
                 reqPropsObj.getSafeFileUri(), mimeType)

    fileInfoObj, trgDiskInfoObj = receiveData(srvObj, reqPropsObj, httpRef)

    processRequest(srvObj, reqPropsObj, httpRef, fileInfoObj, trgDiskInfoObj)

    # If running as a cache archive, update the Cache New Files DBM
    # with the information about the new file.
    if (srvObj.getCachingActive()):
        fileVer  = fileInfoObj.getFileVersion()
        ngamsCacheControlThread.addEntryNewFilesDbm(srvObj,
                                                    trgDiskInfoObj.getDiskId(),
                                                    fileInfoObj.getFileId(),
                                                    fileVer,
                                                    fileInfoObj.getFilename())

    # Create log/syslog entry for the successfulyl handled request.
    msg = genLog("NGAMS_INFO_FILE_ARCHIVED", [reqPropsObj.getSafeFileUri()])
    msg = msg + ". Time: %.6fs" % (time.time() - archive_start)
    logger.info(msg, extra={'to_syslog': True})

    srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
    srvObj.ingestReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                       NGAMS_SUCCESS, msg, trgDiskInfoObj)


# EOF
