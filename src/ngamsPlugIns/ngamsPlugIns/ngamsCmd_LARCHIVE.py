#
#    ALMA - Atacama Large Millimiter Array
#    (c) European Southern Observatory, 2002
#    Copyright by ESO (in the framework of the ALMA collaboration),
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
"""
NGAS Command Plug-In, implementing a Local Archive Command.

This works by calling archiveFromFile, which in turn takes care of all the handling

Usgae example with wget:

wget -O LARCHIVE.xml "http://192.168.1.123:7777/LARCHIVE?fileUri=/home/ngas/NGAS/log/LogFile.nglog"
"""

import logging
import os
import time

from ngamsLib import ngamsHighLevelLib, ngamsPlugInApi
from ngamsLib.ngamsCore import TRACE, NGAMS_HTTP_GET, \
    NGAMS_ARCHIVE_CMD, NGAMS_HTTP_FILE_URL, cpFile, NGAMS_NOTIF_NO_DISKS, \
    mvFile, NGAMS_FAILURE, NGAMS_PICKLE_FILE_EXT, \
    rmFile, genLog, NGAMS_ONLINE_STATE, NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE, \
    getDiskSpaceAvail, loadPlugInEntryPoint, NGAMS_TEXT_MT
from ngamsServer import ngamsArchiveUtils, ngamsCacheControlThread


logger = logging.getLogger(__name__)

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

    Returns:         Execution result object of DAPI
    """
    T = TRACE()

    logger.debug("Archiving file: %s", filename)
    logger.debug("Mimetype used is %s", mimeType)
    if (reqPropsObj):
        logger.debug("Request Properties Object given - using this")
        reqPropsObjLoc = reqPropsObj
    else:
        logger.debug("No Request Properties Object given - creating one")
        reqPropsObjLoc = ngamsArchiveUtils.ngamsReqProps.ngamsReqProps()
    stagingFile = filename
    try:
        if (mimeType == None):
            mimeType = ngamsHighLevelLib.determineMimeType(srvObj.getCfg(),
                                                           filename)
        archive_start = time.time()

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
                trgDiskInfo = ngamsArchiveUtils.ngamsDiskUtils.\
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
                ngamsPlugInApi.notify(srvObj,
                                         NGAMS_NOTIF_NO_DISKS,
                                         "NO DISKS AVAILABLE", errMsg)
                raise Exception(errMsg)

        # Set the log cache to 1 during the handling of the file.
        plugIn = srvObj.getMimeTypeDic()[mimeType]
        logger.debug("Invoking DAPI: %s to handle file: %s", plugIn, stagingFile)
        plugInMethod = loadPlugInEntryPoint(plugIn)
        resMain = plugInMethod(srvObj, reqPropsObjLoc)
        # Move the file to final destination.
        st = time.time()
        mvFile(reqPropsObjLoc.getStagingFilename(),
               resMain.getCompleteFilename())
        iorate = reqPropsObjLoc.getSize()/(time.time() - st)

        ngamsArchiveUtils.postFileRecepHandling(srvObj, reqPropsObjLoc, resMain,
                                                trgDiskInfo)
    except Exception as e:
        # If another error occurs, than one qualifying for Back-Log
        # Buffering the file, we have to log an error.
        if (ngamsHighLevelLib.performBackLogBuffering(srvObj.getCfg(),
                                                      reqPropsObjLoc, e)):
            logger.exception("Tried to archive local file %s, keeping original file", filename)
            return [NGAMS_FAILURE, str(e), NGAMS_FAILURE]
        else:
            logger.exception("Tried to archive local file %s, " + \
                             "moving it to Bad Files Directory " + \
                             "-- cannot be handled", filename)
            ngamsHighLevelLib.moveFile2BadDir(srvObj.getCfg(), filename)
            # Remove pickle file if available.
            pickleObjFile = filename + "." + NGAMS_PICKLE_FILE_EXT
            if (os.path.exists(pickleObjFile)):
                logger.debug("Removing Back-Log Buffer Pickle File: %s", pickleObjFile)
                rmFile(pickleObjFile)
            return [NGAMS_FAILURE, str(e), NGAMS_FAILURE]

    # If the file was handled successfully, we remove it from the
    # Back-Log Buffer Directory unless the local file was a log-file
    # in which case we leave the cleanup to the Janitor-Thread.
    if stagingFile.find('LOG-ROTATE') > -1:
        logger.debug("Successfully archived local file: %s", filename)
    else:
        logger.debug("Successfully archived local file: %s. Removing staging file.", filename)
        rmFile(stagingFile)
        rmFile(stagingFile + "." + NGAMS_PICKLE_FILE_EXT)

    logger.debug("Archived local file: %s. Time (s): %.3f",
                 filename, time.time() - archive_start)
    return (resMain, trgDiskInfo, iorate)



def handleCmd(srvObj,
              reqPropsObj,
              httpRef):
    """
    Handle the Quick Archive (QARCHIVE) Command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        (fileId, filePath) tuple.
    """
    T = TRACE()

    # Check if the URI is correctly set.
    logger.debug("Check if the URI is correctly set.")
    logger.debug("ReqPropsObj status: %s", reqPropsObj.getObjStatus())
    parsDic = reqPropsObj.getHttpParsDic()
    if (not parsDic.has_key('fileUri') or parsDic['fileUri'] == ""):
        errMsg = genLog("NGAMS_ER_MISSING_URI")
        raise Exception(errMsg)
    else:
        reqPropsObj.setFileUri(parsDic['fileUri'])
        fileUri = reqPropsObj.getFileUri()
    # Is this NG/AMS permitted to handle Archive Requests?
    logger.debug("Is this NG/AMS permitted to handle Archive Requests?")
    if (not srvObj.getCfg().getAllowArchiveReq()):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Archive"])
        raise Exception(errMsg)
    srvObj.checkSetState("Archive Request", [NGAMS_ONLINE_STATE],
                         [NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE],
                         NGAMS_ONLINE_STATE, NGAMS_BUSY_SUBSTATE,
                         updateDb=False)

    # Get mime-type (try to guess if not provided as an HTTP parameter).
    logger.debug("Get mime-type (try to guess if not provided as an HTTP parameter).")
    if (not parsDic.has_key('mimeType') or parsDic['mimeType'] == ""):
        mimeType = ""
        reqPropsObj.setMimeType("")
    else:
        reqPropsObj.setMimeType(parsDic['mimeType'])
        mimeType = reqPropsObj.getMimeType()

    if (reqPropsObj.getMimeType() == ""):
        mimeType = ngamsHighLevelLib.\
                   determineMimeType(srvObj.getCfg(), reqPropsObj.getFileUri())
        reqPropsObj.setMimeType(mimeType)
    else:
        mimeType = reqPropsObj.getMimeType()

    ioTime = 0
    reqPropsObj.incIoTime(ioTime)

    (resDapi, targDiskInfo, iorate) = archiveFromFile(srvObj, fileUri, 0, mimeType, reqPropsObj)
    if (resDapi == NGAMS_FAILURE):
        errMsg = targDiskInfo
        httpRef.send_data(errMsg, NGAMS_TEXT_MT, code=500)
        return

    # Get crc info
#     info(3, "Get checksum info")
#     crc = None

    # TODO: Investigate how CRC calculation could be performed best.
#     checksumPlugIn = "ngamsGenCrc32"
#     checksum = str(crc)
#     info(3, "Invoked Checksum Plug-In: " + checksumPlugIn +\
#             " to handle file: " + resDapi.getCompleteFilename() +\
#             ". Result: " + checksum)


    # Inform the caching service about the new file.
    logger.debug("Inform the caching service about the new file.")
    if (srvObj.getCachingActive()):
        diskId      = resDapi.getDiskId()
        fileId      = resDapi.getFileId()
        fileVersion = 1
        filename    = resDapi.getRelFilename()
        ngamsCacheControlThread.addEntryNewFilesDbm(srvObj, diskId, fileId,
                                                   fileVersion, filename)

    # Update disk info in NGAS Disks.
    logger.debug("Update disk info in NGAS Disks.")
    srvObj.getDb().updateDiskInfo(resDapi.getFileSize(), resDapi.getDiskId())

    # Check if the disk is completed.
    # We use an approximate estimate for the remaning disk space to avoid
    # to read the DB.
    logger.debug("Check available space in disk")
    availSpace = getDiskSpaceAvail(targDiskInfo.getMountPoint(), smart=False)
    if (availSpace < srvObj.getCfg().getFreeSpaceDiskChangeMb()):
        targDiskInfo.setCompleted(1).setCompletionDate(time.time())
        targDiskInfo.write(srvObj.getDb())

    # Request after-math ...
    srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
    msg = "Successfully handled Archive Pull Request for data file " +\
          "with URI: " + reqPropsObj.getSafeFileUri()
    logger.info(msg)
    httpRef.send_ingest_status(msg, targDiskInfo)

    # Trigger Subscription Thread. This is a special version for MWA, in which we simply swapped MIRRARCHIVE and QARCHIVE
    # chen.wu@icrar.org
    logger.debug("triggering SubscriptionThread for file %s", resDapi.getFileId())
    srvObj.addSubscriptionInfo([(resDapi.getFileId(),
                                 resDapi.getFileVersion())], [])
    srvObj.triggerSubscriptionThread()


    return (resDapi.getFileId(), '%s/%s' % (targDiskInfo.getMountPoint(), resDapi.getRelFilename()),
            iorate)

# EOF
