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

import time, base64

import PccUtTime

from ngams import *
import ngamsDb, ngamsLib, ngamsStatus
import ngamsHighLevelLib, ngamsDiskUtils, ngamsFileUtils
import ngamsFileInfo, ngamsArchiveCmd
import ngamsCacheControlThread


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
                             findTargetDisk(srvObj.getDb(), srvObj.getCfg(),
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
        timer = PccUtTime.Timer()
        code, msg, hdrs, data = ngamsLib.\
                                httpGetUrl(reqPropsObj.getFileUri(),
                                           dataTargFile = stagingFilename)
        reqPropsObj.incIoTime(timer.stop())

        # Check if the retrival was successfull.
        if (int(code) != NGAMS_HTTP_SUCCESS):
            try:
                statObj = ngamsStatus.ngamsStatus().unpackXmlDoc(data)
                msg = statObj.getMessage()
            except:
                msg = "Error retrieving file via URL: %s" %\
                      reqPropsObj.getFileUri()
            raise Exception, msg
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
    ngamsFileUtils.checkChecksum(srvObj, fileInfoObj,
                                 reqPropsObj.getStagingFilename())

    # Generate the DB File Information.
    newFileInfoObj = fileInfoObj.clone().\
                     setDiskId(trgDiskInfoObj.getDiskId()).\
                     setCreationDateFromSecs(int(time.time() + 0.5))
        
    # Generate the final storage location and move the file there.
    targetFilename = os.path.normpath("%s/%s" %\
                                      (trgDiskInfoObj.getMountPoint(),
                                       newFileInfoObj.getFilename()))
    info(3,"Move Restore Staging File to final destination: %s->%s ..." %\
         (reqPropsObj.getStagingFilename(), targetFilename))
    timer = PccUtTime.Timer()
    mvFile(reqPropsObj.getStagingFilename(), targetFilename)
    ioTime = timer.stop()
    reqPropsObj.incIoTime(ioTime)
    info(3,"Moved Restore Staging File to final destination: %s->%s" %\
         (reqPropsObj.getStagingFilename(), targetFilename))

    # Update the DB with the information about the new file.
    # Update information for Main File/Disk in DB.
    newFileInfoObj.write(srvObj.getDb())
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
    
    archiveTimer = PccUtTime.Timer()

    # Execute the init procedure for the ARCHIVE Command.
    mimeType = ngamsArchiveCmd.archiveInitHandling(srvObj, reqPropsObj,
                                                   httpRef)
    # If mime-type is None, the request has been handled, i.e., it might have
    # been a probe request or the server acting as proxy.
    if (not mimeType): return
    info(1, "Archiving file: " + reqPropsObj.getSafeFileUri() +\
         " with mime-type: " + mimeType + " ...")

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
    msg = msg + ". Time: %.6fs" % (archiveTimer.stop())
    sysLogInfo(1, msg)
    info(1, msg)
    
    srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
    srvObj.ingestReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                       NGAMS_SUCCESS, msg, trgDiskInfoObj)
   

# EOF
