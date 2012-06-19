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
# "@(#) $Id: ngamsCmd_QARCHIVE.py,v 1.6 2009/12/07 16:36:40 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  03/02/2009  Created
#

"""
NGAS Command Plug-In, implementing a Quick Archive Command.

This works in a similar way as the 'standard' ARCHIVE Command, but has been
simplified in a few ways:
  
  - No replication to a Replication Volume is carried out.
  - Target disks are selected randomly, disregarding the Streams/Storage Set
    mappings in the configuration. This means that 'volume load balancing' is
    provided.
  - Archive Proxy Mode is not supported.
  - No probing for storage availability is supported.
  - In general, less SQL queries are performed and the algorithm is more
    light-weight.
"""

import random

import pcc, PccUtTime
from ngams import *
import ngamsLib, ngamsStatus, ngamsDbm, ngamsDbCore, ngamsFileInfo
import ngamsDiskInfo, ngamsHighLevelLib


GET_AVAIL_VOLS_QUERY = "SELECT %s FROM ngas_disks nd WHERE completed=0 AND " +\
                       "host_id='%s'"


def getTargetVolume(srvObj):
    """
    Get a random target volume with availability.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).
        
    Returns:        Target volume object or None (ngamsDiskInfo | None).
    """
    T = TRACE()

    sqlQuery = GET_AVAIL_VOLS_QUERY % (ngamsDbCore.getNgasDisksCols(),
                                       getHostId())
    res = srvObj.getDb().query(sqlQuery, ignoreEmptyRes=0)
    if (res == [[]]):
        return None
    else:
        # Shuffle the results.
        random.shuffle(res[0])
        return ngamsDiskInfo.ngamsDiskInfo().unpackSqlResult(res[0][0])


def updateDiskInfo(srvObj,
                   resDapi):
    """
    Update the row for the volume hosting the new file.

    srvObj:    Reference to NG/AMS server class object (ngamsServer).

    resDapi:   Result returned from the DAPI (ngamsDapiStatus).

    Returns:   Void.
    """
    T = TRACE()

    sqlQuery = "UPDATE ngas_disks SET " +\
               "number_of_files=(number_of_files + 1), " +\
               "bytes_stored=(bytes_stored + %d) WHERE " +\
               "disk_id='%s'"
    sqlQuery = sqlQuery % (resDapi.getFileSize(), resDapi.getDiskId())
    srvObj.getDb().query(sqlQuery, ignoreEmptyRes=0)


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
        
    Returns:        Void.
    """
    T = TRACE()

    # Check if the URI is correctly set.
    if (reqPropsObj.getFileUri() == ""):
        errMsg = genLog("NGAMS_ER_MISSING_URI")
        error(errMsg)
        raise Exception, errMsg

    # Is this NG/AMS permitted to handle Archive Requests?
    if (not srvObj.getCfg().getAllowArchiveReq()):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Archive"])
        raise Exception, errMsg
    srvObj.checkSetState("Archive Request", [NGAMS_ONLINE_STATE],
                         [NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE],
                         NGAMS_ONLINE_STATE, NGAMS_BUSY_SUBSTATE,
                         updateDb=False)

    # Get mime-type (try to guess if not provided as an HTTP parameter).
    if (reqPropsObj.getMimeType() == ""):
        mimeType = ngamsHighLevelLib.\
                   determineMimeType(srvObj.getCfg(), reqPropsObj.getFileUri())
        reqPropsObj.setMimeType(mimeType)
    else:
        mimeType = reqPropsObj.getMimeType()

    if reqPropsObj.getFileUri().startswith('http://'):
    ## Set reference in request handle object to the read socket.
        reqPropsObj.setReadFd(ngamsHighLevelLib.\
                      openCheckUri(reqPropsObj.getFileUri()))

    # Determine the target volume, ignoring the stream concept.
    targDiskInfo = getTargetVolume(srvObj)
    reqPropsObj.setTargDiskInfo(targDiskInfo)
        
    # Generate staging filename.
    baseName = os.path.basename(reqPropsObj.getFileUri())
    stgFilename = os.path.join("/", targDiskInfo.getMountPoint(), 
                               NGAMS_STAGING_DIR,
                               genUniqueId() + "___" + baseName)
    info(3, "Staging filename is: %s" % stgFilename)
    reqPropsObj.setStagingFilename(stgFilename)
                            
    # Retrieve file contents (from URL, archive pull, or by storing the body
    # of the HTTP request, archive push).
    ioTime = ngamsHighLevelLib.saveInStagingFile(srvObj.getCfg(), reqPropsObj,
                                                 stgFilename, targDiskInfo)
    reqPropsObj.incIoTime(ioTime)

    # Invoke DAPI.
    plugIn = srvObj.getMimeTypeDic()[mimeType]
    try:
        exec "import " + plugIn
    except Exception, e:
        errMsg = "Error loading DAPI: %s. Error: %s" % (plugIn, str(e))
        raise Exception, errMsg
    info(2, "Invoking DAPI: " + plugIn +\
         " to handle data for file with URI: " + baseName)
    timeBeforeDapi = time.time()
    resDapi = eval(plugIn + "." + plugIn + "(srvObj, reqPropsObj)")
    if (getVerboseLevel() > 4):
        info(4, "Invoked DAPI: %s. Time: %.3fs." %\
             (plugIn, (time.time() - timeBeforeDapi)))
        info(4, "Result DAPI: %s" % str(resDapi.toString()))

    # Move file to final destination.
    ioTime = mvFile(reqPropsObj.getStagingFilename(),
                    resDapi.getCompleteFilename())
    reqPropsObj.incIoTime(ioTime)
    
    # Generate check-sum (invoke checksum plug-in).
    checksumPlugIn = srvObj.getCfg().getChecksumPlugIn()
    if (checksumPlugIn != ""):
        info(4, "Invoking Checksum Plug-In: " + checksumPlugIn +\
             " to handle file: " + resDapi.getCompleteFilename())
        exec "import " + checksumPlugIn
        checksum = eval(checksumPlugIn + "." + checksumPlugIn +\
                        "(srvObj, resDapi.getCompleteFilename(), 0)")
        info(4, "Invoked Checksum Plug-In: " + checksumPlugIn +\
             " to handle file: " + resDapi.getCompleteFilename() +\
             ". Result: " + checksum)
    else:
        checksum = ""

    # Check/generate remaining file info + update in DB.
    ts = PccUtTime.TimeStamp().getTimeStamp()
    creDate = getFileCreationTime(resDapi.getCompleteFilename())
    fileInfo = ngamsFileInfo.ngamsFileInfo().\
               setDiskId(resDapi.getDiskId()).\
               setFilename(resDapi.getRelFilename()).\
               setFileId(resDapi.getFileId()).\
               setFileVersion(resDapi.getFileVersion()).\
               setFormat(resDapi.getFormat()).\
               setFileSize(resDapi.getFileSize()).\
               setUncompressedFileSize(resDapi.getUncomprSize()).\
               setCompression(resDapi.getCompression()).\
               setIngestionDate(ts).\
               setChecksum(checksum).setChecksumPlugIn(checksumPlugIn).\
               setFileStatus(NGAMS_FILE_STATUS_OK).\
               setCreationDate(creDate)
    fileInfo.write(srvObj.getDb())
   
    # Update disk info in NGAS Disks.
    updateDiskInfo(srvObj, resDapi)

    # Check if the disk is completed.
    # We use an approximate extimate for the remaning disk space to avoid
    # to read the DB.
    availSpace = getDiskSpaceAvail(targDiskInfo.getMountPoint(), smart=False)
    if (availSpace < srvObj.getCfg().getFreeSpaceDiskChangeMb()):
        complDate = PccUtTime.TimeStamp().getTimeStamp()
        targDiskInfo.setCompleted(1).setCompletionDate(complDate)
        targDiskInfo.write(srvObj.getDb())

    # Request after-math ...
    srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
    msg = "Successfully handled Archive Pull Request for data file " +\
          "with URI: " + reqPropsObj.getSafeFileUri()
    info(1, msg)
    srvObj.ingestReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                       NGAMS_SUCCESS, msg, targDiskInfo)

    # Trigger Subscription Thread.
    msg = "triggering SubscriptionThread"
    info(3, msg)
    srvObj.addSubscriptionInfo([(resDapi.getFileId(),
                                 resDapi.getFileVersion())], [])
    srvObj.triggerSubscriptionThread()

    return


# EOF
