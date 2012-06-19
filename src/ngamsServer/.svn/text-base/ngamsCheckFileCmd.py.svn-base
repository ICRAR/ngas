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
# "@(#) $Id: ngamsCheckFileCmd.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  24/04/03    Created
#

"""
Contains the functions to handle the CHECKFILE command.
"""

from ngams import *
import ngamsDbCore, ngamsDb, ngamsFileUtils


def handleCmdCheckFile(srvObj,
                       reqPropsObj,
                       httpRef):
    """
    Handle CHECKFILE command.
        
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler object
                    (ngamsHttpRequestHandler).
        
    Returns:        Void.
    """
    fileId      = ""
    fileVersion = 1
    diskId      = ""
    hostId      = ""
    if (reqPropsObj.hasHttpPar("file_id")):
        fileId = reqPropsObj.getHttpPar("file_id")
    if (reqPropsObj.hasHttpPar("file_version")):
        fileVersion = reqPropsObj.getHttpPar("file_version")
    if (reqPropsObj.hasHttpPar("disk_id")):
        diskId = reqPropsObj.getHttpPar("disk_id")
    if (reqPropsObj.hasHttpPar("host_id")):
        hostId = reqPropsObj.getHttpPar("host_id")

    # At least File ID must be specified (File Version is defaulted to 1).
    if (not fileId):
        errMsg = "Must specify a File ID for the CHECKFILE command."
        raise  Exception, errMsg

    # Get the info for the file matching the query.
    fileLocInfo = ngamsFileUtils.locateArchiveFile(srvObj, fileId, fileVersion,
                                                   diskId, hostId)
    
    # Now carry out the check.
    checkReport   = []
    fileLocation  = fileLocInfo[0]
    fileHostId    = fileLocInfo[1]
    fileIpAddress = fileLocInfo[2]
    filePortNo    = fileLocInfo[3]
    fileMtPt      = fileLocInfo[4] 
    if (fileLocation == NGAMS_HOST_LOCAL):
        info(3,"File is located on local host - carrying out the check on " +\
             "the file locally")
        # Get the Disk ID if not defined.
        if (not diskId):
            diskIdMtPts = srvObj.getDb().\
                          getDiskIdsMtPtsMountedDisks(fileHostId)
            for diskIdMtPt in diskIdMtPts:
                if (diskIdMtPt[1] == fileMtPt): diskId = diskIdMtPt[0]

        tmpFileRes = srvObj.getDb().getFileInfoFromFileIdHostId(fileHostId,
                                                                fileId,
                                                                fileVersion,
                                                                diskId)
        tmpDiskRes = srvObj.getDb().getDiskInfoFromDiskId(diskId)
        fileSlotId = tmpDiskRes[ngamsDbCore.NGAS_DISKS_SLOT_ID]
        sum1FileInfo = [fileSlotId,
                        tmpDiskRes[ngamsDbCore.NGAS_DISKS_MT_PT],
                        tmpFileRes[ngamsDbCore.NGAS_FILES_FILE_NAME],
                        tmpFileRes[ngamsDbCore.NGAS_FILES_CHECKSUM],
                        tmpFileRes[ngamsDbCore.NGAS_FILES_CHECKSUM_PI],
                        tmpFileRes[ngamsDbCore.NGAS_FILES_FILE_ID],
                        tmpFileRes[ngamsDbCore.NGAS_FILES_FILE_VER],
                        tmpFileRes[ngamsDbCore.NGAS_FILES_FILE_SIZE],
                        tmpFileRes[ngamsDbCore.NGAS_FILES_FILE_STATUS],
                        tmpFileRes[ngamsDbCore.NGAS_FILES_DISK_ID]]
        ngamsFileUtils.checkFile(srvObj, sum1FileInfo, checkReport)
        if (not checkReport):
            msg = genLog("NGAMS_INFO_FILE_OK",
                         [fileId, int(fileVersion), diskId, fileSlotId,
                          fileHostId])
        else:
            discrepancies = ""
            for discrepancy in checkReport:
                discrepancies += ". Descrepancy: " + discrepancy[0]
            msg = genLog("NGAMS_ER_FILE_NOK", [fileId, int(fileVersion),
                                               diskId, fileSlotId, fileHostId,
                                               discrepancies])
        info(3,msg)
        reqPropsObj.setCompletionTime(1)
        srvObj.updateRequestDb(reqPropsObj)
        srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                     NGAMS_SUCCESS, msg)
        return
    elif (srvObj.getCfg().getProxyMode() or
          (fileLocInfo[0] == NGAMS_HOST_CLUSTER)):
        info(3,"File is remote or located within the private network of " +\
             "the contacted NGAS system -- this server acting as proxy " +\
             "and forwarding request to remote NGAS system: %s/%d" %\
             (fileHostId, filePortNo))
        httpStatCode, httpStatMsg, httpHdrs, data =\
                      srvObj.forwardRequest(reqPropsObj, httpRef,
                                            fileHostId, filePortNo)
        return
    else:
        # Send back an HTTP re-direction response to the requestor.
        info(3,"File to be checked is stored on a remote host not within " +\
             "private network, Proxy Mode is off - sending back HTTP " +\
             "re-direction response")
        reqPropsObj.setCompletionTime(1)
        srvObj.updateRequestDb(reqPropsObj)
        srvObj.httpRedirReply(reqPropsObj, httpRef, fileHostId, filePortNo)
        return
    

# EOF
