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
# "@(#) $Id: ngamsRemDiskCmd.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  15/04/2002  Created
#

"""
Functions to handle the REMDISK command.
"""

import os, glob

from ngams import *
import ngamsDbm, ngamsDb, ngamsLib
import ngamsFileInfo, ngamsDiskInfo, ngamsStatus, ngamsDiskInfo
import ngamsFileList, ngamsHighLevelLib, ngamsDiskUtils
import ngamsRemUtils, ngamsNotification


def _remDisk(srvObj,
             reqPropsObj,
             diskId,
             execute,
             tmpFilePat):
    """
    See documentation for the ngamsRemDiskCmd.remDisk() function.
    """
    # Get disk info -- if any available.
    sqlDiskInfo = srvObj.getDb().getDiskInfoFromDiskId(diskId)
    diskInfo = ngamsDiskInfo.ngamsDiskInfo()
    if (sqlDiskInfo != []):
        if (diskInfo.unpackSqlResult(sqlDiskInfo).getHostId() != getHostId()):
            sqlDiskInfo = None

    # Check that the disk is mounted in this unit (no proxy for the REMDISK
    # Command ...).
    if (diskInfo.getHostId() != getHostName()):
        errMsg = "Disk referred to by Disk ID: %s seems not to be mounted " +\
                 "in this unit: %s -- rejecting REMDISK Command"
        errMsg = errMsg % (diskId, getHostName())
        error(errMsg)
        raise Exception, errMsg

    # Check that execution of the request can be granted.
    filesMisCopyDbmName, filesNotRegDbmName, fileListDbmName =\
                         ngamsRemUtils.checkFileCopiesAndReg(srvObj, 3,
                                                             tmpFilePat, None,
                                                             diskId)
    status = ngamsRemUtils._remStatErrReport(srvObj, reqPropsObj, tmpFilePat,
                                             filesMisCopyDbmName,
                                             filesNotRegDbmName,
                                             fileListDbmName, diskId)
    misCopiesDbm = ngamsDbm.ngamsDbm(filesMisCopyDbmName)
    if (misCopiesDbm.getCount() == 0):
        info(1,"Disk with ID: %s approved for deletion" % diskId)
    else:
        info(1,"Disk with ID: %s rejected for deletion" % diskId)
    if (status): return status

    # Check if there is enough space on the disk to store the file info
    # cache files.
    kbPerFile = 0.256
    tmpDbm = ngamsDbm.ngamsDbm(fileListDbmName, writePerm = 0)
    noOfFiles = tmpDbm.getCount()
    del tmpDbm
    kbsAvail = getDiskSpaceAvail(diskInfo.getMountPoint(), format="KB",float=1)
    kbsReq = (kbPerFile * noOfFiles)
    msg = "Space required for REMDISK Command: %.1f KB, " +\
          "space available: %.1f KB"
    info(3,msg % (kbsReq, kbsAvail))
    if (kbsReq > kbsAvail):
        errMsg = "Not enough space on disk to carry out REMDISK Command. " +\
                 "Host: %s, Disk ID: %s, Mount Point: %s. " +\
                 "Required disk space: %.3f MB"
        errMsg = errMsg % (getHostName(), diskInfo.getDiskId(),
                           diskInfo.getMountPoint(), (kbsReq / 1024.))
        error(errMsg)
        raise Exception, errMsg

    #########################################################################
    # Execute the deletion if execute = 1 and a matching disk was found.
    # Otherwise just return the ngamsStatus object as confirmation.
    #########################################################################
    if (execute and (sqlDiskInfo != [])):
        # We delete first the info in the DB in connection with the disk and
        # afterwards the contents on the disk. This is done like this, since
        # it is considered worse having entries for files in the DB which are
        # not available on the disk since NGAS uses the info in the DB to check
        # for the number of available copies.
        info(1,"Removing DB record for disk with ID: " + diskId + " ...")
        try:
            tmpDir = os.path.dirname(tmpFilePat)
            srvObj.getDb().deleteDiskInfo(diskId, 1)
        except Exception, e:
            errMsg = genLog("NGAMS_ER_DEL_DISK_DB", [diskId, str(e)])
            error(errMsg)
            raise Exception, errMsg
        info(1,"Deleting contents on disk with ID: " + diskId + " ...")
        try:
            ngamsLib.rmFile(os.path.normpath(diskInfo.getMountPoint() + "/*"))
        except Exception, e:
            errMsg = genLog("NGAMS_ER_DEL_DISK", [diskId, str(e)])
            error(errMsg)
            raise Exception, errMsg
        try:
            # Remember to remove entry for disk from the Physical Disk Dic.
            del srvObj.getDiskDic()[diskInfo.getSlotId()]
        except:
            pass
        
        infoMsg = genLog("NGAMS_INFO_DEL_DISK", [diskId])
        info(1,infoMsg)

        # Add entry in the NGAS Disks History Table.
        ngasDiskInfo = ngamsDiskUtils.prepNgasDiskInfoFile(diskInfo, 1, 1)
        srvObj.getDb().addDiskHistEntry(diskId, "Disk Removed",
                                        NGAMS_XML_MT, ngasDiskInfo)
    elif (sqlDiskInfo != []):
        infoMsg = genLog("NGAMS_INFO_DEL_DISK_SEL", [diskId]) 
    else:
        infoMsg = genLog("NGAMS_WA_DEL_DISK2", [getHostId(), diskId])
 
    # Generate status.
    status = srvObj.genStatus(NGAMS_SUCCESS, infoMsg)
    if (sqlDiskInfo): status.addDiskStatus(diskInfo)
    #########################################################################
    
    return status


def remDisk(srvObj,
            reqPropsObj,
            diskId,
            execute):
    """
    Select a disk for removal and remove the information if so specified
    from the NGAS DB and delete all information on the disk.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    diskId:         ID of disk. Complete ID must be specified. I.e., no
                    wildcards are handled (string).
    
    execute:        If set to 1 the information about the disk will be deleted.
                    Otherwise only the information about the disk selected for
                    deletion will be queried (integer/0|1).

    Returns:        Status object contained information about disk
                    selected for deletion/deleted (ngamsStatus).
    """
    T = TRACE()
    
    tmpFilePat = ngamsHighLevelLib.genTmpFilename(srvObj.getCfg(),
                                                  "REMDISK_CMD")
    try:
        status = _remDisk(srvObj, reqPropsObj, diskId, execute, tmpFilePat)
        rmFile(tmpFilePat + "*")
        return status
    except Exception, e:
        rmFile(tmpFilePat + "*")
        raise e


def handleCmdRemDisk(srvObj,
                     reqPropsObj,
                     httpRef):
    """
    Handle REMDISK command. See also 'handleRemDisk()'.
        
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).
        
    Returns:        Void.
    """
    if (not srvObj.getCfg().getAllowRemoveReq()):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Remove"])
        error(errMsg)
        raise Exception, errMsg

    diskId      = ""
    execute     = 0
    if (reqPropsObj.hasHttpPar("disk_id")):
        diskId = reqPropsObj.getHttpPar("disk_id")
    if (diskId == ""):
        errMsg = genLog("NGAMS_ER_CMD_SYNTAX",
                        [NGAMS_REMDISK_CMD, "Missing parameter: disk_id"])
        raise Exception, errMsg
    if (not srvObj.getDb().diskInDb(diskId)):
        errMsg = genLog("NGAMS_ER_UNKNOWN_DISK", [diskId])
        raise Exception, errMsg
    if (reqPropsObj.hasHttpPar("execute")):
        try:
            execute = int(reqPropsObj.getHttpPar("execute"))
        except:
            errMsg = genLog("NGAMS_ER_REQ_HANDLING", ["Must provide proper " +\
                            "value for parameter: execute (0|1)"])
            raise Exception, errMsg
        
    # Carry out the command.
    status = remDisk(srvObj, reqPropsObj, diskId, execute)

    # Send reply back to requestor (if not already done).
    if (not status): return
    xmlStat = status.genXmlDoc(0, 1, 1, 1, 0)
    xmlStat = ngamsHighLevelLib.addDocTypeXmlDoc(srvObj, xmlStat,
                                                 NGAMS_XML_STATUS_ROOT_EL,
                                                 NGAMS_XML_STATUS_DTD)
    if (status.getStatus() == NGAMS_SUCCESS):
        httpStat = NGAMS_HTTP_SUCCESS
    else:
        httpStat = NGAMS_HTTP_BAD_REQ
    srvObj.httpReplyGen(reqPropsObj, httpRef, httpStat, xmlStat,
                        dataInFile=0, contentType = NGAMS_XML_MT)


# EOF
