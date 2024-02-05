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
# "@(#) $Id: ngamsRemFileCmd.py,v 1.6 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  15/04/2002  Created
#
"""
Functions to handle the REMFILE Command.
"""

import logging
import os

import six
from six.moves import filter

from ngamsLib import ngamsDbm, ngamsDbCore, ngamsHighLevelLib
from ngamsLib.ngamsCore import genLog, NGAMS_REMFILE_CMD, \
    rmFile, NGAMS_SUCCESS, NGAMS_XML_MT
from .. import ngamsRemUtils


logger = logging.getLogger(__name__)

def _remFile(srvObj,
             reqPropsObj,
             diskId,
             fileId,
             fileVersion,
             execute,
             tmpFilePat):
    """
    See documentation for the ngamsRemFileCmd.remFile() function.
    """
    # Check for illegal parameter combinations.
    if not diskId or not fileId:
        errMsg = "Disk ID: %s, File ID: %s, File Version: %d" %\
                 (diskId, fileId, fileVersion)
        errMsg = genLog("NGAMS_ER_CMD_SYNTAX", [NGAMS_REMFILE_CMD, errMsg])
        raise Exception(errMsg)

    # Get the information from the DB about the files in question.
    hostId = None
    diskIds = []
    fileIds = []
    if diskId:
        diskIds = [diskId]
    elif fileId:
        hostId = srvObj.getHostId()
    if fileId:
        fileIds = [fileId]
    if fileVersion == -1:
        fileVersion = None

    files = srvObj.db.getFileSummary1(hostId, diskIds, fileIds, ignore=None)
    if fileVersion:
        files = filter(lambda f: fileVersion == f[ngamsDbCore.SUM1_VERSION], files)
    fileListDbmName   = os.path.normpath(tmpFilePat + "_FILE_LIST")
    fileListDbm = ngamsDbm.enumerate_to_dbm(fileListDbmName, files)

    # Check if the files selected for deletion are available within the NGAS
    # system, in at least 3 copies.
    filesMisCopyDbmName, filesNotRegDbmName, complFileListDbmName =\
                         ngamsRemUtils.checkFileCopiesAndReg(srvObj,
                                                             3, tmpFilePat,
                                                             fileListDbmName)
    status = ngamsRemUtils._remStatErrReport(srvObj, reqPropsObj, tmpFilePat,
                                             filesMisCopyDbmName,
                                             filesNotRegDbmName,
                                             fileListDbmName, diskId,
                                             fileId, fileVersion)
    if (status): return status

    # Check that none of the matching files are stored on other NGAS Nodes.
    # If such are found, do not consider these.
    fileListDbm.initKeyPtr()
    remKeyList = []
    while (1):
        key, fileInfo = fileListDbm.getNext()
        if (not key): break
        if (fileInfo[ngamsDbCore.SUM1_HOST_ID] != srvObj.getHostId()):
            remKeyList.append(key)
    for remKey in remKeyList: fileListDbm.rem(remKey)

    #########################################################################
    # Execute the deletion if execute = 1 and files were found to be deleted.
    #########################################################################
    successDelCount = 0
    failedDelCount = 0
    if (execute):
        fileListDbm.initKeyPtr()
        run = 1
        # TODO: This should be changed to a single or a few DB transactions for all files
        # and a bulk rm for the same number of files.
        while (run):
            key, fileInfo = fileListDbm.getNext()
            if (not key):
                run = 0
                continue
            try:
                diskId   = fileInfo[ngamsDbCore.SUM1_DISK_ID]
                fileId   = fileInfo[ngamsDbCore.SUM1_FILE_ID]
                fileVer  = fileInfo[ngamsDbCore.SUM1_VERSION]
                mtPt     = fileInfo[ngamsDbCore.SUM1_MT_PT]
                filename = fileInfo[ngamsDbCore.SUM1_FILENAME]
                complFilename = os.path.normpath(mtPt + "/" + filename)
                msg = "Deleting DB info for file: %s/%s/%d"
                logger.debug(msg, diskId, fileId, fileVer)
                # We remove first the DB info and afterwards the file on the
                # disk. The reason for this is that it is considered worse
                # to have an entry for a file in the DB, which is not on disk
                # than vice versa, since NGAS uses the info in the DB to check
                # for the number of available copies.
                try:
                    srvObj.getDb().deleteFileInfo(srvObj.getHostId(), diskId, fileId, fileVer)
                    infoMsg = genLog("NGAMS_INFO_DEL_FILE",
                                     [diskId, fileId, fileVer])
                    logger.debug(infoMsg)
                    successDelCount += 1
                except Exception as e:
                    failedDelCount += 1
                    errMsg = genLog("NGAMS_ER_DEL_FILE_DB",
                                    [diskId, fileId, fileVer, str(e)])
                    logger.warning(errMsg)
                # Removing the DB info was successful, remove the copy on disk.
                msg = "Deleting copy of file: %s/%s/%d: %s"
                logger.debug(msg, diskId, fileId, fileVer, complFilename)
                rmFile(complFilename)
            except Exception as e:
                failedDelCount += 1
                errMsg = genLog("NGAMS_ER_DEL_FILE_DISK",
                                [diskId, fileId, fileVer, str(e)])
                logger.warning(errMsg)

    # Generate status.
    filesSelected = fileListDbm.getCount()
    infoMsg = genLog("NGAMS_INFO_FILE_DEL_STAT",
                     [filesSelected, successDelCount, failedDelCount])
    status = srvObj.genStatus(NGAMS_SUCCESS, infoMsg)

    return status


def remFile(srvObj,
            reqPropsObj,
            diskId,
            fileId,
            fileVersion,
            execute):
    """
    Function used to delete information about files in the NGAS DB and to
    delete files on the disks. Only local files are considered.

    The information selected for deletion is selected applying the
    following criterias:

      o diskId!="", fileId=="", fileVersion==-1:
        All files on the given disk are taken.

      o diskId!="", fileId!="", fileVersion==-1:
        All files with the given File ID on the disk with the given ID
        will be selected. No specific file version will be taken into account.

      o diskId!="", fileId!="", fileVersion!=-1:
        The referenced file with the given File ID and File Version on the
        given ID is selected (if this exists).

      o diskId=="", fileId!="", fileVersion==-1:
        Not allowed.

      o diskId=="", fileId!="", fileVersion!=-1:
        Not allowed.

      o diskId=="", fileId=="", fileVersion!=-1:
        Not allowed.

      o diskId!="", fileId=="", fileVersion!=-1:
        Not allowed


    The function will not carry out the actual deletion of the selected
    files if the 'execute' parameter is set to 0. In this case a report
    with the items selected for deletion will be generated.


    NOTE: No files are selected for deletion, unless there are at least three
          copies of the file (File ID + File Version), which apparently are
          accessible. In addition, these three copies have to be distributed
          on three different disks (storage media).

    srvObj:          Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:     Request Property object to keep track of actions done
                     during the request handling (ngamsReqProps).

    diskId:          ID of disk. It is not possible to specify a pattern,
                     must be exact ID (string).

    fileId:          ID of file. It is possible to specify a file pattern
                     using wild cards (string).

    fileVersion:     Version of file to delete. If not specified, all
                     files matching the Disk ID + File ID will be selected.
                     Otherwise it is possible to specify a single file for
                     deletion (integer).

    execute:         If set to 0, only a report of the information that has
                     been selected for deletion is generated (integer/0|1).

    Returns:         Status object with a list of disks and corresponding
                     files deleted (ngamsStatus).
    """
    tmpFilePat = ngamsHighLevelLib.genTmpFilename(srvObj.getCfg(),
                                                  "REMFILE_CMD")
    try:
        status = _remFile(srvObj, reqPropsObj, diskId, fileId, fileVersion,
                          execute, tmpFilePat)
        return status
    finally:
        rmFile(tmpFilePat + "*")


def handleCmd(srvObj,
                     reqPropsObj,
                     httpRef):
    """
    Handle REMFILE command. See also 'handleRemFile()'.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    if (not srvObj.getCfg().getAllowRemoveReq()):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Remove"])
        raise Exception(errMsg)

    diskId      = ""
    fileId      = ""
    fileVersion = -1
    execute     = 0
    if (reqPropsObj.hasHttpPar("disk_id")):
        diskId = reqPropsObj.getHttpPar("disk_id")
    if (reqPropsObj.hasHttpPar("file_id")):
        fileId = reqPropsObj.getHttpPar("file_id")
    if (reqPropsObj.hasHttpPar("file_version")):
        fileVersion = int(reqPropsObj.getHttpPar("file_version"))
    if (reqPropsObj.hasHttpPar("execute")):
        try:
            execute = int(reqPropsObj.getHttpPar("execute"))
        except:
            errMsg = genLog("NGAMS_ER_REQ_HANDLING", ["Must provide proper " +\
                            "value for parameter: execute (0|1)"])
            raise Exception(errMsg)

    # Carry out the command.
    status = remFile(srvObj, reqPropsObj, diskId, fileId, fileVersion, execute)

    # Send reply back to requestor.
    xmlStat = status.genXmlDoc(0, 1, 1, 1, 0)
    xmlStat = ngamsHighLevelLib.addStatusDocTypeXmlDoc(xmlStat, httpRef.host)
    httpRef.send_data(six.b(xmlStat), NGAMS_XML_MT)


# EOF
