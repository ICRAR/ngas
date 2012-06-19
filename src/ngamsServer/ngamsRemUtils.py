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
# "@(#) $Id: ngamsRemUtils.py,v 1.6 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  11/06/2003  Created
#

"""
Contains common functions used for the REMFILE and REMDISK commands.
"""

import os, time, glob

from ngams import *
import ngamsLib, ngamsDbm, ngamsDbCore, ngamsDb
import ngamsDiskInfo, ngamsFileInfo, ngamsFileList
import ngamsHighLevelLib, ngamsNotification
import ngamsFileUtils

   
def checkSpuriousFiles(srvObj,
                       tmpFilePat = None,
                       hostId = None,
                       diskId = None,
                       fileId = None,
                       fileVersion = None):
    """
    Check if there are any spurious files in the DB either marked as to be
    ignored or having a status indicating a problem. If such are found
    according to the criterias defined, these are added in a DBM DB, which
    name is returned.

    srvObj:          Reference to NG/AMS server class object (ngamsServer).

    tmpFilePat:      Pattern to apply for temporary files (string).
    
    hostId:          Name of NGAS host on which the files reside (string).

    diskId:          Disk ID of disk to take into account (string|None).

    fileId:          File ID of file(s) to take into account (string|None).

    fileVersion:     Version of file(s) to take into account (integer|None).

    Returns:         Returns name of DBM DB with references to spurious
                     files found (string).
    """
    T = TRACE()
    
    if (hostId == ""): hostId = None
    if (diskId == ""): diskId = None
    if (fileId == ""): fileId = None
    if (fileVersion == -1): fileVersion = None

    # DBM DB containing information about spurious files.
    filename = "_SPURIOUS_FILES"
    if (tmpFilePat):
        spuriousFilesDbmName = os.path.normpath(tmpFilePat + filename)
    else:
        spuriousFilesDbmName = ngamsHighLevelLib.\
                               genTmpFilename(srvObj.getCfg(), filename)
    spuriousFilesDbm = ngamsDbm.ngamsDbm(spuriousFilesDbmName, writePerm=1)

    # Check that there are no spurious files in connection with this disk in
    # the DB (where ngas_files.ignore != 0 or ngas_files.status != "1*******"
    cursorObj = srvObj.getDb().\
                getFileSummarySpuriousFiles1(hostId, diskId, fileId,
                                             fileVersion)
    while (1):
        fileList = cursorObj.fetch(200)
        if (not fileList): break

        # Loop over the files.
        for fileInfo in fileList:
            spuriousFilesDbm.addIncKey(fileInfo)
        spuriousFilesDbm.sync()
        time.sleep(0.2)
    del cursorObj
    del spuriousFilesDbm

    return spuriousFilesDbmName


def checkFilesRemovable(srvObj,
                        fileListDbmName,
                        tmpFilePat):
    """
    Check for each file represented by a File Info Object if it can be
    deleted/removed from the disk.

    srvObj:          Reference to NG/AMS server class object (ngamsServer).
  
    fileListDbmName: Name of DBM DB containing the information about the
                     files in question (string

    tmpFilePat:      Pattern used to generate temporary filenames (string).

    Returns:         Name of DBM DB with information about files that
                     cannot be removed (string).
    """
    T = TRACE()

    # DBM DB containing information about non-removable files.
    filename = "_NON_REMOVABLE_FILES"
    nonRemFilesDbmName = os.path.normpath(tmpFilePat + "_NON_REMOVABLE_FILES")
    nonRemFilesDbm     = ngamsDbm.ngamsDbm(nonRemFilesDbmName, writePerm = 1)

    fileListDbm = ngamsDbm.ngamsDbm(fileListDbmName)
    fileListDbm.initKeyPtr()
    while (1):
        key, fileInfo = fileListDbm.getNext()
        if (not key): break
        mtPt = fileInfo[ngamsDbCore.SUM1_MT_PT]
        fileName = fileInfo[ngamsDbCore.SUM1_FILENAME]
        complFilename = os.path.normpath(mtPt + "/" + fileName)
        if (not ngamsLib.fileRemovable(complFilename)):
            nonRemFilesDbm.addIncKey(fileInfo)
    del nonRemFilesDbm
    del fileListDbm

    return nonRemFilesDbmName


def _notify(srvObj,
            reqPropsObj,
            statRep,
            mimeType,
            cmd):
    """
    Prepare and send an Email Notification Message.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
    
    statRep:        Status report filename (string).
    
    mimeType:       Mime-type of data to send (string/mime-type).

    Returns:        Void.
    """
    emailAdrList = reqPropsObj.getHttpPar("notif_email").split(",")
    attachmentName = cmd + "-StatusReport"
    attachmentName += "-" + reqPropsObj.getHttpPar("disk_id")
    ngamsNotification.notify(srvObj.getCfg(), NGAMS_NOTIF_INFO,
                             cmd + " STATUS REPORT", statRep,
                             emailAdrList, 1, mimeType, attachmentName, 1)

 
def _remStatErrReport(srvObj,
                      reqPropsObj,
                      tmpFilePat, 
                      filesMisCopiesDbmName,
                      filesNotRegDbmName,
                      fileListDbmName,
                      diskId = None,
                      fileId = None,
                      fileVersion = None):
    """
    Function to generate an error report in case of the REMDISK and REMFILE
    commands.

    srvObj:                Reference to NG/AMS server class object
                           (ngamsServer).
  
    reqPropsObj:           Request Property object to keep track of actions
                           done during the request handling (ngamsReqProps).

    tmpFilePat:            File pattern to be used when generating temporary
                           files (string).
    
    filesMisCopiesDbmName: Name of DBM containing references to files with
                           less than the required number of copies (string).
    
    filesNotRegDbmName:    Name of DBM containing complete filenames as keys
                           referring to files, which are not registered in
                           the DB (string).
    
    fileListDbmName:       Name of DBM containing all information about the
                           files concerned with the query.
    
    diskId:                Disk ID if relevant (string).
    
    fileId:                File ID if relevant (string).
    
    fileVersion:           File Version if relevant (integer).

    Returns:               In case inconsistencies were found, an ngamsStatus
                           object with more information is returned. Otherwise,
                           if everything was found to be OK, None is returned
                           (ngamsStatus|None).
    """
    cmd = reqPropsObj.getCmd()

    #########################################################################
    # At least three copies of each file?
    #########################################################################
    filesMisCopyDbm = ngamsDbm.ngamsDbm(filesMisCopiesDbmName, writePerm = 0)
    misFileCopies = filesMisCopyDbm.getCount()
    if (misFileCopies and reqPropsObj.hasHttpPar("notif_email")):
        xmlReport = 0
        if (xmlReport):
            # TODO:
            pass
        else:
            statRep = tmpFilePat + "_NOTIF_EMAIL.txt"
            fo = open(statRep, "w")
            timeStamp = PccUtTime.TimeStamp().getTimeStamp()
            tmpFormat = cmd + " STATUS REPORT - " +\
                        "MISSING FILE COPIES:\n\n" +\
                        "==Summary:\n\n" +\
                        "Date:                       %s\n" +\
                        "NGAS Host ID:               %s\n" +\
                        "Disk ID:                    %s\n" +\
                        "Files Detected:             %d\n\n" +\
                        "==File List:\n\n"
            fo.write(tmpFormat % (timeStamp, getHostId(),diskId,misFileCopies))
            tmpFormat = "%-32s %-12s %-6s\n"
            fo.write(tmpFormat % ("File ID", "Version", "Copies"))
            fo.write(tmpFormat % (32 * "-", 7 * "-", 6 * "-"))

            # Loop over the files an generate the report.
            filesMisCopyDbm.initKeyPtr()
            while (1):
                fileKey, fileInfo = filesMisCopyDbm.getNext()
                if (not fileKey): break
                noOfCopies = fileInfo.getTag().split(": ")[-1]
                fo.write(tmpFormat % (fileInfo.getFileId(),
                                      str(fileInfo.getFileVersion()),
                                          noOfCopies))
            fo.write(52 * "-")
            fo.write("\n\n==END\n")
            fo.close()
            mimeType = NGAMS_TEXT_MT

        # Send out Notification Email.
        _notify(srvObj, reqPropsObj, statRep, mimeType, cmd)
    del filesMisCopyDbm
 
    # Generate error message if files with less than the required number
    # of copies are concerned by the query.
    if (misFileCopies):
        errMsg = genLog("NGAMS_WA_FILE_COPIES")
        status = srvObj.genStatus(NGAMS_FAILURE, errMsg)
        status.setMessage(status.getMessage() +\
                          " Cannot remove item: %s/%s/%s" %
                          (str(diskId), str(fileId), str(fileVersion)))
        warning(errMsg) 
        return status
    #########################################################################
     
    #########################################################################
    # Check for spurious files.
    #########################################################################
    spuriousFilesDbmName = checkSpuriousFiles(srvObj, tmpFilePat, getHostId(),
                                              diskId, fileId, fileVersion)
    spuriousFilesDbm = ngamsDbm.ngamsDbm(spuriousFilesDbmName, writePerm = 0)
    spuriousFiles = spuriousFilesDbm.getCount()
    srvDataChecking = srvObj.getDb().getSrvDataChecking(getHostId())
    if (spuriousFiles and reqPropsObj.hasHttpPar("notif_email")):
        xmlReport = 0
        if (xmlReport):
            # TODO:
            pass
        else:
            statRep = tmpFilePat + "_NOTIF_EMAIL.txt"
            fo = open(statRep, "w")
            timeStamp = PccUtTime.TimeStamp().getTimeStamp()
            tmpFormat = cmd + " STATUS REPORT - SPURIOUS FILES:\n\n" +\
                        "==Summary:\n\n" +\
                        "Date:                       %s\n" +\
                        "NGAS Host ID:               %s\n" +\
                        "Disk ID:                    %s\n" +\
                        "Spurious Files:             %d\n"
            fo.write(tmpFormat % (timeStamp,getHostId(),diskId,spuriousFiles))
            if (srvDataChecking):
                fo.write("Note: NGAS Host is performing Data Consistency " +\
                         "Checking - consider to switch off!\n")
            fo.write("\n==File List:\n\n")
            tmpFormat = "%-32s %-12s %-11s %-11s\n"
            fo.write(tmpFormat % ("File ID", "File Version", "File Status",
                                  "Ignore Flag"))
            fo.write(tmpFormat % (32 * "-", 12 * "-", 11 * "-", 11 * "-"))

            # Loop over the files an generate the report.
            spuriousFilesDbm.initKeyPtr()
            while (1):
                key, fileInfo = spuriousFilesDbm.getNext()
                if (not key): break
                fileId      = fileInfo[ngamsDbCore.SUM1_FILE_ID]
                fileVersion = str(fileInfo[ngamsDbCore.SUM1_VERSION])
                fileStatus  = fileInfo[ngamsDbCore.SUM1_FILE_STATUS]
                ignoreFlag  = str(fileInfo[ngamsDbCore.SUM1_FILE_IGNORE])
                fo.write(tmpFormat % (fileId, fileVersion, fileStatus,
                                      ignoreFlag))
            fo.write("\n\n==END\n")
            fo.close()
            mimeType = NGAMS_TEXT_MT

        # Send out Notification Email.
        _notify(srvObj, reqPropsObj, statRep, mimeType, cmd)
    del spuriousFilesDbm

    # Generate error message if spurious files were found.
    if (spuriousFiles):
        errMsg = genLog("NGAMS_WA_FILE_NON_REM")
        warning(errMsg)
        status = srvObj.genStatus(NGAMS_FAILURE, errMsg)
        msg = status.getMessage() + " Cannot delete specified files"
        if (srvDataChecking):
            msg += ". Note: NGAS Host is performing Data Consistency " +\
                   "Checking - consider to switch off!"
        status.setMessage(msg)
        return status
    #########################################################################
   
    #########################################################################
    # Unregistered files found?
    #########################################################################
    filesNotRegDbm = ngamsDbm.ngamsDbm(filesNotRegDbmName, writePerm = 0)
    unRegFilesFound = filesNotRegDbm.getCount()
    if (unRegFilesFound and reqPropsObj.hasHttpPar("notif_email")):
        xmlReport = 0
        if (xmlReport):
            # TODO:
            pass
        else:
            statRep = tmpFilePat + "_NOTIF_EMAIL.txt"
            fo = open(statRep, "w")
            timeStamp = PccUtTime.TimeStamp().getTimeStamp()
            tmpFormat = cmd + " STATUS REPORT - " +\
                        "NON-REGISTERED FILES:\n\n" +\
                        "==Summary:\n\n" +\
                        "Date:                       %s\n" +\
                        "NGAS Host ID:               %s\n" +\
                        "Disk ID:                    %s\n" +\
                        "Non-Registered Files:       %d\n\n" +\
                        "==File List:\n\n"
            fo.write(tmpFormat % (timeStamp, getHostId(), diskId,
                                  unRegFilesFound))

            # Loop over the files an generate the report.
            filesNotRegDbm.initKeyPtr()
            while (1):
                fileName, dummy = filesNotRegDbm.getNext()
                if (not fileName): break
                fo.write(fileName + "\n")
            fo.write("\n\n==END\n")
            fo.close()
            mimeType = NGAMS_TEXT_MT

        # Send out Notification Email.
        _notify(srvObj, reqPropsObj, statRep, mimeType, cmd)
    del filesNotRegDbm

    # Generate error message if non-registered files found.
    if (unRegFilesFound):
        errMsg = genLog("NGAMS_WA_FILES_NOT_REG")
        warning(errMsg)
        status = srvObj.genStatus(NGAMS_FAILURE, errMsg)
        status.setMessage(status.getMessage() +\
                          " Cannot remove volume: %s/%s/%s" %
                          (str(diskId), str(fileId), str(fileVersion)))
        return status
    #########################################################################

    #########################################################################
    # Check if the files to be removed actually can be removed.
    #########################################################################
    nonRemFilesDbmName = checkFilesRemovable(srvObj,fileListDbmName,tmpFilePat)
    nonRemFilesDbm = ngamsDbm.ngamsDbm(nonRemFilesDbmName, writePerm = 0)
    nonRemFiles = nonRemFilesDbm.getCount()
    if (nonRemFiles and reqPropsObj.hasHttpPar("notif_email")):
        xmlReport = 0
        if (xmlReport):
            # TODO:
            pass
        else:
            statRep = tmpFilePat + "_NOTIF_EMAIL.txt"
            fo = open(statRep, "w")
            timeStamp = PccUtTime.TimeStamp().getTimeStamp()
            tmpFormat = cmd + " STATUS REPORT - " +\
                        "NON-REMOVABLE FILES:\n\n" +\
                        "==Summary:\n\n" +\
                        "Date:                       %s\n" +\
                        "NGAS Host ID:               %s\n" +\
                        "Disk ID:                    %s\n" +\
                        "Non-Removable Files:        %d\n\n" +\
                        "==File List:\n\n"
            fo.write(tmpFormat % (timeStamp, getHostId(), diskId, nonRemFiles))

            tmpFormat = "%-32s %-12s %-32s\n"
            fo.write(tmpFormat % ("File ID", "File Version", "Filename"))
            fo.write(tmpFormat % (32 * "-", 12 * "-", 32 * "-"))

            # Loop over the files an generate the report.
            nonRemFilesDbm.initKeyPtr()
            while (1):
                key, fileInfo = nonRemFilesDbm.getNext()
                if (not key): break
                fo.write(tmpFormat % (fileInfo[ngamsDbCore.SUM1_FILE_ID],
                                      fileInfo[ngamsDbCore.SUM1_VERSION],
                                      fileInfo[ngamsDbCore.SUM1_FILENAME]))
            fo.write("\n\n==END\n")
            fo.close()
            mimeType = NGAMS_TEXT_MT

        # Send out Notification Email.
        _notify(srvObj, reqPropsObj, statRep, mimeType, cmd)
    del nonRemFilesDbm

    # Generate error message if files concerned by the query could not be
    # deleted.
    if (nonRemFiles):
        errMsg = genLog("NGAMS_WA_FILE_NON_REM")
        warning(errMsg)
        status = srvObj.genStatus(NGAMS_FAILURE, errMsg)
        status.setMessage(status.getMessage() +\
                          " Cannot delete specified files")
        return status
    #########################################################################

    # If we got to this point, no problems where found.
    return None


def checkFileCopiesAndReg(srvObj,
                          minReqCopies,
                          dbFilePat,
                          fileListDbmName = None,
                          diskId = None,
                          ignoreMounted = 0):
    """
    The function checks for each file referenced if there are at least
    'minReqCopies' copies available somewhere in this NGAS cluster. For the
    files where this is not the case, an entry is added in a ngasDiskInfo
    object indicating that this file

    If an entire disk is analysed (about to be deleted), it is also checked if
    each file stored on the disk is registered in the DB. Otherwise an NG/AMS
    File List is returned, containing references to the files not registered.

    srvObj:          Instance of the NG/AMS Server Class (ngamsServer).
    
    minReqCopies:    Minimum number of copies required (integer).
    
    dbFilePat:       Filename pattern used to build the DBM containing
                     information about the files (string).

    fileListDbmName: Name of DBM DB containing explicit references to files
                     to be checked if they can be deleted. The information
                     in this table is pickled lists with the lay-out defined
                     by ngamsDb._ngasFilesCols (string).

    diskId:          Used to refer to all files stored on a disk (string|None).

    ignoreMounted:   Carry out the check also if the disk is not mounted
                     (integer/0|1). 

    Returns:         Tuple contaning the filenames of three DBM DBs with the
                     following information:

                       o Files not having the specified number of copies.
                         The contents of this DB are keys (Disk ID + File ID +
                         File Version), pointing to pickled ngamsFileInfo
                         objects.

                       o Files found on the disk but not registered.
                         This contains the complete filenames of files found
                         on the disk, which are not registered. These filenames
                         are the keys of this DBM DB.

                       o Complete list of files referenced in connection
                         with the query. The contents of this DB are keys,
                         which are a simple counter pointing to pickled
                         list containing the information as returned by
                         ngamsDb.getFileSummary1().
                    
                     (tuple/string).
    """
    T = TRACE()

    if ((not fileListDbmName) and (not diskId)):
        errMsg = "ngamsSrvUtils.checkFileCopiesAndReg(): Must specify " +\
                 "either a DBM with files to be checked or a Disk ID"
        warning(errMsg)
        raise Exception, errMsg

    # Create DBMs:

    # - DB containing information about files having less then the
    # specified number of copies.
    fileMisCopyDbmName    = os.path.normpath(dbFilePat+"_MISSING_COPIES")
    fileMisCopyDbm        = ngamsDbm.ngamsDbm(fileMisCopyDbmName, writePerm=1)
 
    # - DB that contains information about files stored on the DB,
    # which are not registered in the NGAS DB. At the end of the function,
    # this will contain information about files found on the disk but
    # not registered in the NGAS DB.
    filesOnDiskDicDbmName = os.path.normpath(dbFilePat + "_FILES_ON_DISK")
    filesOnDiskDicDbm     = ngamsDbm.ngamsDbm(filesOnDiskDicDbmName,
                                              writePerm = 1)

    # - DB with information about files referenced by the query.
    if (not fileListDbmName):
        locFileListDbmName = os.path.normpath(dbFilePat + "_FILE_LIST")
        fileListDbm         = ngamsDbm.ngamsDbm(locFileListDbmName,
                                                writePerm = 1)
    else:
        fileListDbm       = ngamsDbm.ngamsDbm(fileListDbmName, writePerm = 0)

    # - Temporary DBM containing information about all File IDs defined
    # by the query.
    fileIdDbmName         = os.path.normpath(dbFilePat + "_FILE_IDS")
    fileIdDbm             = ngamsDbm.ngamsDbm(fileIdDbmName, writePerm = 1)

    # - Temporary DBM containing information about all files available in
    # the system with the File ID/File Version defined by the query.
    complFileListDbmName  = os.path.normpath(dbFilePat + "_COMPL_FILE_LIST")
    complFileListDbm      = ngamsDbm.ngamsDbm(complFileListDbmName,
                                              writePerm = 1)

    # - Temporary DBM that is used to figure out the number of independent
    # copies of each file concerned by the query.
    checkDicDbmName      = os.path.normpath(dbFilePat + "_CHECK_DIC")
    checkDicDbm          = ngamsDbm.ngamsDbm(checkDicDbmName, writePerm = 1)

    # A Disk ID but no file references are given. Retrieve information
    # about files concerned from the DB.
    if (diskId):
        info(4,"Retrieving information about files on disk with ID: " + diskId)
        tmpFileSumDbmName = os.path.normpath(dbFilePat + "_TMP_FILE_SUM")
        tmpFileSumDbmName = srvObj.getDb().\
                            dumpFileSummary1(tmpFileSumDbmName,
                                             None, [diskId], [],
                                             ignore=0, fileStatus=[])
        tmpFileSumDbm = ngamsDbm.ngamsDbm(tmpFileSumDbmName)        
        for key in range(0, tmpFileSumDbm.getCount()):
            tmpFileInfo = tmpFileSumDbm.get(str(key))
            fileListDbm.addIncKey(tmpFileInfo)
            fileId = tmpFileInfo[ngamsDbCore.SUM1_FILE_ID]
            fileIdDbm.add(fileId, "")
            fileVersion = tmpFileInfo[ngamsDbCore.SUM1_VERSION]
            fileKey = ngamsLib.genFileKey(None, fileId, fileVersion)
            checkDicDbm.add(fileKey, {})
        fileListDbm.sync()
        fileIdDbm.sync()
        checkDicDbm.sync()
        del tmpFileSumDbm
        rmFile(tmpFileSumDbmName + "*")
        
        # Get the list of files located on the disk. Later on, remove entries
        # from this dictionary as the files are parsed, based on their DB info,
        # further down in this method.
        #
        # Key in this dictionary is the complete filename of the file.
        info(4,"Get list of files stored on disk ...")
        tmpDiskInfo = srvObj.getDb().getDiskInfoFromDiskId(diskId)
        diskInfoObj = ngamsDiskInfo.ngamsDiskInfo().\
                      unpackSqlResult(tmpDiskInfo)
        if ((not ignoreMounted) and ( not diskInfoObj.getMounted())):
            errMsg = "Rejecting request for removing disk with ID: " +\
                     diskId + " - disk not mounted!"
            raise Exception, errMsg
        if (not ignoreMounted):
            basePath = os.path.normpath(diskInfoObj.getMountPoint())
            pattern = "/*"
            info(4,"Generating list with files on disk with base path: " +\
                 basePath)
            while (1):
                tmpFileList = glob.glob(basePath + pattern)
                if (len(tmpFileList) == 0):
                    break
                else:
                    for filename in tmpFileList:
                        if (os.path.isfile(filename) and
                            (os.path.basename(filename) != NGAMS_DISK_INFO) and
                            (os.path.basename(filename) !=
                             NGAMS_VOLUME_ID_FILE) and
                            (os.path.basename(filename) !=
                             NGAMS_VOLUME_INFO_FILE)):
                            filesOnDiskDicDbm.add(filename, "")
                    pattern += "/*"
                
    # Generate File ID DBM in case a file list DBM is given.
    if (fileListDbmName):
        info(4,"Handling file list DBM given in the function call ...")
        fileListDbm.initKeyPtr()
        while (1):
            key, tmpFileInfo = fileListDbm.getNext()
            if (not key): break

            # Update the File ID DBM.
            fileId = tmpFileInfo[ngamsDbCore.SUM1_FILE_ID]
            fileIdDbm.add(fileId, "")

            # Update the DBM with references to File ID/Version sets.
            fileVersion = tmpFileInfo[ngamsDbCore.SUM1_VERSION]
            fileKey = ngamsLib.genFileKey(None, fileId, fileVersion)
            checkDicDbm.add(fileKey, {})
        fileIdDbm.sync()
        checkDicDbm.sync()

    # We need to generate a list with all files available in the system
    # with the given File ID/File Version.
    info(4,"Retrieving information about all files available with the " +\
         "File ID/File Version as defined by the query")

    # Due to the limitation of the size of SQL queries, we have to split up
    # the SQL query in several sub-queries. The max. length of an SQL query
    # is defined by NGAMS_MAX_SQL_QUERY_SZ, we subtract 512 from this for
    # the general part of the query, and for each filename we calculate a
    # length of len(File ID) + 4 as contribution to the SQL query.
    maxQuerySize = (NGAMS_MAX_SQL_QUERY_SZ - 512)
    queryIds = []
    querySize = 0
    noOfFileIds = fileIdDbm.getCount()
    fileIdCount = 0
    fileIdDbm.initKeyPtr()
    fileId = "INIT"
    tmpFileSumDbmName = os.path.normpath(dbFilePat + "_TMP_FILE_SUM")
    while (fileId):
        fileId, dummy = fileIdDbm.getNext()
        if (fileId):
            queryIds.append(fileId)
            fileIdCount+= 1
            querySize += (len(fileId) + 4)
        if ((queryIds != []) and
            ((querySize >= maxQuerySize) or (fileIdCount == noOfFileIds))):
            tmpFileSumDbmName = srvObj.getDb().\
                                dumpFileSummary1(tmpFileSumDbmName,
                                                 None, [], queryIds,
                                                 ignore=None, fileStatus=[])
            tmpFileSumDbm = ngamsDbm.ngamsDbm(tmpFileSumDbmName)
            for key in range(0, tmpFileSumDbm.getCount()):
                # Take only a sub-result if that File ID + Version
                # is concerned by the query.
                tmpFileInfo = tmpFileSumDbm.get(str(key))
                tmpFileId = tmpFileInfo[ngamsDbCore.SUM1_FILE_ID]
                tmpFileVersion = tmpFileInfo[ngamsDbCore.SUM1_VERSION]
                tmpFileKey = ngamsLib.genFileKey(None,tmpFileId,tmpFileVersion)
                if (checkDicDbm.hasKey(tmpFileKey)):
                    complFileListDbm.addIncKey(tmpFileInfo)
            complFileListDbm.sync()
            del tmpFileSumDbm
            rmFile(tmpFileSumDbmName + "*")
            queryIds = []
            querySize = 0

    # Now, go through the files found and order these such that we end up with
    # a Dictionary with "<File ID>_<File Version>" as keys referring
    # to a dictionary with the Disk IDs of the disks hosting the files as
    # keys, and the information for each file on that disk as a tupple.
    #
    # It is the intention to figure out how many copies we have of each file
    # identified by File ID + File Version stored ON DIFFERENT STORAGE MEDIAS
    # + on different hosts.
    info(4,"Generate DBM DB with info about independent file copies ...")
    complFileListDbm.initKeyPtr()
    while (1):
        fileKey, fileInfo = complFileListDbm.getNext()
        if (not fileKey): break
        checkDicKey = ngamsLib.genFileKey(None,
                                          fileInfo[ngamsDbCore.SUM1_FILE_ID],
                                          fileInfo[ngamsDbCore.SUM1_VERSION])
        tmpDic = checkDicDbm.get(checkDicKey)
        tmpDic[fileInfo[ngamsDbCore.SUM1_DISK_ID]] = fileInfo
        checkDicDbm.add(checkDicKey, tmpDic)
    
    # Check if there are at least minReqCopies occurrences of the files +
    # check that all files are registered (if a Disk ID is specified).
    info(4,"Check for files with less copies than: " + str(minReqCopies))
    checkDicDbm.initKeyPtr()
    while (1):
        checkDicKey, tmpDic = checkDicDbm.getNext()
        if (not checkDicKey): break
    
        tmpDicKeys = tmpDic.keys()
        noOfCopies = len(tmpDicKeys)
        if (noOfCopies < minReqCopies):
            tmpFileInfo = tmpDic[tmpDicKeys[0]]
            fileId      = tmpFileInfo[ngamsDbCore.SUM1_FILE_ID]
            fileVersion = tmpFileInfo[ngamsDbCore.SUM1_VERSION]
            tmpFileObj = ngamsFileInfo.ngamsFileInfo().\
                         setFileId(fileId).\
                         setFileVersion(fileVersion).\
                         setTag("Independent copies: " + str(noOfCopies))
            fileKey = ngamsLib.genFileKey(None, fileId, fileVersion)
            fileMisCopyDbm.add(fileKey, tmpFileObj)
    
        # Remove this file from the Files On Disk DBM - do this only
        # if a Disk ID is specified.
        if (diskId):
            if (tmpDic.has_key(diskId)):
                fileInfo = tmpDic[diskId]
                filename = os.path.\
                           normpath(fileInfo[ngamsDbCore.SUM1_MT_PT] +\
                                    "/" + fileInfo[ngamsDbCore.SUM1_FILENAME])
                if (filesOnDiskDicDbm.hasKey(filename)):
                    filesOnDiskDicDbm.rem(filename)
        
    # Close all DBM objects.
    del fileMisCopyDbm
    del filesOnDiskDicDbm
    del fileListDbm
    del fileIdDbm
    del complFileListDbm
    del checkDicDbm
    
    # The DBM filesOnDiskDicDbmName now contains references to files,
    # which are found on the disk but not registered in the DB.
    return (fileMisCopyDbmName, filesOnDiskDicDbmName, complFileListDbmName)


# EOF
