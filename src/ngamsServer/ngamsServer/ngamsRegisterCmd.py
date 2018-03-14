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
# "@(#) $Id: ngamsRegisterCmd.py,v 1.8 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  02/04/2002  Created
#
"""
Contains functions for handling the REGISTER command.
"""

import logging
import os
import threading
import time

import six

from ngamsLib.ngamsCore import TRACE, rmFile, NGAMS_HTTP_GET, \
    NGAMS_REGISTER_CMD, mvFile, getFileCreationTime, \
    NGAMS_FILE_STATUS_OK, genLog, NGAMS_SUCCESS, NGAMS_XML_MT, NGAMS_TEXT_MT, \
    NGAMS_NOTIF_INFO, NGAMS_DISK_INFO, NGAMS_VOLUME_ID_FILE, \
    NGAMS_VOLUME_INFO_FILE, NGAMS_REGISTER_THR, \
    NGAMS_ONLINE_STATE, NGAMS_IDLE_SUBSTATE, \
    NGAMS_BUSY_SUBSTATE, loadPlugInEntryPoint, toiso8601
from ngamsLib import ngamsDbm, ngamsReqProps, ngamsFileInfo, ngamsDbCore, \
    ngamsHighLevelLib, ngamsDiskUtils, ngamsLib, ngamsFileList, \
    ngamsNotification, ngamsDiskInfo, ngamsPlugInApi, ngamsCore
from . import ngamsArchiveUtils, ngamsCacheControlThread

logger = logging.getLogger(__name__)

def _registerExec(srvObj,
                  fileListDbmName,
                  tmpFilePat,
                  diskInfoDic,
                  reqPropsObj = None):
    """
    Register the files listed in the File List DBM (ngamsDbm), which
    match the mime-type(s) either specified in the 'mimeType' parameter,
    or if this is not specified, which match all the mime-types specified
    in the configuration file.

    When the registration procedure has been executed, the function sends
    an Email Notification message indicating which files were registered
    if the HTTP parameter 'notif_email' is given.

    The functions creates a File Info Objects per file handled and
    writes this in a temporary file, which is a DBM file. The
    keys in this DB is simply the file number in the sequence of files
    handled, pointing to a pickled ngamsFileInfo object.

    Each of the File Info Objects indicates if the file was registered or not.
    This is done by setting the tag of the File Info Object to one of the
    following values:

      REGISTERED:  The file was successfully registered in the NGAS DB

      FAILED:      The file was selected for registration but could not
                   be properly registered because of inconsistencies.
                   The status will be of the format: 'FAILED[: <reason>]'.

      REJECTED:    A file found under the specified path directory was
                   not accepted for cloning, usually because the mime-type
                   was not correct. The status will be of the format:
                   'REJECTED[: <reason>]'.

    Note, that registration is comparable to archiving of files. For that
    reason a DAPI must be provided for each type of file that should be
    registered. If this is not fullfilled, the file registration will
    fail.

    Only files stored on one of the NGAS disks configured in the
    configuration file, are considered. Files stored in other locations
    are ignored.

    srvObj:          Instance of NG/AMS Server object (ngamsServer).

    fileListDbmName: Name of a DBM containing the information
                     about the files to be registered. Each element in the
                     list is referred to by a key, which is a number.
                     These points to a pickled list for each file containing
                     the following information:

                       [<Filename>, <Disk ID>, <Mime-Type>]

                     The information for each disk concerned is also contained
                     in the DB referred to by its Disk ID and by its mount
                     point. The data is a pickled instance of the ngamsDiskInfo
                     class (string).

    tmpFilePat:      Pattern for temporary files used during the registration
                     process (string).

    diskInfoDic:     Dictionary with Disk IDs as keys pointing to the info
                     about the disk (dictionary/ngamsDiskInfo).

    reqPropsObj:     If an NG/AMS Request Properties Object is given, the
                     Request Status will be updated as the request is carried
                     out (ngamsReqProps).

    Returns:         Void.
    """
    T = TRACE()

    emailNotif = 0
    if (reqPropsObj):
        if (reqPropsObj.hasHttpPar("notif_email")):
            emailNotif = 1

    # Create the temporary BSD DB to contain the information for the
    # Email Notification Message.
    if (emailNotif):
        regDbmName = tmpFilePat + "_NOTIF_EMAIL"
        regDbm = ngamsDbm.ngamsDbm(regDbmName, writePerm = 1)

    # Open the DBM containing the list of files to (possibly) register.
    fileListDbm = ngamsDbm.ngamsDbm(fileListDbmName, writePerm = 1)

    # Want to parse files in alphabetical order.
    # TODO: Portatibility issue. Try to avoid UNIX shell commands for sorting.
    tmpFileList = tmpFilePat + "_FILE_LIST"
    rmFile(tmpFileList)
    with open(tmpFileList, "wb") as fo:
        fileListDbm.initKeyPtr()
        while (1):
            dbmKey, fileInfo = fileListDbm.getNext()
            if (not dbmKey): break
            fo.write(dbmKey + b"\n")
    sortFileList = tmpFilePat + "_SORT_FILE_LIST"
    rmFile(sortFileList)
    shellCmd = "sort %s > %s" % (tmpFileList, sortFileList)
    stat, out, err = ngamsCore.execCmd(shellCmd)
    if (stat != 0):
        raise Exception("Error executing command: %s. Error: %s, %s" %\
              (shellCmd, str(out), str(err)))
    rmFile(tmpFileList)

    # Go through each file in the list, check if the mime-type is among the
    # ones, which apply for registration. If yes try to register the file
    # by invoking the corresponding DAPI on the file.
    checksumPlugIn  = srvObj.getCfg().getChecksumPlugIn().strip()
    fileRegCount    = 0
    fileFailCount   = 0
    fileRejectCount = 0
    regTimeAccu     = 0.0
    fileCount       = 0
    fo = open(sortFileList)
    run = 1
    while (run):
        reg_start = time.time()
        dbmKey = fo.readline()
        if (dbmKey.strip() == ""):
            run = 0
            continue
        fileInfo    = fileListDbm.get(dbmKey[0:-1])
        filename    = fileInfo[0]
        diskId      = fileInfo[1]
        mimeType    = fileInfo[2]

        # Register the file. Check first, that exactly this file is
        # not already registered. In case it is, the file will be rejected.
        regPi = srvObj.getCfg().register_plugins[mimeType]
        logger.debug("Plugin found for %s: %s", mimeType, regPi)
        params = ngamsPlugInApi.parseRawPlugInPars(regPi.pars)
        tmpReqPropsObj = ngamsReqProps.ngamsReqProps().\
                         setMimeType(mimeType).\
                         setStagingFilename(filename).\
                         setTargDiskInfo(diskInfoDic[diskId]).\
                         setHttpMethod(NGAMS_HTTP_GET).\
                         setCmd(NGAMS_REGISTER_CMD).\
                         setSize(os.path.getsize(filename)).\
                         setFileUri(filename).\
                         setNoReplication(1)

        tmpFileObj = ngamsFileInfo.ngamsFileInfo()
        try:
            # Invoke Registration Plug-In.
            piName = regPi.name
            plugInMethod = loadPlugInEntryPoint(piName)
            piRes = plugInMethod(srvObj, tmpReqPropsObj, params)
            del tmpReqPropsObj

            # Check if this file is already registered on this disk. In case
            # yes, it is not registered again.
            files = srvObj.db.getFileSummary1(srvObj.getHostId(), [piRes.getDiskId()],
                                              [piRes.getFileId()])
            fileRegistered = 0
            for tmpFileInfo in files:
                tmpMtPt = tmpFileInfo[ngamsDbCore.SUM1_MT_PT]
                tmpFilename = tmpFileInfo[ngamsDbCore.SUM1_FILENAME]
                tmpComplFilename = os.path.normpath(tmpMtPt + "/" +\
                                                    tmpFilename)
                if (tmpComplFilename == filename):
                    fileRegistered = 1
                    break

            if (fileRegistered):
                fileRejectCount += 1
                tmpMsgForm = "REJECTED: File with File ID/Version: %s/%d " +\
                             "and path: %s is already registered on disk " +\
                             "with Disk ID: %s"
                tmpMsg = tmpMsgForm % (piRes.getFileId(),
                                       piRes.getFileVersion(), filename,
                                       piRes.getDiskId())
                logger.warning(tmpMsg + ". File is not registered again.")
                if (emailNotif):
                    tmpFileObj.\
                                 setDiskId(diskId).setFilename(filename).\
                                 setTag(tmpMsg)
                    regDbm.addIncKey(tmpFileObj)
                if (reqPropsObj):
                    reqPropsObj.incActualCount(1)
                    ngamsHighLevelLib.stdReqTimeStatUpdate(srvObj, reqPropsObj,
                                                           regTimeAccu)
                regTimeAccu += time.time() - reg_start
                fileCount += 1
                continue

            # Calculate checksum (if plug-in specified).
            if (checksumPlugIn != ""):
                logger.debug("Invoking Checksum Plug-In: %s to handle file: %s",
                             checksumPlugIn, filename)
                plugInMethod = loadPlugInEntryPoint(checksumPlugIn)
                checksum = plugInMethod(srvObj, filename, 0)
            else:
                checksum = ""

            # Move file and update information about file in the NGAS DB.
            mvFile(filename, piRes.getCompleteFilename())
            ngamsArchiveUtils.updateFileInfoDb(srvObj, piRes, checksum,
                                               checksumPlugIn)
            ngamsDiskUtils.updateDiskStatusDb(srvObj.getDb(), piRes)
            ngamsLib.makeFileReadOnly(piRes.getCompleteFilename())

            if (emailNotif):
                uncomprSize = piRes.getUncomprSize()
                ingestDate  = time.time()
                creDateSecs = getFileCreationTime(filename)
                tmpFileObj.\
                             setDiskId(diskId).\
                             setFilename(filename).\
                             setFileId(piRes.getFileId()).\
                             setFileVersion(piRes.getFileVersion()).\
                             setFormat(piRes.getFormat()).\
                             setFileSize(piRes.getFileSize()).\
                             setUncompressedFileSize(uncomprSize).\
                             setCompression(piRes.getCompression()).\
                             setIngestionDate(ingestDate).\
                             setIgnore(0).\
                             setChecksum(checksum).\
                             setChecksumPlugIn(checksumPlugIn).\
                             setFileStatus(NGAMS_FILE_STATUS_OK).\
                             setCreationDate(creDateSecs).\
                             setTag("REGISTERED")
            fileRegCount += 1

            # If running as a cache archive, update the Cache New Files DBM
            # with the information about the new file.
            if (srvObj.getCachingActive()):
                fileVer = fio.getFileVersion()
                ngamsCacheControlThread.addEntryNewFilesDbm(srvObj, diskId,
                                                            piRes.getFileId(),
                                                            fileVer, filename)

            # Generate a confirmation log entry.
            msg = genLog("NGAMS_INFO_FILE_REGISTERED",
                         [filename, piRes.getFileId(), piRes.getFileVersion(),
                          piRes.getFormat()])
            time.sleep(0.005)
            regTime = time.time() - reg_start
            msg = msg + ". Time: %.3fs." % (regTime)
            logger.info(msg, extra={'to_syslog': 1})
        except Exception as e:
            errMsg = genLog("NGAMS_ER_FILE_REG_FAILED", [filename, str(e)])
            logger.error(errMsg)
            if (emailNotif):
                tmpFileObj.\
                             setDiskId(diskId).setFilename(filename).\
                             setTag(errMsg)
            fileFailCount += 1
            regTime = time.time() - reg_start
            # TODO (rtobar, 2016-01): Why don't we raise an exception here?
            #      Otherwise the command appears as successful on the
            #      client-side

        # Add the file information in the registration report.
        if (emailNotif): regDbm.addIncKey(tmpFileObj)

        # Update request status time information.
        regTimeAccu += regTime
        if (reqPropsObj):
            reqPropsObj.incActualCount(1)
            ngamsHighLevelLib.stdReqTimeStatUpdate(srvObj, reqPropsObj,
                                                   regTimeAccu)
        fileCount += 1
    fo.close()
    rmFile(sortFileList)
    if (emailNotif): regDbm.sync()
    del fileListDbm
    rmFile(fileListDbmName + "*")

    # Final update of the Request Status.
    if (reqPropsObj):
        if (reqPropsObj.getExpectedCount() and reqPropsObj.getActualCount()):
            complPercent = (100.0 * (float(reqPropsObj.getActualCount()) /
                                     float(reqPropsObj.getExpectedCount())))
        else:
            complPercent =  100.0
        reqPropsObj.setCompletionPercent(complPercent, 1)
        reqPropsObj.setCompletionTime(1)
        srvObj.updateRequestDb(reqPropsObj)

    # Send Register Report with list of files cloned to a possible
    # requestor(select) of this.
    if (emailNotif):
        xmlStat = 0
        if (xmlStat):
            # Create an instance of the File List Class, used to store the
            # Registration Report.
            regStat = ngamsFileList.\
                      ngamsFileList("FILE_REGISTRATION_STATUS",
                                    "Status report for file " +\
                                    "registration")

            # Loop over the file objects in the BSD DB and add these
            # in the status object.
            regDbm.initKeyPtr()
            while (1):
                key, tmpFileObj = regDbm.getNext()
                if (not key): break
                regStat.addFileInfoObj(tmpFileObj)

            # Set overall status of registration procedure.
            regStat.setStatus("Files Found: " + str(fileCount + 1) + ", "+\
                              "Files Registered: " + str(fileRegCount) +\
                              ", " +\
                              "Files Failed: " + str(fileFailCount) + ", " +\
                              "Files Rejected: " + str(fileRejectCount))
            status = srvObj.genStatus(NGAMS_SUCCESS,
                                      "REGISTER command status report").\
                                      addFileList(regStat)
            statRep = status.genXmlDoc()
            statRep = ngamsHighLevelLib.addStatusDocTypeXmlDoc(srvObj, statRep)
            mimeType = NGAMS_XML_MT
        else:
            # Generate a 'simple' ASCII report.
            statRep = tmpFilePat + "_NOTIF_EMAIL.txt"
            fo = open(statRep, "w")
            if (reqPropsObj):
                path = reqPropsObj.getHttpPar("path")
            else:
                path = "-----"
            if (fileCount):
                timePerFile = (regTimeAccu / fileCount)
            else:
                timePerFile = 0
            tmpFormat = "REGISTER STATUS REPORT:\n\n" +\
                        "==Summary:\n\n" +\
                        "Date:                       %s\n" +\
                        "NGAS Host:                  %s\n" +\
                        "Search Path:                %s\n" +\
                        "Total Number of Files:      %d\n" +\
                        "Number of Registered Files: %d\n" +\
                        "Number of Failed Files:     %d\n" +\
                        "Number of Rejected Files:   %d\n" +\
                        "Total processing time (s):  %.3f\n" +\
                        "Handling time per file (s): %.3f\n\n" +\
                        "==File List:\n\n"
            fo.write(tmpFormat % (toiso8601(), srvObj.getHostId(), path, fileCount,
                                  fileRegCount, fileFailCount, fileRejectCount,
                                  regTimeAccu, timePerFile))
            tmpFormat = "%-80s %-32s %-3s %-10s\n"
            fo.write(tmpFormat %\
                     ("Filename", "File ID", "Ver", "Status"))
            fo.write(tmpFormat % (80 * "-", 32 * "-", 3 * "-", 10 * "-"))
            regDbm.initKeyPtr()
            while (1):
                key, tmpFileObj = regDbm.getNext()
                if (not key): break
                mtPt = diskInfoDic[tmpFileObj.getDiskId()].getMountPoint()
                filename = os.path.normpath(mtPt + "/" +\
                                            tmpFileObj.getFilename())
                line = tmpFormat %\
                       (filename, tmpFileObj.getFileId(),
                        str(tmpFileObj.getFileVersion()),
                        tmpFileObj.getTag())
                fo.write(line)

            fo.write(128 * "-")
            fo.write("\n\n==END\n")
            fo.close()
            mimeType = NGAMS_TEXT_MT

        # Send out the status report.
        emailAdrList = reqPropsObj.getHttpPar("notif_email").split(",")
        attachmentName = "RegisterStatusReport"
        if (reqPropsObj.hasHttpPar("path")):
            attachmentName += "-" + reqPropsObj.getHttpPar("path").\
                              replace("/", "_")
        ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(), NGAMS_NOTIF_INFO,
                                 "REGISTER STATUS REPORT", statRep,
                                 emailAdrList, 1, mimeType, attachmentName, 1)
        del regDbm
        rmFile(regDbmName + "*")
        rmFile(statRep)

    # Generate final status log + exit.
    if (fileCount > 0):
        timePerFile = (regTimeAccu / fileCount)
    else:
        timePerFile = 0.0

    msg = "Registration procedure finished processing Register Request - " + \
          "terminating. Files handled: %d. Total time: %.3fs. " + \
          "Average time per file: %.3fs."
    logger.debug(msg, fileCount, regTimeAccu, timePerFile)


def _registerThread(srvObj,
                    fileListDbmName,
                    tmpFilePat,
                    diskInfoDic,
                    reqPropsObj = None,
                    dummyPar = None):
    """
    See documentation for _registerExec().

    The purpose of this function is merely to excapsulate the actual
    cloning to make it possible to clean up in case an exception is thrown.
    """
    try:
        _registerExec(srvObj, fileListDbmName, tmpFilePat, diskInfoDic,
                      reqPropsObj)
        rmFile(tmpFilePat + "*")
        return
    except Exception:
        logger.exception("Exception raised in Register Sub-Thread")
        rmFile(tmpFilePat + "*")
        raise


def register(srvObj,
             path,
             mimeType = "",
             reqPropsObj = None,
             httpRef = None):
    """
    Function to generate a list of candidate files to register, and to
    launch a thread that actually carries out the registration.

    When the possible candidate files haven been selected, a reply is sent
    back to the requestor if the 'httpRef' object is given.

    srvObj:       Instance of NG/AMS Server object (ngamsServer).

    path:         The path name, which is used as starting point for
                  the searching for files (string).

    mimeType:     Comma separated list of mime-types, which should be
                  considered for registration (string).

    reqPropsObj:  If an NG/AMS Request Properties Object is given, the
                  Request Status will be updated as the request is carried
                  out (ngamsReqProps).

    httpRef:      Reference to the HTTP request handler
                  object (ngamsHttpRequestHandler).

    Returns:      Void.
    """
    T = TRACE()

    # Check if the given path or file exists.
    if (not os.path.exists(path)):
        errMsg = genLog("NGAMS_ER_FILE_REG_FAILED",
                        [path, "Non-existing file or path."])
        raise Exception(errMsg)

    # Check if the given path/file is on one of the NGAS Disks.
    foundPath = 0
    for slotId in srvObj.getDiskDic().keys():
        do = srvObj.getDiskDic()[slotId]
        if (path.find(do.getMountPoint()) == 0):
            foundPath = 1
            break
    if (not foundPath):
        errMsg = genLog("NGAMS_ER_FILE_REG_FAILED",
                        [path, "File or path specified is not located on an "
                         "NGAS Disk."])
        raise Exception(errMsg)

    # Create file pattern for temporary files.
    tmpFilePat=ngamsHighLevelLib.genTmpFilename(srvObj.getCfg(),"REGISTER_CMD")

    # Generate dictionary with mime-types to accept.
    mimeTypeDic = {}
    if (mimeType != ""):
        for mt in mimeType.split(","):
            try:
                mimeTypeDic[mt] = srvObj.getMimeTypeDic()[mt]
            except:
                errMsg = genLog("NGAMS_ER_REQ_HANDLING",
                                ["Mime-type specified: " + mt + " " +\
                                 "in connection with REGISTER command " +\
                                 "does not have a DAPI defined"])
                raise Exception(errMsg)
    else:
        mimeTypeDic = srvObj.getMimeTypeDic()

    # From the Disk Dictionary, generate a dictionary with mappings from
    # Disk ID + from Mount Point to the disk information for the disk.
    diskInfoDic = {}
    mtPt2DiskInfo = {}
    for slotId in srvObj.getDiskDic().keys():
        if (not srvObj.getCfg().getSlotIdDefined(slotId)): continue
        diskId              = srvObj.getDiskDic()[slotId].getDiskId()
        mtPt                = srvObj.getDiskDic()[slotId].getMountPoint()
        tmpDiskInfo         = ngamsDiskInfo.ngamsDiskInfo().\
                              read(srvObj.getDb(), diskId)
        diskInfoDic[diskId] = tmpDiskInfo
        mtPt2DiskInfo[mtPt] = tmpDiskInfo

    # Generate a list with all files found under the specified path, which
    # are candidates for being registered.
    fileListDbmName = tmpFilePat + "_FILE_LIST"
    rmFile(fileListDbmName + "*")
    fileListDbm = ngamsDbm.ngamsDbm(fileListDbmName, writePerm = 1)
    mimeTypeMappings = srvObj.getCfg().getMimeTypeMappings()
    tmpGlobFile = tmpFilePat + "_FILE_GLOB.glob"
    fileCount = 0
    searchPath = os.path.normpath(path)
    foundVolume = False
    for mtPt in mtPt2DiskInfo.keys():
        mtPt2 = mtPt
        if (mtPt[-1] != "/"): mtPt2 += "/"
        if (searchPath.find(mtPt2) == 0):
            foundVolume = True
            if os.path.isdir(searchPath):
                for root, dirs, files in os.walk(searchPath):
                    for nextFile in files:
                        if nextFile not in \
                        [NGAMS_DISK_INFO, \
                         NGAMS_VOLUME_ID_FILE, \
                         NGAMS_VOLUME_INFO_FILE]:
                            mimeType = ngamsLib.detMimeType(mimeTypeMappings,
                                                            nextFile, 0)
                            tmpFileInfo = [os.path.join(root,nextFile),
                                            mtPt2DiskInfo[mtPt].getDiskId(),
                                            mimeType]
                            fileListDbm.add(os.path.join(root,nextFile), tmpFileInfo)

            elif os.path.isfile(searchPath):  # the path is actually pointing to a file
                nextFile = os.path.basename(searchPath)
                root = os.path.dirname(searchPath)
                if nextFile not in \
                [NGAMS_DISK_INFO, \
                 NGAMS_VOLUME_ID_FILE, \
                 NGAMS_VOLUME_INFO_FILE]:
                    mimeType = ngamsLib.detMimeType(mimeTypeMappings,
                                                    nextFile, 0)
                    tmpFileInfo = [os.path.join(root,nextFile),
                                    mtPt2DiskInfo[mtPt].getDiskId(),
                                    mimeType]
                    fileListDbm.add(os.path.join(root,nextFile), tmpFileInfo)
        if foundVolume: break


#    pattern = ""
#    # TODO: Portatibility issue. The usage of UNIX commands should be
#    #       avoided if possible.
#    while (1):
#        # Use a shell commands here to avoid building up maybe huge lists in
#        # memory/in the Python interpreter. A better way could maybe be found
#        # avoiding to invoke a shell command.
#        rmFile(tmpGlobFile)
#        searchPath = os.path.normpath(path + pattern)
#        tmpCmd = "find " + searchPath + " -maxdepth 1 > " + tmpGlobFile
#        stat, out = commands.getstatusoutput(tmpCmd)
#        if (stat != 0): break
#        fo = open(tmpGlobFile)
#        while (1):
#            nextFile = fo.readline().strip()
#            if (nextFile == ""): break
#            if (nextFile.find("/" + NGAMS_DISK_INFO) != -1): continue
#            if (nextFile.find("/" + NGAMS_VOLUME_ID_FILE) != -1): continue
#            if (nextFile.find("/" + NGAMS_VOLUME_INFO_FILE) != -1): continue
#            if (os.path.isfile(nextFile)):
#                nextFile = os.path.normpath(nextFile)
#
#                # Find the host disk. Only files located on mounted NGAS
#                # disks are considered.
#                for mtPt in mtPt2DiskInfo.keys():
#                    mtPt2 = mtPt
#                    if (mtPt[-1] != "/"): mtPt2 += "/"
#                    if (nextFile.find(mtPt2) == 0):
#                        info(4,"Found candidate file for registering: " +\
#                             nextFile)
#                        # Take only files with a Registration Plug-In defined.
#                        mimeType = ngamsLib.detMimeType(mimeTypeMappings,
#                                                        nextFile, 0)
#                        regPi = srvObj.getCfg().getRegPiFromMimeType(mimeType)
#                        if (regPi != None):
#                            tmpFileInfo = [nextFile,
#                                           mtPt2DiskInfo[mtPt].getDiskId(),
#                                           mimeType]
#                            fileListDbm.add(nextFile, tmpFileInfo)
#                            break
#        fo.close()
#        pattern += "/*"
#    rmFile(tmpGlobFile)
    fileListDbm.sync()
    del fileListDbm

    # Send intermediate reply if the HTTP Reference object is given.
    async = 'async' in reqPropsObj and int(reqPropsObj['async'])
    if httpRef and async:
        logger.debug("REGISTER command accepted - generating immediate " +\
             "confimation reply to REGISTER command")

        # Update the request status in the Request Properties Object.
        reqPropsObj.\
                      setExpectedCount(fileCount).\
                      setActualCount(0).setCompletionPercent(0)
        srvObj.updateRequestDb(reqPropsObj)
        status = srvObj.genStatus(NGAMS_SUCCESS,
                                  "Accepted REGISTER command for execution").\
                                  setReqStatFromReqPropsObj(reqPropsObj).\
                                  setActualCount(0)

    # Launch the register thread or run the command in foreground if wait=1
    if async:
        args = (srvObj, fileListDbmName, tmpFilePat, diskInfoDic,
                reqPropsObj, None)
        thrName = NGAMS_REGISTER_THR + threading.current_thread().getName()
        regThread = threading.Thread(None, _registerThread, thrName, args)
        regThread.setDaemon(0)
        regThread.start()
    else:
        # Carry out the REGISTER Command (directly in this thread) and send
        # reply when this is done.
        _registerExec(srvObj, fileListDbmName, tmpFilePat, diskInfoDic,
                      reqPropsObj)
        msg = "Successfully handled command REGISTER"
        logger.debug(msg)
        status = srvObj.genStatus(NGAMS_SUCCESS, msg).\
                 setReqStatFromReqPropsObj(reqPropsObj).setActualCount(0)

    # Send reply if possible.
    if (httpRef):
        xmlStat = status.genXmlDoc(0, 0, 0, 1, 0)
        xmlStat = ngamsHighLevelLib.addStatusDocTypeXmlDoc(srvObj, xmlStat)
        httpRef.send_data(six.b(xmlStat), NGAMS_XML_MT)


def handleCmd(srvObj,
                      reqPropsObj,
                      httpRef):
    """
    Handle REGISTER command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of
                    actions done during the request handling
                    (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    T = TRACE()

    # Is this NG/AMS permitted to handle Archive Requests?
    if (not srvObj.getCfg().getAllowArchiveReq()):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Register"])
        raise Exception(errMsg)

    # Check if State/Sub-State correct for perfoming the cloning.
    srvObj.checkSetState("Command REGISTER", [NGAMS_ONLINE_STATE],
                         [NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE])

    # Get parameters.
    if (reqPropsObj.hasHttpPar("path")):
        path = reqPropsObj.getHttpPar("path")
    else:
        path =""
    if (reqPropsObj.hasHttpPar("mime_type")):
        mimeType = reqPropsObj.getHttpPar("mime_type")
    else:
        mimeType = ""

    # Carry out the registration.
    register(srvObj, path, mimeType, reqPropsObj, httpRef)


# EOF
