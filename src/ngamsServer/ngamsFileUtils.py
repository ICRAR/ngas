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
# "@(#) $Id: ngamsFileUtils.py,v 1.14 2010/06/23 09:49:43 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  24/04/03    Created
#

"""
Contains various utility functions used by the command handling callback
functions, to deal with archive files.
"""
import os, socket, cPickle, time

import pcc, PccUtTime, PccLog

from ngams import *
import ngamsDbm, ngamsDbCore, ngamsDb, ngamsLib, ngamsFileInfo, ngamsDiskInfo, ngamsStatus
import ngamsHighLevelLib
import ngamsSrvUtils


def _locateArchiveFile(srvObj,
                       fileId,
                       fileVersion,
                       diskId,
                       hostId,
                       reqPropsObj,
                       dbFilename):
    """
    See description of ngamsFileUtils.locateArchiveFile(). This function is
    used simply to encapsulate the complete processing to be able to clean up.
    """
    T = TRACE()

    format = "_locateArchiveFile() - Disk ID: %s - File ID: " +\
             "%s - File Version: %d ..."
    info(4, format % (str(diskId), fileId, int(fileVersion)))
    locTimer = PccUtTime.Timer()
    fileDbm = ngamsDbm.ngamsDbm(dbFilename)

    # Filter out files not on specified host if host ID is given.
    if (hostId):
        fileDbm.initKeyPtr()
        while (1):
            key, fileInfo = fileDbm.getNext()
            if (not key): break
            if (fileInfo[1] != hostId): fileDbm.rem(key)

    # If no file was found we raise an exception.
    if (not fileDbm.getCount()):
        tmpFileRef = fileId
        if (fileVersion > 0): tmpFileRef += "/Version: " + str(fileVersion)
        if (diskId): tmpFileRef += "/Disk ID: " + diskId
        if (hostId): tmpFileRef += "/Host ID: " + hostId
        errMsg = genLog("NGAMS_ER_UNAVAIL_FILE", [tmpFileRef])
        raise Exception, errMsg

    # We now sort the file information sub-lists in the file list.
    # The priori is as follows:
    #
    #   1. Local host.
    #   2. Same cluster.
    #   3. Same domain (e.g. hq.eso.org).
    #   4. Other files (remote files).
    localHostFileList = []
    clusterFileList   = []
    domainFileList    = []
    remoteFileList    = []
    hostList          = {}
    hostDic           = {}
    fileCount         = 0
    idx               = 0
    noOfFiles         = fileDbm.getCount()
    while (fileCount < noOfFiles):
        key = str(idx)
        if (not fileDbm.hasKey(key)):
            idx += 1
            continue
        hostId = fileDbm.get(key)[1]
        hostList[hostId] = ""
        idx += 1
        fileCount += 1
    hostDic = ngamsHighLevelLib.resolveHostAddress(srvObj.getDb(),
                                                   srvObj.getCfg(),
                                                   hostList.keys())

    # Loop over the candidate files and sort them.
    fileCount = idx = 0
    while (fileCount < noOfFiles):
        key = str(idx)
        if (not fileDbm.hasKey(key)):
            idx += 1
            continue
        fileInfo = fileDbm.get(key)
        fileHost = fileInfo[1]
        if (hostDic[fileHost].getHostType() == NGAMS_HOST_LOCAL):
            localHostFileList.append(fileInfo)
        elif (hostDic[fileHost].getHostType() == NGAMS_HOST_CLUSTER):
            clusterFileList.append(fileInfo)
        elif (hostDic[fileHost].getHostType() == NGAMS_HOST_DOMAIN):
            domainFileList.append(fileInfo)
        else:
            # NGAMS_HOST_REMOTE:
            remoteFileList.append(fileInfo)

        idx += 1
        fileCount += 1

    # Remove the BSD DB.
    del fileDbm
    rmFile(dbFilename + "*")

    # The highest priority of the file is determined by the File Version,
    # the latest version is preferred even though this may be stored
    # on another NGAS host.
    # A dictionary is built up, which contains the candidate files. The
    # format is such there each version found is one key. For each key
    # (= version) there is a list with the corresponding file information
    # order according to the location.
    candFileDic = {}
    fileLists = [[NGAMS_HOST_LOCAL,   localHostFileList],
                 [NGAMS_HOST_CLUSTER, clusterFileList],
                 [NGAMS_HOST_DOMAIN,  domainFileList],
                 [NGAMS_HOST_REMOTE,  remoteFileList]]
    for fileListInfo in fileLists:
        location = fileListInfo[0]
        fileList = fileListInfo[1]
        for fileInfo in fileList:
            fileVer = fileInfo[0].getFileVersion()
            # Create a list in connection with each File Version key.
            if (not candFileDic.has_key(fileVer)): candFileDic[fileVer] = []
            candFileDic[fileVer].append([location, fileInfo[0], fileInfo[1]])
    fileVerList = candFileDic.keys()
    fileVerList.sort()
    fileVerList.reverse()
    if ((PccLog.getVerboseLevel() >= 4) or (PccLog.getLogLevel() >= 4)):
        msg = ""
        count = 1
        for fileVer in fileVerList:
            for fi in candFileDic[fileVer]:
                msg += "(" + str(count) + ": Location:" + fi[0] +\
                       ", Host:" + fi[2] + ", Version:" +\
                       str(fi[1].getFileVersion()) + ") "
                count += 1
        info(4,"File list to check: " + msg)

    # If no files were found we raise an exception.
    if (len(candFileDic) == 0):
        if (fileVersion != -1):
            fileRef = fileId + "/V" + str(fileVersion)
        else:
            fileRef = fileId
        errMsg = genLog("NGAMS_ER_UNAVAIL_FILE", [fileRef])
        raise Exception, errMsg

    # We generate a list with the Disk IDs (which we need later).
    # Generate a dictionary with Disk Info Objects.
    diskIdDic = {}
    for fileVer in fileVerList:
        for fileInfo in candFileDic[fileVer]:
            diskIdDic[fileInfo[1].getDiskId()] = fileInfo[1].getDiskId()
    sqlDiskInfo = srvObj.getDb().getDiskInfoFromDiskIdList(diskIdDic.keys())
    diskInfoDic = {}
    for diskInfo in sqlDiskInfo:
        diskInfoObj = ngamsDiskInfo.ngamsDiskInfo().unpackSqlResult(diskInfo)
        diskInfoDic[diskInfoObj.getDiskId()] = diskInfoObj
    info(5,"Disk Info Objects Dictionary: " + str(diskInfoDic))

    # Check if the files are accessible - when the first accessible file
    # in the fileList is found, the information is returned as the file wanted.
    # To check the file accessibility, it is also checked if the NG/AMS
    # 'responsible' for the file, allows for Retrieve Requests (only done
    # in connection with a Retrieve Request).
    info(4,"Checking which of the candidate files should be selected ...")
    srcFileInfo = None
    foundFile   = 0
    for fileVer in fileVerList:
        if (foundFile): break

        for fileInfo in candFileDic[fileVer]:
            location    = fileInfo[0]
            fileInfoObj = fileInfo[1]
            host        = fileInfo[2]
            diskInfoObj = diskInfoDic[fileInfoObj.getDiskId()]
            port        = hostDic[host].getSrvPort()

            info(5,"Checking candidate file with ID: " +\
                 fileInfoObj.getFileId() + " on host/port: " +\
                 host + "/" + str(port) + ". Location: " + location)

            # If the file is stored locally we check if it is accessible,
            # otherwise we send a STATUS/file_access request to the
            # host in question.
            if (location == NGAMS_HOST_LOCAL):
                # Check first if the local system supports retrieve requests.
                # (if relevant).
                if (reqPropsObj):
                    if (reqPropsObj.getCmd() == NGAMS_RETRIEVE_CMD):
                        if (not srvObj.getCfg().getAllowRetrieveReq()):
                            continue

                # Check if the file is accessible.
                filename = os.path.normpath(diskInfoObj.getMountPoint()+"/" +\
                                            fileInfoObj.getFilename())
                info(3,"Checking if local file with name: " + filename +\
                     " is available ...")
                if (not os.path.exists(filename)):
                    info(3,genLog("NGAMS_INFO_FILE_NOT_AVAIL", [fileId, host]))
                else:
                    info(3,genLog("NGAMS_INFO_FILE_AVAIL", [fileId, host]))
                    foundFile = 1
                    break
            else:
                info(3,"Checking if file with ID/Version: "+\
                     fileInfoObj.getFileId() + "/" +\
                     str(fileInfoObj.getFileVersion()) +\
                     " is available on host/port: " + host + "/" + str(port) +\
                     " ...")

                # If a server hosting a file is suspended, it is woken up
                # to be able to check if the file is really accessible.
                if (hostDic[host].getSrvSuspended() == 1):
                    info(3,"Server hosting requested file (" + host + "/" +\
                         str(port) + ") is suspended - waking up server ...")
                    try:
                        ngamsSrvUtils.wakeUpHost(srvObj, host)
                        info(3,"Suspended server hosting requested file (" +\
                             host + "/" + str(port) + ") has been woken up")
                    except Exception, e:
                        warning("Error waking up server hosting selected " +\
                                "file. Error: " + str(e))
                        continue

                # The file is hosted on a host, which is not suspended or
                # which was successfully woken up.
                pars = [["file_access", fileInfoObj.getFileId()]]
                if (fileInfoObj.getFileVersion() != -1):
                    pars.append(["file_version", fileInfoObj.getFileVersion()])
                ipAddress = hostDic[host].getIpAddress()
                authHdr = ngamsSrvUtils.genIntAuthHdr(srvObj)
                statusInfo = ngamsLib.httpGet(ipAddress, port,
                                              NGAMS_STATUS_CMD, 1, pars,
                                              authHdrVal = authHdr)
                statusObj = ngamsStatus.ngamsStatus().\
                            unpackXmlDoc(statusInfo[3], 1)
                info(5,"Result of File Access Query: " +\
                     re.sub("\n", "", str(statusObj.genXml().\
                                          toprettyxml('  ', '\n'))))
                if ((statusObj.getMessage().\
                     find("NGAMS_INFO_FILE_AVAIL") == -1)):
                    info(3,genLog("NGAMS_INFO_FILE_NOT_AVAIL", [fileId, host]))
                else:
                    info(3,genLog("NGAMS_INFO_FILE_AVAIL", [fileId, host]))
                    foundFile = 1
                    break

    # If no file was found we raise an exception.
    if (not foundFile):
        errMsg = genLog("NGAMS_ER_UNAVAIL_FILE", [fileId])
        raise Exception, errMsg

    # The file was found, get the info necessary for the acquiring the file.
    ipAddress = hostDic[host].getIpAddress()
    srcFileInfo = [location, host, ipAddress, port,
                   diskInfoObj.getMountPoint(),
                   fileInfoObj.getFilename(), fileInfoObj.getFileId(),
                   fileInfoObj.getFileVersion(), fileInfoObj.getFormat()]
    format = "Located suitable file for request - File ID: %s. " +\
             "Info for file found - Location: %s - Host ID/IP: %s/%s - " +\
             "Port Number: %s - File Version: %d - Filename: %s - " +\
             "Mime-type: %s"
    info(2,format % (fileId, location, host, ipAddress, port,
                     fileInfoObj.getFileVersion(), fileInfoObj.getFilename(),
                     fileInfoObj.getFormat()))
    reqTime = locTimer.stop()
    return srcFileInfo


def locateArchiveFile(srvObj,
                      fileId,
                      fileVersion = -1,
                      diskId = "",
                      hostId = "",
                      reqPropsObj = None):
    """
    Locate the file indicated by the File ID. Returns a list containing
    the necessary information for retrieving the file:

      [<Location>, <File Host>, <IP Address>, <Port No>, <Mount Point>,
      <Filename>, <File ID>, <File Version>, <Mime-Type>]

    - whereby:

       <Location>     = Location of the file (NGAMS_HOST_LOCAL,
                        NGAMS_HOST_CLUSTER, NGAMS_HOST_DOMAIN,
                        NGAMS_HOST_REMOTE).
       <File Host>    = Host ID of host to be contacted to get access to the
                        file.
       <IP Address>   = IP Address of host to be contacted to get access to the
                        file.
       <Port No>      = Port number used by the NG/AMS Server.
       <Mount Point>  = Mount point at which the file is residing.
       <Filename>     = Name of file relative to mount point.
       <File ID>      = ID of file.
       <File Version> = Version of file.
       <Mime-Type>    = Mime-type of file (as registered in NGAS).

    srvObj:       Reference to NG/AMS server class object (ngamsServer).

    fileId:       File ID of file to locate (string).

    fileVersion:  Version of the file (integer).

    diskId:       ID of the disk hosting the file (string).

    hostId:       ID of the host where the file is located (string).

    reqPropsObj:  Request Property object to keep track of actions done during
                  the request handling (ngamsReqProps|None).

    Returns:      List with information about file location (list).
    """
    T = TRACE()

    locTimer = PccUtTime.Timer()

    # Get a list with the candidate files matching the query conditions.
    fileDbmName = srvObj.getDb().dumpFileInfo(fileId, fileVersion, diskId,
                                              ignore=0)
    try:
        res = _locateArchiveFile(srvObj, fileId, fileVersion, diskId, hostId,
                                 reqPropsObj, fileDbmName)
        rmFile(fileDbmName + "*")
        return res
    except Exception, e:
        rmFile(fileDbmName + "*")
        raise e


def quickFileLocate(srvObj,
                    reqPropsObj,
                    fileId,
                    hostId = None,
                    domain = None,
                    diskId = None,
                    fileVersion = -1):
    """
    Return one file matching the given criteria. A quick version of
    locateArchiveFile().

    fileId:            ID of file to retrieve (string).

    hostId:            Host ID of node hosting file (string|None).

    domain:            Domain in which the node is residing (string|None).

    diskId:            Disk ID of disk hosting file (string|None).

    fileVersion:       Version of file to retrieve (integer).

    Returns:           Tuple with the information:

                         (<Location>, <Host ID>, <Ip Address>, <Port>,
                          <Mountpoint>, <Filename>, <File Version>,
                          <format>) (tuple).
    """
    T = TRACE(5)

    res = srvObj.getDb().getFileSummary3(fileId, hostId, domain, diskId,
                                         fileVersion, cursor=False)
    if (res != [[]]):
        host_id = res[0][0][0]
        host_id = host_id.split(':')[0]  # if the host_id contains a port remove that
        if (host_id == getHostName()):
            location = NGAMS_HOST_LOCAL
        else:
            location = NGAMS_HOST_REMOTE
        retVal = [location] + list(res[0][0])
    else:
        retVal = eval("(" + (8 * ", None")[2:] + ")")
    return retVal


def checkFile(srvObj,
              sum1FileInfo,
              checkReport,
              skipCheckSum = 0):
    """
    Function to carry out a consistency check on a file located on
    the local host.

    srvObj:        Reference to NG/AMS server class object (ngamsServer).

    sum1FileInfo:  List with file information to be extracted using the
                   constants ngamsDbCore.SUM1_* (list).

    checkReport:   Check report with problems encountered in connection with
                   a file. It is a list containing sub-lists with the
                   information:

                     [<Msg>, <File ID>, <File Version>, <Slot ID>, <Disk ID>,
                      <Compl. Filename>]

                   Such new entries will be added in case discrepancies
                   are found (list/list).

    skipCheckSum:  If set to 1, no checksum test is done (integer/0|1).

    Returns:       Void.
    """
    T = TRACE(5)

    dataCheckPrio = srvObj.getCfg().getDataCheckPrio()
    foundProblem  = 0
    fileInfo      = sum1FileInfo
    diskId        = fileInfo[ngamsDbCore.SUM1_DISK_ID]
    slotId        = fileInfo[ngamsDbCore.SUM1_SLOT_ID]
    filename      = os.path.\
                    normpath(fileInfo[ngamsDbCore.SUM1_MT_PT] + "/" +\
                             fileInfo[ngamsDbCore.SUM1_FILENAME])
    if (fileInfo[ngamsDbCore.SUM1_CHECKSUM] == None):
        checksumDb = ""
    else:
        checksumDb = fileInfo[ngamsDbCore.SUM1_CHECKSUM].strip()
    dcpi         = fileInfo[ngamsDbCore.SUM1_CHECKSUM_PI]
    fileId       = fileInfo[ngamsDbCore.SUM1_FILE_ID]
    fileVersion  = fileInfo[ngamsDbCore.SUM1_VERSION]
    fileStatus   = fileInfo[ngamsDbCore.SUM1_FILE_STATUS]
    dbFileSize   = fileInfo[ngamsDbCore.SUM1_FILE_SIZE]
    info(6,"Checking file with ID: " + fileId + " and filename: " + filename +\
         " on NGAS host: " + getHostId() + " ...")

    ## Set the file status "checking bit" temporary to 1.
    #tmpFileStatus = fileStatus[0] + "1" + fileStatus[2:]
    #srvObj.getDb().setFileStatus(fileId, fileVersion, diskId, tmpFileStatus)

    # Create name of temporary file in "<NGAS Rt Pt>/cache" indicating which
    # file is being checked:
    # <NGAS Rt Pt>/cache/<Thr ID>___<Disk ID>___<File ID>___<File Version>.\
    # check
    fileChecked = os.path.normpath("%s|%s___%s___%s___%s.check" %
                                   (
                                    NGAMS_CACHE_DIR, NGAMS_DATA_CHECK_THR,
                                    diskId, fileId,
                                    str(fileVersion))).replace("/", "_")
    fileChecked = srvObj.getCfg().getRootDirectory() + '/' + fileChecked.replace("|","/")

    fileCheckedFo = None
    try:
        # Create file indicating which data file is being checked.
        rmFile(fileChecked)
        fileCheckedFo = open(fileChecked, "w")
        fileCheckedFo.close()

        # Check if file exists.
        fileExists = os.path.exists(filename)
        if (not fileExists):
            foundProblem = 1
            checkReport.append(["ERROR: File in DB missing on disk", fileId,
                                fileVersion, slotId, diskId, filename])
            fileStatus = "1" + fileStatus[1:]
            srvObj.getDb().setFileStatus(fileId, fileVersion, diskId,
                                         fileStatus)

        # Check if file has the correct size.
        if ((not foundProblem) and fileExists):
            fileSize = getFileSize(filename)
            if (fileSize != dbFileSize):
                foundProblem = 1
                format = "ERROR: File has wrong size. Expected: %d/Actual: %d."
                checkReport.append([format % (dbFileSize , fileSize), fileId,
                                    fileVersion, slotId, diskId, filename])
                fileStatus = "1" + fileStatus[1:]
                srvObj.getDb().setFileStatus(fileId, fileVersion, diskId,
                                             fileStatus)

        # Check checksum if this test should not be skipped.
        if ((not foundProblem) and (not skipCheckSum)):
            if (dcpi == None): dcpi = srvObj.getCfg().getChecksumPlugIn()
            if ((dcpi != None) and (dcpi != "")):
                try:
                    exec "import " + dcpi
                    checksumFile = eval(dcpi + "." + dcpi +\
                                        "(srvObj, filename, dataCheckPrio)")
                except Exception, e:
                    # We assume an IO error:
                    # "[Errno 2] No such file or directory"
                    # This problem has already been registered further
                    # up so we ignore it here.
                    rmFile(fileChecked)
                    return
            else:
                checksumFile = ""
            if ((checksumDb == "") and (checksumFile != "")):
                checkReport.append(["NOTICE: Missing checksum - generated",
                                    fileId, fileVersion, slotId, diskId,
                                    filename])
                srvObj.getDb().setFileChecksum(fileId, fileVersion, diskId,
                                               checksumFile, dcpi)
            elif ((checksumDb == "") and (checksumFile == "")):
                checkReport.append(["NOTICE: Missing checksum - " +\
                                    "cannot generate - update DB",
                                    fileId, fileVersion, slotId, diskId,
                                    filename])
            elif (checksumDb != checksumFile):
                checkReport.append(["ERROR: Inconsistent checksum found",
                                    fileId, fileVersion, slotId, diskId,
                                    filename])
                fileStatus = "1" + fileStatus[1:]
                srvObj.getDb().setFileStatus(fileId, fileVersion, diskId,
                                             fileStatus)

        # If file is OK but is marked as 'bad', reset the flag.
        if ((not foundProblem) and (fileStatus[0] == "1")):
            fileStatus = "0" + fileStatus[1:]
            srvObj.getDb().setFileStatus(fileId, fileVersion, diskId,
                                         fileStatus)

        ## Set the checking flag back to 0 in the DB.
        #srvObj.getDb().setFileStatus(fileId, fileVersion, diskId, fileStatus)

        # Reset file indicating which data file is being checked.
        rmFile(fileChecked)
    except Exception, e:
        if (fileCheckedFo): fileCheckedFo.close()
        # Reset file indicating which data file is being checked.
        rmFile(fileChecked)
        raise e


def syncCachesCheckFiles(srvObj,
                         filenames = []):
    """
    Synchronize the file caches and check that the given files are accessible.
    If problems are encountered, an exception is raised indicating which
    file cannot be accessed.

    srvObj:           Server object (ngamsServer).

    filenames:        Filelist to check (list).

    Returns:          Void.
    """
    try:
        diskSyncPlugIn = srvObj.getCfg().getDiskSyncPlugIn()
        if (diskSyncPlugIn):
            info(3,"Invoking Disk Sync Plug-In: %s ..." % diskSyncPlugIn)
            exec "import " + diskSyncPlugIn
            eval(diskSyncPlugIn + "." + diskSyncPlugIn + "(srvObj)")
            info(3,"Invoked Disk Sync Plug-In: %s" % diskSyncPlugIn)
            logFlush()
        else:
            notice("No Disk Sync Plug-In defined - consider to provide one!")
        #commands.getstatusoutput("sync")
        for file in filenames: os.stat(file)
    except Exception, e:
        errMsg = "Severe error occurred! Could not sync file caches or " +\
                 "file not accessible! Error: " + str(e)
        raise Exception, errMsg


def checkChecksum(srvObj,
                  fileInfoObj,
                  filename):
    """
    Check the checksum of a file by applying the given checksum method and
    checksum value given in a File Info Object on the file.

    srvObj:       Reference to server object (ngamsServer).

    fileInfoObj:  File Info Object with information about the file
                  (ngamsFileInfo).

    filename:     Name of file to check (string).

    Returns:      Void.

    Exceptions:   An exception is raised if the checksum is illegal.
    """
    # If a checksum value is available in NGAS Files, check the checksum
    # of the file.
    if (fileInfoObj.getChecksumPlugIn() and fileInfoObj.getChecksum()):
        dcpi = fileInfoObj.getChecksumPlugIn()
        exec "import " + dcpi
        checksumStgFile = eval(dcpi + "." + dcpi + "(srvObj, filename)")
        if (checksumStgFile != fileInfoObj.getChecksum()):
            msg = "Illegal checksum for file to re-archive. Reference " +\
                  "file: %s/%s/%s" % (fileInfoObj.getDiskId(),
                                      fileInfoObj.getFileId(),
                                      str(fileInfoObj.getFileVersion()))
            warning(msg)
            raise Exception, msg
    else:
        msg = "No checksum or checksum plug-in specified for source " +\
              "file: %s/%s/%s - skipping checksum check"
        info(1, msg % (fileInfoObj.getDiskId(), fileInfoObj.getFileId(),
                       str(fileInfoObj.getFileVersion())))


# EOF
