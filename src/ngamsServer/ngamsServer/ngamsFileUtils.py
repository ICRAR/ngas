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

import binascii
import collections
import contextlib
import functools
import logging
import os
import re
import struct
import time

import ngamsSrvUtils
from ngamsLib import ngamsDbm, ngamsDbCore, ngamsDiskInfo, ngamsStatus, \
    ngamsHttpUtils
from ngamsLib import ngamsHighLevelLib
from ngamsLib.ngamsCore import TRACE, NGAMS_HOST_LOCAL, NGAMS_HOST_CLUSTER, \
    NGAMS_HOST_DOMAIN, rmFile, NGAMS_HOST_REMOTE, NGAMS_RETRIEVE_CMD, genLog, \
    NGAMS_STATUS_CMD, NGAMS_CACHE_DIR, \
    NGAMS_DATA_CHECK_THR, getFileSize, loadPlugInEntryPoint

_crc32c_available = True
try:
    import crc32c
except ImportError:
    _crc32c_available = False

logger = logging.getLogger(__name__)

# The checksum_info fields are:
#  * init: the initial value of the checksum before running over the data
#  * method: the accumulative checksum method invoked for each piece of data
#  * final: Converts the final checksum to get the final value
#  * from_bytes: converts a sequence of bytes into a checksum value
#
# In NGAS checksum values are treated as integers (and then stored as their
# string representation in the database), which is why the `final` and
# `from_bytes` functions need to be aligned.
checksum_info = collections.namedtuple('crc_info', 'init method final from_bytes equals')

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

    msg = "_locateArchiveFile() - Disk ID: %s - File ID: " +\
          "%s - File Version: %d ..."
    logger.debug(msg, str(diskId), fileId, int(fileVersion))

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
    hostDic = ngamsHighLevelLib.resolveHostAddress(srvObj.getHostId(),
                                                   srvObj.getDb(),
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
    if logger.level <= logging.DEBUG:
        msg = ""
        count = 1
        for fileVer in fileVerList:
            for fi in candFileDic[fileVer]:
                msg += "(" + str(count) + ": Location:" + fi[0] +\
                       ", Host:" + fi[2] + ", Version:" +\
                       str(fi[1].getFileVersion()) + ") "
                count += 1
        logger.debug("File list to check: " + msg)

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
    logger.debug("Disk Info Objects Dictionary: %s", str(diskInfoDic))

    # Check if the files are accessible - when the first accessible file
    # in the fileList is found, the information is returned as the file wanted.
    # To check the file accessibility, it is also checked if the NG/AMS
    # 'responsible' for the file, allows for Retrieve Requests (only done
    # in connection with a Retrieve Request).
    logger.debug("Checking which of the candidate files should be selected ...")
    foundFile   = 0
    for fileVer in fileVerList:
        if (foundFile): break

        for fileInfo in candFileDic[fileVer]:
            location    = fileInfo[0]
            fileInfoObj = fileInfo[1]
            host        = fileInfo[2]
            diskInfoObj = diskInfoDic[fileInfoObj.getDiskId()]
            port        = hostDic[host].getSrvPort()

            logger.debug("Checking candidate file with ID: %s on host/port: %s/%s. " + \
                         "Location: %s",
                         fileInfoObj.getFileId(), host, str(port), location)

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
                logger.debug("Checking if local file with name: %s is available", filename)
                if (not os.path.exists(filename)):
                    logger.debug(genLog("NGAMS_INFO_FILE_NOT_AVAIL", [fileId, host]))
                else:
                    logger.debug(genLog("NGAMS_INFO_FILE_AVAIL", [fileId, host]))
                    foundFile = 1
                    break
            else:
                logger.debug("Checking if file with ID/Version: %s/%s " +\
                             "is available on host/port: %s/%s",
                             fileInfoObj.getFileId(), str(fileInfoObj.getFileVersion()),
                             host, str(port))

                # If a server hosting a file is suspended, it is woken up
                # to be able to check if the file is really accessible.
                if (hostDic[host].getSrvSuspended() == 1):
                    logger.debug("Server hosting requested file (%s/%s) is suspended " + \
                                 "- waking up server ...",
                                 host, str(port))
                    try:
                        ngamsSrvUtils.wakeUpHost(srvObj, host)
                        logger.debug("Suspended server hosting requested file (%s/%s) " +\
                                     "has been woken up",
                                     host, str(port))
                    except Exception:
                        logger.exception("Error waking up server hosting selected " +\
                                "file")
                        continue

                # The file is hosted on a host, which is not suspended or
                # which was successfully woken up.
                pars = [["file_access", fileInfoObj.getFileId()]]
                if (fileInfoObj.getFileVersion() != -1):
                    pars.append(["file_version", fileInfoObj.getFileVersion()])
                ipAddress = hostDic[host].getIpAddress()
                authHdr = ngamsSrvUtils.genIntAuthHdr(srvObj)
                resp = ngamsHttpUtils.httpGet(ipAddress, port, NGAMS_STATUS_CMD,
                                        pars=pars, auth=authHdr)
                with contextlib.closing(resp):
                    data = resp.read()
                statusObj = ngamsStatus.ngamsStatus().unpackXmlDoc(data, 1)

                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("Result of File Access Query: %s",
                                 re.sub("\n", "", str(statusObj.genXml().toprettyxml('  ', '\n'))))
                if ((statusObj.getMessage().\
                     find("NGAMS_INFO_FILE_AVAIL") == -1)):
                    logger.debug(genLog("NGAMS_INFO_FILE_NOT_AVAIL", [fileId, host]))
                else:
                    logger.debug(genLog("NGAMS_INFO_FILE_AVAIL", [fileId, host]))
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
    msg = "Located suitable file for request - File ID: %s. " +\
          "Info for file found - Location: %s - Host ID/IP: %s/%s - " +\
          "Port Number: %s - File Version: %d - Filename: %s - " +\
          "Mime-type: %s"
    logger.debug(msg, fileId, location, host, ipAddress, port,
                 fileInfoObj.getFileVersion(), fileInfoObj.getFilename(),
                 fileInfoObj.getFormat())
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

    # Get a list with the candidate files matching the query conditions.
    fileDbmName = srvObj.getDb().dumpFileInfo(fileId, fileVersion, diskId,
                                              ignore=0)
    try:
        res = _locateArchiveFile(srvObj, fileId, fileVersion, diskId, hostId,
                                 reqPropsObj, fileDbmName)
        return res
    finally:
        rmFile(fileDbmName + "*")


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
    res = srvObj.getDb().getFileSummary3(fileId, hostId, domain, diskId,
                                         fileVersion, cursor=False)
    if res:
        host_id = res[0][0]
        if host_id == srvObj.getHostId():
            location = NGAMS_HOST_LOCAL
        else:
            location = NGAMS_HOST_REMOTE
        return [location] + list(res[0])

    return 8 * (None,)


def checkFile(srvObj,
              sum1FileInfo,
              checkReport,
              skipCheckSum = 0,
              executor=None):
    """
    Function to carry out a consistency check on a file.
    If `stop_evt` and `allowed_evt` are given, then `get_checksum_interruptible`
    is used internally by this method; otherwise `get_checksum` is used.
    If `executor` is given, then it is used to carry out the execution of the
    checksum calculation; otherwise `get_checksum` is used.
    """

    executor = executor or get_checksum

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
    crc_variant  = fileInfo[ngamsDbCore.SUM1_CHECKSUM_PI]
    fileId       = fileInfo[ngamsDbCore.SUM1_FILE_ID]
    fileVersion  = fileInfo[ngamsDbCore.SUM1_VERSION]
    dbFileSize   = fileInfo[ngamsDbCore.SUM1_FILE_SIZE]
    logger.debug("Checking file with ID: %s and filename: %s on NGAS host: %s",
                 fileId, filename, srvObj.getHostId())

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
            logger.error('File %s does not exist on disk', filename)

            checkReport.append(["ERROR: File in DB missing on disk", fileId,
                                fileVersion, slotId, diskId, filename])
            srvObj.db.set_valid_checksum(fileId, fileVersion, diskId, False)

        # Check if file has the correct size.
        if ((not foundProblem) and fileExists):
            fileSize = getFileSize(filename)
            if (fileSize != dbFileSize):
                foundProblem = 1
                logger.error('File %s has wrong size. Expected: %d/Actual: %d',
                             filename, dbFileSize, fileSize)
                format = "ERROR: File has wrong size. Expected: %d/Actual: %d."
                checkReport.append([format % (dbFileSize , fileSize), fileId,
                                    fileVersion, slotId, diskId, filename])
                srvObj.db.set_valid_checksum(fileId, fileVersion, diskId, False)

        # Check checksum if this test should not be skipped.
        if ((not foundProblem) and (not skipCheckSum)):
            if crc_variant is None:
                crc_variant = srvObj.getCfg().getCRCVariant()
            checksum_info = get_checksum_info(crc_variant)
            if crc_variant is not None:
                try:
                    blockSize = srvObj.getCfg().getBlockSize()
                    if blockSize == -1:
                        blockSize = 4096
                    checksum_typ = get_checksum_name(crc_variant)

                    # Calculate the checksum, possibly under the executor
                    start = time.time()
                    checksumFile = executor(blockSize, filename, crc_variant)
                    duration = time.time() - start

                    fsize_mb = getFileSize(filename) / 1024. / 1024.
                    logger.info("Checked %s in %.4f [s] using %s. Check ran at %.3f [MB/s]. Checksum file/db:  %s / %s",
                                filename, duration, str(crc_variant), fsize_mb / duration,
                                str(checksumFile), checksumDb)
                except Exception, e:
                    # We assume an IO error:
                    # "[Errno 2] No such file or directory"
                    # This problem has already been registered further
                    # up so we ignore it here.
                    rmFile(fileChecked)
                    logger.exception("Error while checking file %s", filename)
                    return
            else:
                checksumFile = ""
            if not checksumDb and checksumFile:
                checkReport.append(["NOTICE: Missing checksum - generated",
                                    fileId, fileVersion, slotId, diskId,
                                    filename])
                srvObj.getDb().setFileChecksum(srvObj.getHostId(),
                                               fileId, fileVersion, diskId,
                                               checksumFile, checksum_typ)
            elif not checksumDb and not checksumFile:
                checkReport.append(["NOTICE: Missing checksum - " +\
                                    "cannot generate - update DB",
                                    fileId, fileVersion, slotId, diskId,
                                    filename])
            elif not checksum_info.equals(checksumDb, checksumFile):
                logger.error("File %s has inconsistent checksum! file/db: %s / %s",
                             filename, str(checksumFile), checksumDb)
                checkReport.append(["ERROR: Inconsistent checksum found",
                                    fileId, fileVersion, slotId, diskId,
                                    filename])
                srvObj.db.set_valid_checksum(fileId, fileVersion, diskId, False)

        # If file is OK but is marked as 'bad', reset the flag.
        if not foundProblem:
            srvObj.db.set_valid_checksum(fileId, fileVersion, diskId, True)

        ## Set the checking flag back to 0 in the DB.
        #srvObj.getDb().setFileStatus(fileId, fileVersion, diskId, fileStatus)

        # Reset file indicating which data file is being checked.
        rmFile(fileChecked)
    except Exception:
        if (fileCheckedFo): fileCheckedFo.close()
        # Reset file indicating which data file is being checked.
        rmFile(fileChecked)
        raise


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
            logger.debug("Invoking Disk Sync Plug-In: %s ...", diskSyncPlugIn)
            plugInMethod = loadPlugInEntryPoint(diskSyncPlugIn)
            plugInMethod(srvObj)
            logger.debug("Invoked Disk Sync Plug-In: %s", diskSyncPlugIn)
        else:
            logger.warning("No Disk Sync Plug-In defined - consider to provide one!")
        #commands.getstatusoutput("sync")
        for file in filenames: os.stat(file)
    except Exception, e:
        errMsg = "Severe error occurred! Could not sync file caches or " +\
                 "file not accessible! Error: " + str(e)
        raise Exception, errMsg


CHECKSUM_NULL = -1
CHECKSUM_CRC32_INCONSISTENT = 0
CHECKSUM_CRC32C = 1
CHECKSUM_CRC32Z = 2

def _normalize_variant(variant_or_name):

    variant = variant_or_name

    if variant is None:
        variant = CHECKSUM_NULL

    # A plug-in name or variant name
    elif isinstance(variant, basestring):
        # In NGAS versions <= 8 the CRC was calculated as a separate step after
        # archiving a file, and therefore was loaded as a plugin that received a
        # filename when invoked.
        # These two are the names stored at the database of those plugins, although
        # the second one is simply a dummy name
        if variant in ('ngamsGenCrc32', 'StreamCrc32', 'crc32'):
            variant = CHECKSUM_CRC32_INCONSISTENT
        elif variant == 'crc32c':
            variant = CHECKSUM_CRC32C
        elif variant == 'crc32z':
            variant = CHECKSUM_CRC32Z
        else:
            variant = int(variant)

    return variant

def _filter_none(cond):
    def wrapped(x, y):
        if x is None:
            return y is None
        elif y is None:
            return x is None
        return cond(x, y)
    return wrapped

def get_checksum_info(variant_or_name):
    """
    Given a CRC variant, this method returns the method that should be
    continuously called to calculate the CRC of a given byte stream.

    The variant_or_name argument can be a number, where 0 is python's binascii
    crc32 implementation and 1 is Intel's SSE 4.2 CRC32c implementation, or a
    name indicating one of the old NGAMS plug-in names for performing CRC.
    The special value -1 means that no checksum is performed, and thus this
    method returns None
    """
    variant = _normalize_variant(variant_or_name)
    if variant == CHECKSUM_NULL:
        return None
    if variant == CHECKSUM_CRC32_INCONSISTENT:
        # This version of the crc is inconsistent because depending on the
        # python version binascii.crc32 returns signed or unsigned values.
        # python version <2.6 returned signed/unsigned depending on the platform,
        # 2.6+ returns always signed, 3+ returns always unsigned).
        # Since we support Python 2.7 only we assume values are signed,
        # but this will bite us in the future
        return checksum_info(0, binascii.crc32, lambda x: x, lambda x: struct.unpack('!i', x), _filter_none(lambda x, y: (int(x) & 0xffffffff) == (int(y) & 0xffffffff)))
    elif variant == CHECKSUM_CRC32C:
        if not _crc32c_available:
            raise Exception('Intel SSE 4.2 CRC32c instruction is not available')
        return checksum_info(0, crc32c.crc32, lambda x: x & 0xffffffff, lambda x: struct.unpack('!I', x), _filter_none(lambda x, y: int(x) == int(y)))
    elif variant == CHECKSUM_CRC32Z:
        # A consistent way of using binascii.crc32.
        return checksum_info(0, binascii.crc32, lambda x: x & 0xffffffff, lambda x: struct.unpack('!I', x), _filter_none(lambda x, y: int(x) == int(y)))
    raise Exception('Unknown CRC variant: %r' % (variant_or_name,))

def get_checksum_name(variant_or_name):
    """
    Given a CRC variant, this method returns the name used to denote that
    variant.

    The variant_or_name argument can be a number, where 0 is python's binascii
    crc32 implementation and 1 is Intel's SSE 4.2 CRC32c implementation, or a
    name indicating one of the old NGAMS plug-in names for performing CRC.
    The special value -1 means that no checksum is performed, and thus this
    method returns 'nocrc'
    """
    variant = _normalize_variant(variant_or_name)
    if variant == CHECKSUM_NULL:
        return None
    if variant == CHECKSUM_CRC32_INCONSISTENT:
        return 'crc32'
    elif variant == CHECKSUM_CRC32C:
        return 'crc32c'
    elif variant == CHECKSUM_CRC32Z:
        return 'crc32z'
    raise Exception('Unknown CRC variant: %d' % (variant_or_name,))

def get_checksum(blocksize, fin, checksum_variant):
    """
    Returns the checksum of a file (or file object) using the given checksum type.
    """
    crc_info = get_checksum_info(checksum_variant)
    if crc_info is None:
        return None

    crc_m = crc_info.method

    # fin can be a filename, in which case we open (and then close) it
    my_fileobj = None
    fileobj = fin
    if isinstance(fin, basestring):
        my_fileobj = fileobj = open(fin, 'rb')

    # Read and checksum, thank you very much
    read = fileobj.read
    crc = crc_info.init
    try:
        while True:
            block = read(blocksize)
            if not block:
                break
            crc = crc_m(block, crc)
    finally:
        # We opened it, we close it
        if my_fileobj:
            try:
                my_fileobj.close()
            except:
                pass

    crc = crc_info.final(crc)
    return crc

def get_checksum_interruptible(blocksize, filename, checksum_variant,
                               checksum_allow_evt, checksum_stop_evt):
    """
    Like get_checksum, but the inner loop's execution is conditioned by two
    events to signal a full stop, and whether the execution of the inner loop
    should continue or not.

    When the caller sets the `stop_evt`, the `allowed_evt` should also be set;
    otherwise the execution will hang indefinitely.
    """
    crc_info = get_checksum_info(checksum_variant)
    if crc_info is None:
        return None
    crc_m = crc_info.method
    crc = crc_info.init
    with open(filename, 'rb') as f:
        for block in iter(functools.partial(f.read, blocksize), ''):
            checksum_allow_evt.wait()
            if checksum_stop_evt.is_set():
                return
            crc = crc_m(block, crc)
    crc = crc_info.final(crc)
    return crc

def check_checksum(srvObj, fio, filename):
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
    crc_variant = fio.getChecksumPlugIn()
    checksum_info = get_checksum_info(crc_variant)
    stored_checksum = fio.getChecksum()
    if crc_variant is not None and stored_checksum is not None:
        stored_checksum = str(stored_checksum)
        blockSize = srvObj.getCfg().getBlockSize()
        if blockSize == -1:
            blockSize = 4096
        current_checksum = str(get_checksum(blockSize, filename, crc_variant))
        if not checksum_info.equals(current_checksum, stored_checksum):
            msg = "Illegal checksum (found: %s, expected %s) on file %s/%s/%s" % \
                  (current_checksum, stored_checksum, fio.getDiskId(), fio.getFileId(), fio.getFileVersion())
            raise Exception(msg)
    else:
        msg = "No checksum or checksum variant specified for file " +\
              "%s/%s/%s - skipping checksum check"
        logger.info(msg, fio.getDiskId(), fio.getFileId(), fio.getFileVersion())

# EOF