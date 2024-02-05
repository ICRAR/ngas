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
# "@(#) $Id: ngamsHighLevelLib.py,v 1.11 2010/04/13 19:13:23 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  25/03/2002  Created
#
"""
Contains higher level common functions.
"""

import contextlib
import email.mime.multipart
import errno
import logging
import os
import shutil
import smtplib
import socket
import threading
import random
import tempfile
import time

from .ngamsCore import genLog, NGAMS_HOST_LOCAL,\
    NGAMS_HOST_CLUSTER, NGAMS_HOST_DOMAIN, NGAMS_HOST_REMOTE,\
    NGAMS_UNKNOWN_MT, NGAMS_STAGING_DIR, NGAMS_TMP_FILE_PREFIX,\
    checkCreatePath, checkAvailDiskSpace,\
    getFileSize, NGAMS_BAD_FILES_DIR, NGAMS_BAD_FILE_PREFIX, NGAMS_STATUS_CMD,\
    mvFile, NGAMS_HTTP_UNAUTH, NGAMS_HTTP_SUCCESS, to_valid_filename
from . import ngamsLib, ngamsHostInfo, ngamsHttpUtils


logger = logging.getLogger(__name__)

# Dictionary with semaphores to ensure mutual exclusion on disk access
_diskMutexSems = {}


def getHostInfoFromHostIds(dbConObj,
                           hostList):
    """
    Return a dictionary with the information in connection with each host.
    If for a host ID given, no information is found in ngas_hosts, the
    value for this wil be None.

    hostList:     List of host IDs (list/string).

    Returns:      Dictionary with ngamsHostInfo objects. Keys are the
                  host names. If for a host name no information was
                  found, None will be the value (dictionary).
    """
    resHostList = dbConObj.getHostInfoFromHostIds(hostList)
    hostDic = {}
    for host in resHostList:
        hostInfo = ngamsHostInfo.ngamsHostInfo().unpackFromSqlQuery(host)
        hostDic[hostInfo.getHostId()] = hostInfo
    return hostDic


def updateSrvHostInfo(dbConObj, hostInfoObj):
    """
    Update the information in the DB, which is managed by the server
    itself. All members of the ngamsHostInfo object starting with 'setSrv'
    are written from the object into the DB. The member set by
    'setHostId()' must be defined as well.

    dbConObj:        Instance of NG/AMS DB class (ngamsDb).

    hostInfoObj:     Instance of the ngamsHostInfo class. The
                     information in this object will be written in
                     the DB (ngamsHostInfo).

    ignoreErr:       If set to 1, a possible exception thrown will be
                     caught, and this error ignored. Otherwise the
                     method will throw an exception itself (integer/0|1).

    Returns:         Void.
    """
    dbConObj.updateSrvHostInfo(hostInfoObj.getHostId(),
                               [hostInfoObj.getSrvVersion(),
                                hostInfoObj.getSrvPort(),
                                hostInfoObj.getSrvArchive(),
                                hostInfoObj.getSrvRetrieve(),
                                hostInfoObj.getSrvProcess(),
                                hostInfoObj.getSrvRemove(),
                                hostInfoObj.getSrvDataChecking(),
                                hostInfoObj.getSrvState()])


def _addHostInDic(dbConObj,
                  hostId,
                  hostDic):
    """
    Internal function to add host information in a dictionary.

    dbConObj:    DB object used when accessing the DB (ngamsDb).

    hostId:      ID of host to add (string).

    hostDic:     Dictionary with host IDs as keys pointing to instances
                 of ngamsHostInfo (dictionary).

    Returns:     Void.
    """
    tmpHostInfo = dbConObj.getHostInfoFromHostIds([hostId])
    if not tmpHostInfo:
        raise Exception(genLog("NGAMS_AL_MIS_HOST", [hostId]))
    sqlHostInfo = tmpHostInfo[0]
    hostDic[hostId] = ngamsHostInfo.ngamsHostInfo().\
                      unpackFromSqlQuery(sqlHostInfo)


def resolveHostAddress(localHostId,
                       dbConObj,
                       ngamsCfgObj,
                       hostList):
    """
    Generate a dictionary mapping hostnames into the name of the host that
    should actually be contacted for communication. This is done since when
    using the concept of Clusters having a Master Unit as entry point, rather
    than contacting one of the hosts within the Private Network of the NGAS
    Cluster, the Master Unit of the cluster should be contacted.

    If for a host no information is found in the NGAS DB, the same port
    number as for the contacted host is taken.

    dbConObj:    DB object used when accessing the DB (ngamsDb).

    hostList:    List containing names of hosts for which to find
                 the corresponding port numbers (list/string).

    Returns:     Dictionary with hostnames as keys containing
                 ngamsHostInfo objects (dictionary).
    """
    try:
        hostInfoDic = getHostInfoFromHostIds(dbConObj, hostList)
    except:
        hostInfoDic = {}
        for host in hostList:
            hostInfoDic[host] = None

    if localHostId not in hostInfoDic:
        _addHostInDic(dbConObj, localHostId, hostInfoDic)
    for hostName in hostList:
        if hostName not in hostInfoDic:
            errMsg = genLog("NGAMS_AL_MIS_HOST", [hostName])
            raise Exception(errMsg)
        hi = hostInfoDic[hostName]

        # Find out if this host is local, within a cluster, within the same
        # domain or remote.
        hostIpInfo = hi.getIpAddress().split(".")
        if (hi.getHostId() == localHostId):
            hi.setHostType(NGAMS_HOST_LOCAL)
        elif (hi.getClusterName() == hostInfoDic[localHostId].getClusterName()):
            # Host is within the same cluster as where this request is handled.
            hi.setHostType(NGAMS_HOST_CLUSTER)
        elif (hi.getDomain() == hostInfoDic[localHostId].getDomain()):
            # Host is within the same domain. Set the information about the
            # host to be contacted for handling the request.
            clusterName = hi.getClusterName()
            if ((clusterName == None) or (clusterName.strip() == "")):
                raise Exception("No Cluster Name specified in NGAS DB for " +\
                      "host: " + hi.getHostId())
            if clusterName not in hostInfoDic:
                _addHostInDic(dbConObj, clusterName, hostInfoDic)
            hi.\
                 setHostType(NGAMS_HOST_DOMAIN).\
                 setHostId(hostInfoDic[clusterName].getHostId()).\
                 setIpAddress(hostInfoDic[clusterName].getIpAddress())
        else:
            # It's a remote host somewhere 'in the world'. Set the information
            # about the host to be contacted for handling the request.
            clusterName = hi.getClusterName()
            if clusterName not in hostInfoDic:
                _addHostInDic(dbConObj, clusterName, hostInfoDic)
            hi.\
                 setHostType(NGAMS_HOST_REMOTE).\
                 setHostId(hostInfoDic[clusterName].getHostId()).\
                 setIpAddress(hostInfoDic[clusterName].getIpAddress())

    return hostInfoDic


def addStatusDocTypeXmlDoc(xml, host):
    """
    Generates an XML document (as an ASCII document) with the proper
    document type definition in it, e.g.:


    <!DOCTYPE NgamsStatus SYSTEM
       http://acngast1.hq.eso.org:7777/RETRIEVE?internal=ngamsCfg.dtd>

    xmlDoc:       XML document in ASCII format (string).

    host:         The host serving this document (string).

    Returns:      XML document generated.
    """

    docType = "<!DOCTYPE NgamsStatus SYSTEM \"http://%s/RETRIEVE?internal=ngamsStatus.dtd\">" % host
    xmlDocList = xml.splitlines()
    xmlDocList = [xmlDocList[0]] + [docType] + xmlDocList[1:]
    return '\n'.join(xmlDocList)


def determineMimeType(ngamsCfgObj,
                      filename,
                      noException = 0):
    """
    Determine mime-type of a file, based on the information in the
    NG/AMS Configuration and the filename (extension) of the file.

    ngamsCfgObj:   NG/AMS Configuration Object (ngamsConfig).

    filename:      Filename (string).

    noException:   If the function should not throw an exception
                   this parameter should be 1. In that case it
                   will return NGAMS_UNKNOWN_MT (integer).

    Returns:       Mime-type (string).
    """
    return ngamsLib.detMimeType(ngamsCfgObj.getMimeTypeMappings(),
                                filename, noException)


def acquireDiskResource(ngamsCfgObj,
                        slotId):
    """
    Acquire access right to a disk resource.

    ngamsCfgObj:   NG/AMS Configuration Object (ngamsConfig).

    slotId:        Slot ID referring to the disk resource (string).

    Returns:       Void.
    """
    storageSet = ngamsCfgObj.getStorageSetFromSlotId(slotId)
    if (not storageSet.getMutex()): return

    global _diskMutexSems
    if slotId not in _diskMutexSems:
        _diskMutexSems[slotId] = threading.Semaphore(1)
    code = str(abs(random.gauss(10000000,10000000))).split(".")[0]
    logger.debug("Requesting access to disk resource with Slot ID: %s (Code: %s)",
                 slotId, str(code))
    _diskMutexSems[slotId].acquire()
    logger.debug("Access granted")


def releaseDiskResource(ngamsCfgObj,
                        slotId):
    """
    Release a disk resource acquired with
    ngamsHighLevelLib.acquireDiskResource().

    ngamsCfgObj:   NG/AMS Configuration Object (ngamsConfig).

    slotId:        Slot ID referring to the disk resource (string).

    Returns:       Void.
    """
    storageSet = ngamsCfgObj.getStorageSetFromSlotId(slotId)
    if (not storageSet.getMutex()): return

    global _diskMutexSems
    if slotId not in _diskMutexSems:
        _diskMutexSems[slotId] = threading.Semaphore(1)
    logger.debug("Releasing disk resource with Slot ID: %s", slotId)
    _diskMutexSems[slotId].release()


def genProcDirName(ngamsCfgObj):
    """
    Generate a unique Processing Directory.

    ngamsCfgObj:    NG/AMS Configuration Object (ngamsConfig).

    Returns:
    """
    procdir = ngamsCfgObj.getProcessingDirectory()
    return tempfile.mkdtemp(dir=procdir)


def checkAddExt(ngamsCfgObj,
                mimeType,
                filename):
    """
    Check that a given filename has the extension as expected from the
    mime-type supposed for the file. If the file does not have the
    proper extension, the latter is added.

    ngamsCfgObj:    NG/AMS Configuration (ngamsConfig).

    mimeType:       Expected mime-type of the data file (string).

    filename:       Filename as given in the URI in the HTTP request (string).

    Returns:        Filename possibly with proper extension added (string).
    """
    expExt = ngamsCfgObj.getExtFromMimeType(mimeType)
    if (expExt == NGAMS_UNKNOWN_MT):
        msg = "Mime-type specified: %s is not defined in the mime-type " +\
              "mappings in the configuration"
        logger.info(msg, mimeType)
        return filename
    elif len(expExt) == 0:
        msg = "No extension added to filename due to configuration mapping."
        logger.debug(msg)
        return filename
    if ((filename.rfind(expExt) + len(expExt)) != len(filename)):
        filename = "%s.%s" % (filename, expExt)
        logger.debug("New filename: %s", filename)
    elif filename.rfind(expExt) == -1: # this fixes the case where len(filename)==2
        filename = "%s.%s" % (filename, expExt)
        logger.debug("New filename: %s", filename)
    else:
        logger.debug("No extension added to filename")
    return filename


def genStagingFilename(ngamsCfgObj,
                       reqPropsObj,
                       trgDiskInfo,
                       filename,
                       genTmpFiles = 0):
    """
    Generate a Staging Area Filename in which the data is received at first.

    ngamsCfgObj:    NG/AMS Configuration (ngamsConfig).

    reqPropsObj:    NG/AMS Request Properties object (ngamsReqProps).

    diskDic:        Dictionary containing ngamsPhysDiskInfo objects
                    with the information about the disk configuration
                    (dictionary).

    storageSetId:   Storage Set ID (string).

    filename:       Filename as given in the URI in the HTTP request (string).

    genTmpFiles:    If set to 1, the following filenames are generated and
                    returned as a tuple:

                      - Temporary Staging Filename.
                      - (Processing) Staging Filename.
                      - Temporary Request Properties File.
                      - Request Properties File.

    Returns:        Staging Area Filename or tuple with the 5
                    administrative staging files as described above
                    (string|tuple).
    """
    logger.debug("Generating staging filename - Disk ID: %s - URI: %s",
                 trgDiskInfo.getDiskId(), filename)
    try:
        tmpFilename = to_valid_filename(filename)

        # Check proper extension and ensure to obtain a unique name.
        tmpFilename = checkAddExt(ngamsCfgObj, reqPropsObj.getMimeType(),
                                  tmpFilename)

        mountPt = trgDiskInfo.getMountPoint()
        staging_dir = os.path.join(mountPt, NGAMS_STAGING_DIR)
        checkCreatePath(staging_dir)
        stagingFilename = tempfile.mktemp(suffix="-" + tmpFilename, prefix='', dir=staging_dir)

        logger.debug("Staging filename is: %s", stagingFilename)
        reqPropsObj.setStagingFilename(stagingFilename)

        if (genTmpFiles):
            tmpStagingFilename = tempfile.mktemp(suffix=tmpFilename,
                                                 prefix=NGAMS_TMP_FILE_PREFIX,
                                                 dir=staging_dir)
            tmpReqPropFilename = tmpStagingFilename + ".pickle"
            reqPropFilename = stagingFilename + ".pickle"

        logger.debug("Generated staging filename: %s", stagingFilename)
        if (genTmpFiles):
            return (tmpStagingFilename, stagingFilename, tmpReqPropFilename,
                    reqPropFilename)
        else:
            return stagingFilename
    except Exception as e:
        errMsg = "Problem generating Staging Filename " +\
                 "(in ngamsHighLevelLib.genStagingFilename()). Exception: " +\
                 str(e)
        raise Exception(errMsg)


def checkIfFileExists(dbConObj,
                      fileId,
                      diskId,
                      fileVersion,
                      completeFilename):
    """
    Checks if a file already has been successfully archived. This
    is determined by the file having an entry in the ngas_files DB
    and that the file is located on the disk.

    dbConObj:           DB connection object (ngamsDb).

    fileId:             File ID (string).

    diskId:             Disk ID (string).

    fileVersion:        Version of the file (integer).

    completeFilename:   Complete filename (string).

    Returns:            1 if file exists, 0 if not (integer).
    """
    if (not dbConObj.fileInDb(diskId, fileId, fileVersion)): return 0
    return os.path.exists(completeFilename)


def copyFile(ngamsCfgObj,
             srcFileSlotId,
             trgFileSlotId,
             srcFilename,
             trgFilename):
    """
    Copy a file, possibly between two different disks.

    ngamsCfgObj:        Instance of NG/AMS Configuration (ngamsConfig).

    srcFileSlotId:      Slot ID for source file (string).

    trgFileSlotId:      Slot ID for target file (string).

    srcFilename:        Source file filename (string).

    trgFilename:        Target file filename (string).

    Returns:            Tuple. Element 0: Time in took to move
                        file (s) (tuple).
    """
    logger.debug("Copying file: %s to filename: %s ...",
                 srcFilename, trgFilename)
    try:
        # Make mutual exclusion on disk access (if requested).
        acquireDiskResource(ngamsCfgObj, srcFileSlotId)
        if (srcFileSlotId != trgFileSlotId):
            acquireDiskResource(ngamsCfgObj, trgFileSlotId)
        try:
            checkCreatePath(os.path.dirname(trgFilename))
            fileSize = getFileSize(srcFilename)
            checkAvailDiskSpace(trgFilename, fileSize)

            start = time.time()
            # Make target file writable if existing.
            if (os.path.exists(trgFilename)):
                os.chmod(trgFilename, 420)
            shutil.copyfile(srcFilename, trgFilename)
            deltaTime = time.time() - start

            logger.debug("File: %s copied to filename: %s",
                         srcFilename, trgFilename)
        except Exception as e:
            errMsg = genLog("NGAMS_AL_CP_FILE",
                            [srcFilename, trgFilename, str(e)])
            raise Exception(errMsg)

        # Release disk resouces
        releaseDiskResource(ngamsCfgObj, srcFileSlotId)
        if (srcFileSlotId != trgFileSlotId):
            releaseDiskResource(ngamsCfgObj, trgFileSlotId)
        return [deltaTime]
    except:
        # Release disk resouces
        releaseDiskResource(ngamsCfgObj, srcFileSlotId)
        if (srcFileSlotId != trgFileSlotId):
            releaseDiskResource(ngamsCfgObj, trgFileSlotId)
        raise


def getNewFileVersion(dbConObj,
                      fileId):
    """
    The function figures out which should be the File Version for
    a new file with the given ID being archived.

    dbConObj:           DB connection object (ngamsDb).

    fileId:             ID of file (integer).

    Returns:            File version for new file with the
                        given ID (integer).
    """
    latestFileVersion = dbConObj.getLatestFileVersion(fileId)
    if (latestFileVersion == -1):
        return 1
    else:
        return (latestFileVersion + 1)


def moveFile2BadDir(disk, srcFilename):
    """
    Move a file to the Bad File Directory on the destination disk.

    Returns:         Name of filename in Bad Files Area (string).
    """

    badFilesDir = os.path.join(disk.getMountPoint(), NGAMS_BAD_FILES_DIR)
    checkCreatePath(badFilesDir)

    trgFilename = tempfile.mktemp(suffix="-" + os.path.basename(srcFilename),
                                  prefix=NGAMS_BAD_FILE_PREFIX,
                                  dir=badFilesDir)
    mvFile(srcFilename, trgFilename)
    logger.info("Moved %s to Bad Files Area %s", srcFilename, trgFilename)
    return trgFilename


def performBackLogBuffering(ngamsCfgObj,
                            reqPropsObj,
                            ex):
    """
    Determine whether the circumstances qualify for doing back-log
    buffering of the data.

    ngamsCfgObj:     Configuration object (ngamsConfig).

    reqPropsObj:     Request handling properties object (ngamsReqProps).

    ex:              Exception thrown (Exception).

    Returns:         1 if Back-Log Buffering should be done,
                     0 if not (integer).
    """
    # If the error occurred is one of the following:
    #
    #   o NGAMS_ER_DB_COM:
    #   o NGAMS_ER_PROB_STAGING_AREA:
    #   o NGAMS_ER_PROB_BACK_LOG_BUF:
    #   o NGAMS_AL_MV_FILE:
    #
    # - the data of the file being handled (archived) will be buffered in the
    # Data File Back-Log Directory and handled at a later stage. A condition in
    # this connection is that no the data or part of it, has not yet been
    # received, or that the _entire_ file has been received.
    dataSize = reqPropsObj.getSize()
    if (ngamsCfgObj.getBackLogBuffering() and \
        ((reqPropsObj.getBytesReceived() == 0) or \
         (reqPropsObj.getBytesReceived() == dataSize))):
        tstErrs = ["NGAMS_ER_DB_COM:",
                   "NGAMS_ER_PROB_STAGING_AREA:",
                   "NGAMS_ER_PROB_BACK_LOG_BUF:",
                   "NGAMS_AL_MV_FILE:"]
        exErr = str(ex)
        for err in tstErrs:
            if (exErr.find(err) != -1): return 1
    return 0


def pingServer(host, port, timeout):
    """
    The function tries to ping (sends STATUS command) to the NG/AMS Server
    running on the given host using the given port number.
    """
    logger.debug("Pinging NG/AMS Server: %s:%d. Timeout: %.3f [s]", host, port, timeout)

    startTime = time.time()
    while True:
        try:
            resp = ngamsHttpUtils.httpGet(host, port, NGAMS_STATUS_CMD)
            with contextlib.closing(resp):
                if resp.status in (NGAMS_HTTP_SUCCESS, NGAMS_HTTP_UNAUTH):
                    logger.debug("Successfully pinged NG/AMS Server")
                    return
        except socket.error as e:
            if e.errno != errno.ECONNREFUSED:
                raise
            if (time.time() - startTime) >= timeout:
                break
            time.sleep(0.2)

    errMsg = "NGAS Server running on %s:%d did not respond within %.3f [s]"
    raise Exception(errMsg % (host, port, timeout))


def stdReqTimeStatUpdate(srvObj,
                         reqPropsObj,
                         accumulatedTime):
    """
    Function to update the time statistics in an instance of the Request
    Properties Object.

    srvObj:             Reference to instance of server class (ngamsServer).

    reqPropsObj:        Request Property object to keep track of actions done
                        during the request handling (ngamsReqProps).

    accumulatedTime:    Accumulated time used to handle the request so
                        far (float).

    Return:             Updated Request Properties object (ngamsReqProps).
    """
    if (reqPropsObj.getActualCount() and reqPropsObj.getExpectedCount()):
        complPercent = (100.0 * (float(reqPropsObj.getActualCount()) /
                                 float(reqPropsObj.getExpectedCount())))
        reqPropsObj.setCompletionPercent(complPercent, 1)
        avgTimePerFile = (accumulatedTime/float(reqPropsObj.getActualCount()))
        totTime = (float(reqPropsObj.getExpectedCount()) * avgTimePerFile)
        reqPropsObj.setEstTotalTime(totTime)
        remainTime = (float(reqPropsObj.getExpectedCount() -
                            reqPropsObj.getActualCount()) * avgTimePerFile)
        reqPropsObj.setRemainingTime(remainTime)
        srvObj.updateRequestDb(reqPropsObj)
    return reqPropsObj


def getTmpDir(ngamsCfgObj):
    """
    Get the name of the NG/AMS Temporary Directory.

    ngamsCfgObj:    NG/AMS Configuration Object (ngamsConfig).

    Returns:
    """
    return os.path.normpath(ngamsCfgObj.getRootDirectory() + "/tmp")


def genCacheDirName(ngamsCfgObj):
    """
    Generate the NG/AMS Cache Directory name.

    ngamsCfgObj:    NG/AMS Configuration Object (ngamsConfig).

    Returns:
    """
    return os.path.normpath(ngamsCfgObj.getRootDirectory() + "/cache")


def getNgasTmpDir(ngamsCfgObj):
    """
    Return name of the NGAS Temporary Directory.

    ngamsCfgObj:    NG/AMS Configuration Object (ngamsConfig).

    Returns:        Name of NGAS Temporary Directory (string).
    """
    return os.path.normpath(ngamsCfgObj.getRootDirectory() + "/tmp/")


def getNgasChacheDir(ngamsCfgObj):
    """
    Return name of the NGAS Cache Directory.

    ngamsCfgObj:    NG/AMS Configuration Object (ngamsConfig).

    Returns:        Name of NGAS Cache Directory (string).
    """
    return os.path.normpath(ngamsCfgObj.getRootDirectory() + "/cache/")


def genTmpFilename(ngamsCfgObj,
                   filename):
    """
    Generate an NG/AMS  temporary filename. The final filename will be
    the filename indicated with a time stamp + a unique number prepended.

    ngamsCfgObj:    NG/AMS Configuration Object (ngamsConfig).

    filename:       Base filename (string).

    Returns:        Temporary filename (string).
    """
    return tempfile.mktemp(suffix=filename, dir=getNgasTmpDir(ngamsCfgObj))


def sendEmail(ngamsCfgObj,
              subject,
              recList,
              fromField,
              dataRef,
              contentType=None,
              attachmentName=None):
    """
    Send an e-mail to the recipients specified.

    ngamsCfgObj:    Reference to object containing NG/AMS
                    configuration file (ngamsConfig).

    smtpHost:       Mail server to use for sending the mail (string).

    subject:        Subject of mail message (string).

    recList:        List containing recipients, e.g. user@test.com (string).

    fromField:      Name for the from field (string).

    dataRef:        Message to send or reference to file containing data
                    to send (string).

    contentType:    Mime-type of message (string).

    attachmentName: Name of attachment in mail (string).

    dataInFile:     Used to make the sendmail tool send the data contained
                    in a file (integer).

    Returns:        Void.
    """

    if not attachmentName:
        msg = email.message.Message()
    else:
        msg = email.mime.multipart.MIMEMultipart()

    msg['From'] = fromField
    msg['Subject'] = subject

    if attachmentName:
        contentType = contentType or 'text/plain'
        ct_parts = contentType.split('/')
        part = email.mime.multipart.MIMEBase(ct_parts[0], ct_parts[1])
        with open(dataRef, 'rb') as f:
            part.set_payload(f.read())
        if ct_parts[0] != 'text':
            email.encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=attachmentName)
        msg.attach(part)
    else:
        if contentType:
            msg["Content-Type"] = contentType
        msg.set_payload(dataRef)

    smtpHost = ngamsCfgObj.getNotifSmtpHost()
    port = ngamsCfgObj.getNotifSmtpPort()
    client = smtplib.SMTP(smtpHost, port)
    for rcpt in recList:
        msg['To'] = rcpt
        client.sendmail(fromField, rcpt, msg.as_string())


# EOF
