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
# "@(#) $Id: ngamsRetrieveCmd.py,v 1.12 2010/06/22 13:19:40 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  11/01/2002  Created
#
"""
Function + code to handle the RETRIEVE Command.
"""

import errno
import io
import os
import select
import shutil
import socket
import time

from ngamsLib import ngamsDppiStatus, ngamsStatus
from ngamsLib import ngamsHighLevelLib, ngamsLib
from ngamsLib.ngamsCore import info, warning, NGAMS_TEXT_MT, error, getFileSize, \
    TRACE, genLog, NGAMS_PROC_FILE, NGAMS_HTTP_SUCCESS, NGAMS_PROC_DATA, \
    NGAMS_HOST_LOCAL, \
    NGAMS_HOST_CLUSTER, NGAMS_HOST_REMOTE, checkCreatePath, NGAMS_RETRIEVE_CMD, \
    NGAMS_FAILURE, NGAMS_PROC_STREAM, NGAMS_ONLINE_STATE, NGAMS_IDLE_SUBSTATE, \
    NGAMS_BUSY_SUBSTATE, loadPlugInEntryPoint
import ngamsSrvUtils, ngamsFileUtils



################################################################################
# SENDFILE BEGINS
################################################################################
# Wrap the sendfile module into a sendfile method that is usable and that is
# aware of socket timeouts
# This code came originally from the creator of the sendfile python module,
# Giampaolo Rodola:
#
# http://grodola.blogspot.com/2014/06/python-and-sendfile.html

try:
    memoryview  # py 2.7 only
except NameError:
    memoryview = lambda x: x

if os.name == 'posix':
    import sendfile as pysendfile  # requires "pip install pysendfile"
else:
    pysendfile = None


_RETRY = frozenset((errno.EAGAIN, errno.EALREADY, errno.EWOULDBLOCK,
                    errno.EINPROGRESS))


class _GiveupOnSendfile(Exception):
    pass


if pysendfile is not None:

    def _sendfile_use_sendfile(sock, file, offset=0, count=None):
        _check_sendfile_params(sock, file, offset, count)
        sockno = sock.fileno()
        try:
            fileno = file.fileno()
        except (AttributeError, io.UnsupportedOperation) as err:
            raise _GiveupOnSendfile(err)  # not a regular file
        try:
            fsize = os.fstat(fileno).st_size
        except OSError:
            raise _GiveupOnSendfile(err)  # not a regular file
        if not fsize:
            return 0  # empty file
        blocksize = fsize if not count else count

        timeout = sock.gettimeout()
        if timeout == 0:
            raise ValueError("non-blocking sockets are not supported")
        # poll/select have the advantage of not requiring any
        # extra file descriptor, contrarily to epoll/kqueue
        # (also, they require a single syscall).
        if hasattr(select, 'poll'):
            if timeout is not None:
                timeout *= 1000
            pollster = select.poll()
            pollster.register(sockno, select.POLLOUT)

            def wait_for_fd():
                if pollster.poll(timeout) == []:
                    raise socket._socket.timeout('timed out')
        else:
            # call select() once in order to solicit ValueError in
            # case we run out of fds
            try:
                select.select([], [sockno], [], 0)
            except ValueError:
                raise _GiveupOnSendfile(err)

            def wait_for_fd():
                fds = select.select([], [sockno], [], timeout)
                if fds == ([], [], []):
                    raise socket._socket.timeout('timed out')

        total_sent = 0
        # localize variable access to minimize overhead
        os_sendfile = pysendfile.sendfile
        try:
            while True:
                if timeout:
                    wait_for_fd()
                if count:
                    blocksize = count - total_sent
                    if blocksize <= 0:
                        break
                try:
                    sent = os_sendfile(sockno, fileno, offset, blocksize)
                except OSError as err:
                    if err.errno in _RETRY:
                        # Block until the socket is ready to send some
                        # data; avoids hogging CPU resources.
                        wait_for_fd()
                    else:
                        if total_sent == 0:
                            # We can get here for different reasons, the main
                            # one being 'file' is not a regular mmap(2)-like
                            # file, in which case we'll fall back on using
                            # plain send().
                            raise _GiveupOnSendfile(err)
                        raise err
                else:
                    if sent == 0:
                        break  # EOF
                    offset += sent
                    total_sent += sent
            return total_sent
        finally:
            if total_sent > 0 and hasattr(file, 'seek'):
                file.seek(offset)
else:
    def _sendfile_use_sendfile(sock, file, offset=0, count=None):
        raise _GiveupOnSendfile(
            "sendfile() not available on this platform")


def _sendfile_use_send(sock, file, offset=0, count=None):
    _check_sendfile_params(sock, file, offset, count)
    if sock.gettimeout() == 0:
        raise ValueError("non-blocking sockets are not supported")
    if offset:
        file.seek(offset)
    blocksize = min(count, 8192) if count else 8192
    total_sent = 0
    # localize variable access to minimize overhead
    file_read = file.read
    sock_send = sock.send
    try:
        while True:
            if count:
                blocksize = min(count - total_sent, blocksize)
                if blocksize <= 0:
                    break
            data = memoryview(file_read(blocksize))
            if not data:
                break  # EOF
            while True:
                try:
                    sent = sock_send(data)
                except OSError as err:
                    if err.errno in _RETRY:
                        continue
                    raise
                else:
                    total_sent += sent
                    if sent < len(data):
                        data = data[sent:]
                    else:
                        break
        return total_sent
    finally:
        if total_sent > 0 and hasattr(file, 'seek'):
            file.seek(offset + total_sent)


def _check_sendfile_params(sock, file, offset, count):
    if 'b' not in getattr(file, 'mode', 'b'):
        raise ValueError("file should be opened in binary mode")
    if not sock.type & socket.SOCK_STREAM:
        raise ValueError("only SOCK_STREAM type sockets are supported")
    if count is not None:
        if not isinstance(count, int):
            raise TypeError(
                "count must be a positive integer (got %s)" % repr(count))
        if count <= 0:
            raise ValueError(
                "count must be a positive integer (got %s)" % repr(count))


def sendfile(sock, file, offset=0, count=None):
    """sendfile(sock, file[, offset[, count]]) -> sent

    Send a *file* over a connected socket *sock* until EOF is
    reached by using high-performance sendfile(2) and return the
    total number of bytes which were sent.
    *file* must be a regular file object opened in binary mode.
    If sendfile() is not available (e.g. Windows) or file is
    not a regular file socket.send() will be used instead.
    *offset* tells from where to start reading the file.
    If specified, *count* is the total number of bytes to transmit
    as opposed to sending the file until EOF is reached.
    File position is updated on return or also in case of error in
    which case file.tell() can be used to figure out the number of
    bytes which were sent.
    The socket must be of SOCK_STREAM type.
    Non-blocking sockets are not supported.
    """
    try:
        return _sendfile_use_sendfile(sock, file, offset, count)
    except _GiveupOnSendfile:
        return _sendfile_use_send(sock, file, offset, count)

################################################################################
# SENDFILE ENDS
################################################################################


def performStaging(srvObj, reqPropsObj, httpRef, filename):
    """
    if the staging plugin is set, then perform staging
    using the registered staging plugin
    if the file is offline (i.e. on Tape)

    srvObj:       Reference to NG/AMS server class object (ngamsServer).

    filename:     File to be processed (string).

    """
    if srvObj.getCfg().getFileStagingEnable() != 1:
        return

    fspi = srvObj.getCfg().getFileStagingPlugIn()
    if not fspi:
        return

    info(2, "Invoking FSPI.isFileOffline: %s to check file: %s" % (fspi, filename))
    isFileOffline = loadPlugInEntryPoint(fspi, 'isFileOffline')

    if isFileOffline(filename) == 0:
        return

    info(2, "Invoking FSPI.stageFiles: %s to check file: %s" % (fspi, filename))
    stageFiles = loadPlugInEntryPoint(fspi, 'stageFiles')

    try:
        st = time.time()
        stageFiles(filenameList = [filename],
                    requestObj = reqPropsObj,
                    serverObj = srvObj)
        howlong = time.time() - st
        fileSize = getFileSize(filename)
        info(3, 'Staging rate = %.0f Bytes/s (%.0f seconds) for file %s' % (fileSize / howlong, howlong, filename))

    except socket.timeout as t:
        errMsg = 'Staging timed out: %s' % filename
        warning(errMsg)
        srvObj.httpReply(reqPropsObj, httpRef, 504, errMsg, NGAMS_TEXT_MT)
        raise t



def performProcessing(srvObj,
                      reqPropsObj,
                      filename,
                      mimeType):
    """
    Carry out the processing requested.

    srvObj:       Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:  Request Property object to keep track of actions done
                  during the request handling (ngamsReqProps).

    filename:     File to be processed (string).

    mimeType:     Mime-type of file (string).

    Returns:      List with ngamsDppiStatus object
                  (list/ngamsDppiStatus objects).
    """
    T = TRACE()

    statusObjList = []

    # Carry out the processing specified. If no processing is
    # specified, we simply set the source file as the file to be send.
    if (reqPropsObj.hasHttpPar("processing")):
        dppi = reqPropsObj.getHttpPar("processing")
        # Before starting to process, check if the specified DPPI
        # is supported by this NG/AMS.
        if (not srvObj.getCfg().hasDppiDef(dppi)):
            errMsg = genLog("NGAMS_ER_ILL_DPPI", [dppi])
            raise Exception, errMsg
        # Invoke the DPPI.
        info(2, "Invoking DPPI: %s to process file: %s" % (dppi, filename))
        plugInMethod = loadPlugInEntryPoint(dppi)
        statusObj = plugInMethod(srvObj, reqPropsObj, filename)
    else:
        info(2, "No processing requested - sending back file as is")
        resultObj = ngamsDppiStatus.ngamsDppiResult(NGAMS_PROC_FILE, mimeType,
                                                    filename, filename)
        statusObj = ngamsDppiStatus.ngamsDppiStatus().addResult(resultObj)
    statusObjList.append(statusObj)

    return statusObjList


def cleanUpAfterProc(statusObjList):
    """
    Clean up after processing. I.e., remove the directories created for
    holding the files being processed.

    statusObjList:   List of status objects as returned by
                     ngamsCmdHandling.performProcessing()
                     (list/ngamsDppiStatus objects).

    Returns:         Void.
    """
    T = TRACE()

    for statObj in statusObjList:
        for resObj in statObj.getResultList():
            if (resObj.getProcDir() != ""):
                msg = ("Cleaning up processing directory: %s"
                      " after completed processing") % resObj.getProcDir()
                info(3, msg)
                shutil.rmtree(resObj.getProcDir())


def genReplyRetrieve(srvObj,
                     reqPropsObj,
                     httpRef,
                     statusObjList):
    """
    Function to send back a reply with the result queried with the
    RETRIEVE command. After having send back the result, the
    processing areas may be cleaned up.

    srvObj:          Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:     Request Property object to keep track of
                     actions done during the request handling
                     (ngamsReqProps).

    httpRef:         Reference to the HTTP request handler
                     object (ngamsHttpRequestHandler).

    statusObjList:   List of status objects as returned by
                     ngamsCmdHandling.performProcessing()
                     (list/ngamsDppiStatus objects).

    Returns:         Void.
    """
    T = TRACE()

    # Send back reply with the result queried.
    try:
        # TODO: Make possible to send back several results - use multipart
        # mime-type message -- for now only one result is sent back.
        resObj = statusObjList[0].getResultObject(0)
        #info(3, "Getting block size for retrieval")
        blockSize = srvObj.getCfg().getBlockSize()
        mimeType = resObj.getMimeType()
        dataSize = resObj.getDataSize()
        refFilename = resObj.getRefFilename()
        info(3, ("Sending data back to requestor. Reference filename: %s"
                 ". Size: %s") % (refFilename, str(dataSize)))
        srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, None, 0,
                            mimeType, dataSize)
        contDisp = "attachment; filename=\"%s\"" % refFilename
        info(4, "Sending header: Content-Disposition: %s" % contDisp)
        httpRef.send_header('Content-Disposition', contDisp)
        httpRef.wfile.write("\n")

        if reqPropsObj.hasHttpPar("send_buffer"):
            try:
                sendBufSize = int(reqPropsObj.getHttpPar("send_buffer"))
                httpRef.wfile._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF,sendBufSize)
            except Exception, ee:
                warning('Fail to reset the send_buffer size: %s' % str(ee))

        # Send back data from the memory buffer, from the result file, or
        # from HTTP socket connection.
        if resObj.getObjDataType() == NGAMS_PROC_DATA:
            info(3, "Sending data in buffer to requestor ...")
            httpRef.wfile.write(resObj.getDataRef())
        elif resObj.getObjDataType() == NGAMS_PROC_FILE:
            info(3, ("Reading data block-wise from file and sending "
                    "to requestor ..."))
            # use kernel zero-copy file send if available
            dataSent = 0
            dataref = resObj.getDataRef()
            with open(dataref, 'rb') as fd:
                st = time.time()
                sendfile(httpRef.wfile._sock, fd)
                howlong = time.time() - st
                info(3, "Retrieval transfer rate = %.0f Bytes/s for file %s" \
                        % (dataSent / howlong, refFilename))
        else:
            # NGAMS_PROC_STREAM - read the data from the File Object in
            # blocks and send it directly to the requestor.
            info(3,"Routing data from foreign location to requestor ...")
            dataSent = 0
            dataToSent = dataSize
            while (dataSent < dataToSent):
                tmpData = resObj.getDataRef().read(blockSize)
                httpRef.wfile.write(tmpData)
                dataSent += len(tmpData)

        info(4, "HTTP reply sent to: %s" % str(httpRef.client_address))
        reqPropsObj.setSentReply(1)

    finally:
        cleanUpAfterProc(statusObjList)


def _handleRemoteIntFile(srvObj,
                         reqPropsObj,
                         httpRef):
    """
    Retrieve the remote, internal file and send it back to the requestor.

    srvObj:        Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:   Request Property object to keep track of actions done
                   during the request handling (ngamsReqProps).

    httpRef:       Reference to the HTTP request handler object
                   (ngamsHttpRequestHandler).

    Returns:       Void.
    """
    T = TRACE()

    forwardHost = reqPropsObj.getHttpPar("host_id")
    forwardPort = srvObj.getDb().getPortNoFromHostId(forwardHost)
    httpStatCode, httpStatMsg, httpHdrs, data =\
                  srvObj.forwardRequest(reqPropsObj, httpRef, forwardHost,
                                        forwardPort, autoReply = 1)


def _handleCmdRetrieve(srvObj,
                       reqPropsObj,
                       httpRef):
    """
    Carry out the action of a RETRIEVE command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of
                    actions done during the request handling
                    (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    T = TRACE()
    # For data files, retrieval must be enabled otherwise the request is
    # rejected.
    if (not srvObj.getCfg().getAllowRetrieveReq()):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Retrieve"])
        error(errMsg)
        raise Exception, errMsg

    # Get query information.
    '''if (reqPropsObj.hasHttpPar("ng_log")):
        if (reqPropsObj.hasHttpPar("host_id")):
            if (reqPropsObj.getHttpPar("host_id") != srvObj.getHostId()):
                _handleRemoteIntFile(srvObj, reqPropsObj, httpRef)
                return

        # If there is a Local Log File, send it back.
        locLogFile = srvObj.getCfg().getLocalLogFile()
        if (os.path.exists(locLogFile)):
            mimeType = NGAMS_TEXT_MT
            srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                                locLogFile, 1, mimeType)
            return
        else:
            errMsg = genLog("NGAMS_ER_UNAVAIL_FILE", ["ng_log: " + locLogFile])
            error(errMsg)
            raise Exception, errMsg

    # This is a massive security risk and is not necessary
    elif (reqPropsObj.hasHttpPar("cfg")):
        if (reqPropsObj.hasHttpPar("host_id")):
            if (reqPropsObj.getHttpPar("host_id") != srvObj.getHostId()):
                _handleRemoteIntFile(srvObj, reqPropsObj, httpRef)
                return

        # Send back the file.
        srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                            srvObj.getCfg().getCfg(), 1, "text/xml")
        return
    elif (reqPropsObj.hasHttpPar("internal")):
        if (reqPropsObj.hasHttpPar("host_id")):
            if (reqPropsObj.getHttpPar("host_id") != srvObj.getHostId()):
                _handleRemoteIntFile(srvObj, reqPropsObj, httpRef)
                return

        # Handle internal (local) non-archive file or send back directory
        # contents info.
        intPath = reqPropsObj.getHttpPar("internal")
        if (intPath.strip() == ""):
            raise Exception, "Illegal path specified for RETRIEVE?internal"

        # If specified path is a directory, return contents of the directory.
        if (os.path.isdir(intPath) or (intPath == "/")):
            info(2,"Querying info about directory: %s" % intPath)
            comment = "Info about folder: %s" % intPath
            fileListObj = ngamsFileList.ngamsFileList("DIR-INFO", comment,
                                                      NGAMS_SUCCESS)
            if (intPath[-1] != "/"): intPath += "/"
            globFileList = glob.glob(os.path.normpath(intPath + "*"))

            # To get the permissions, owner, group, access and modification
            # time we use 'ls -l' for now.
            # TODO: PORTABILITY ISSUE: Avoid usage of UNIX commands.
            # dpallot: use os.walk functionality
            lsCmd = "ls -l %s" % intPath
            stat, lsBuf = commands.getstatusoutput(lsCmd)
            dirInfoList = lsBuf.split("\n")
            dirDic = {}
            for dirInfo in dirInfoList[1:]:
                dirInfo = dirInfo.strip()
                dirEls = cleanList(dirInfo.split(" "))
                if (len(dirEls) != 9): continue
                # Example:
                # -rw-r----- 1 ngas ngas 102 2007-03-30 13:10:38.000 +0200 XX
                # -rw-rw-r-- 1 ngas ngas 488 Oct 26     14:50              YY
                entryName = os.path.normpath(intPath + dirEls[8])
                dirDic[entryName] = dirEls

            # Unpack the information about each entry.
            for filename in globFileList:
                if (filename[:-1] == intPath): continue
                statInfo = os.stat(filename)
                tmpFileObj = ngamsFileInfo.ngamsFileInfo().\
                             setFilename(os.path.normpath(filename)).\
                             setPermissions(dirDic[filename][0]).\
                             setOwner(dirDic[filename][2]).\
                             setGroup(dirDic[filename][3]).\
                             setAccDateFromSecs(statInfo[7]).\
                             setModDateFromSecs(statInfo[8]).\
                             setCreationDate(statInfo[9]).\
                             setFileSize(statInfo[6])
                fileListObj.addFileInfoObj(tmpFileObj)
            statObj = srvObj.genStatus(NGAMS_SUCCESS, "Successfully handled " +
                                       "RETRIEVE Command").\
                                       addFileList(fileListObj)
            xmlStat = ngamsHighLevelLib.\
                      addDocTypeXmlDoc(srvObj, statObj.genXmlDoc(0, 0, 1),
                                       NGAMS_XML_STATUS_ROOT_EL,
                                       NGAMS_XML_STATUS_DTD)
            srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                                xmlStat, 0, NGAMS_XML_MT, len(xmlStat), [], 1)
            return

        # Check that it is not tried to retrieve a data file in this way.
        # This is done by checking if the file is located in one of the
        # storage areas. Certain files like NgasDiskInfo, DB Snapshot Files,
        # ect., can be retrieved.
        complFilename = ngamsLib.locateInternalFile(intPath)
        diskIdsMtPts = srvObj.getDb().getDiskIdsMtPtsMountedDisks(srvObj.getHostId())
        mountRtDir = srvObj.getCfg().getRootDirectory()
        for diskInfo in diskIdsMtPts:
            tmpDir   = os.path.normpath(diskInfo[1] + "/tmp/")
            cacheDir = os.path.normpath(diskInfo[1] + "/cache/")
            dbDir    = os.path.normpath(diskInfo[1] + "/.db/")
            if ((os.path.basename(complFilename) != NGAMS_DISK_INFO) and
                (os.path.basename(complFilename) != NGAMS_VOLUME_ID_FILE) and
                (os.path.basename(complFilename) != NGAMS_VOLUME_INFO_FILE) and
                (complFilename.find(tmpDir) == -1) and
                (complFilename.find(cacheDir) == -1) and
                (complFilename.find(dbDir) == -1) and
                (complFilename.find(diskInfo[1]) == 0)):
                errMsg = genLog("NGAMS_ER_ILL_RETRIEVE_REQ",
                                ["File requested appears to be an archived " +\
                                "data file. Retrieve these using the " +\
                                "RETRIEVE command + a combination of " +\
                                "File ID, File Version and Disk ID"])
                raise Exception, errMsg

        # OK, get the file and send it back.
        if ((complFilename.find(".xml") != -1) or
            (complFilename.find(".dtd") != -1) or
            (complFilename.find(NGAMS_DISK_INFO) != -1)):
            mimeType = "text/xml"
        elif (complFilename.find(".html") != -1):
            mimeType = "text/html"
        else:
            mimeType = ngamsHighLevelLib.\
                       determineMimeType(srvObj.getCfg(), complFilename, 1)
            if (mimeType == NGAMS_UNKNOWN_MT):
                # ".py", ...
                mimeType = NGAMS_TEXT_MT

        # Send back the file.
        srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                            complFilename, 1, mimeType)
        return'''

    # At least file_id must be specified if not an internal file has been
    # requested.
    issueRetCmdErr = 0
    if (not reqPropsObj.hasHttpPar("file_id")):
        issueRetCmdErr = 1
    else:
        if (reqPropsObj.getHttpPar("file_id").strip() == ""):
            issueRetCmdErr = 1
    if (issueRetCmdErr):
        errMsg = genLog("NGAMS_ER_RETRIEVE_CMD")
        error(errMsg)
        raise Exception, errMsg
    fileId = reqPropsObj.getHttpPar("file_id")
    info(3,"Handling request for file with ID: " + fileId)
    fileVer = -1
    if (reqPropsObj.hasHttpPar("file_version")):
        fileVer = int(reqPropsObj.getHttpPar("file_version"))
    diskId = ""
    if (reqPropsObj.hasHttpPar("disk_id")):
        diskId = reqPropsObj.getHttpPar("disk_id")
    hostId = ""
    if (reqPropsObj.hasHttpPar("host_id")):
        hostId = reqPropsObj.getHttpPar("host_id")
    domain = ""
    if (reqPropsObj.hasHttpPar("domain")):
        domain = reqPropsObj.getHttpPar("domain")
    quickLocation = False
    if (reqPropsObj.hasHttpPar("quick_location")):
        quickLocation = int(reqPropsObj.getHttpPar("quick_location"))

    # First try the quick retrieve attempt, just try to get the first
    # (and best?) suitable file which is online and located on a node in the
    # same domain as the contacted node.
    ipAddress = None
    if (quickLocation):
        location, host, ipAddress, port, mountPoint, filename,\
                  fileVersion, mimeType =\
                  ngamsFileUtils.quickFileLocate(srvObj, reqPropsObj, fileId,
                                                 hostId, domain, diskId,
                                                 fileVer)

    # If not located the quick way try the normal way.
    if (not ipAddress):
        # Locate the file best suiting the query and send it back if possible.
        location, host, ipAddress, port, mountPoint, filename, fileId,\
                  fileVersion, mimeType =\
                  ngamsFileUtils.locateArchiveFile(srvObj, fileId, fileVer,
                                                   diskId, hostId, reqPropsObj)

    # If still not located, try to contact associated NGAS sites to query
    # if the file is available there.
    # TODO:
    if (not ipAddress):
        pass

    if (location == NGAMS_HOST_LOCAL):
        # Get the file and send back the contents from this NGAS host.
        srcFilename = os.path.normpath("{0}/{1}".format(mountPoint, filename))

        # Perform the possible file staging
        performStaging(srvObj, reqPropsObj, httpRef, srcFilename)

        # Perform the possible processing requested.
        procResult = performProcessing(srvObj,reqPropsObj,srcFilename,mimeType)
    elif (((location == NGAMS_HOST_CLUSTER) or \
           (location == NGAMS_HOST_REMOTE)) and \
           srvObj.getCfg().getProxyMode()):

        info(3,"NG/AMS Server acting as proxy - requesting file with ID: " +\
             fileId + " from NG/AMS Server on host/port: " + host + "/" +\
             str(port) + " ...")

        # Act as proxy - get the file from the NGAS host specified and
        # send back the contents. The file is temporarily stored in the
        # Processing Area.
        procDir = ngamsHighLevelLib.genProcDirName(srvObj.getCfg())
        checkCreatePath(procDir)
        pars = []
        for par in reqPropsObj.getHttpParNames():
            if (par != "initiator"):
                pars.append([par, reqPropsObj.getHttpPar(par)])
        authHdr = ngamsSrvUtils.genIntAuthHdr(srvObj)
        httpStatCode, httpStatMsg, httpHdrs, data =\
                      ngamsLib.httpGet(ipAddress, port, NGAMS_RETRIEVE_CMD, 1,
                                       pars,"",srvObj.getCfg().getBlockSize(),
                                       timeOut = None, returnFileObj = 1,
                                       authHdrVal = authHdr)
        httpHdrDic = ngamsLib.httpMsgObj2Dic(httpHdrs)
        dataSize = int(httpHdrDic["content-length"])

        # Check that the Retrieve Request was successful.
        try:
            tmpStatObj = ngamsStatus.ngamsStatus().\
                         unpackXmlDoc(data, getStatus=1)
        except Exception, e:
            # Data was not a NG/AMS XML Status Document.
            tmpStatObj = None
            pass
        if (tmpStatObj):
            if (tmpStatObj.getStatus() == NGAMS_FAILURE):
                raise Exception, tmpStatObj.getMessage()

        tmpPars = ngamsLib.parseHttpHdr(httpHdrDic["content-disposition"])
        dataFilename = tmpPars["filename"]

        # Generate fake ngamsDppiStatus object.
        resultObj = ngamsDppiStatus.ngamsDppiResult(NGAMS_PROC_STREAM,
                                                    mimeType, data,
                                                    dataFilename, procDir,
                                                    dataSize)
        procResult = [ngamsDppiStatus.ngamsDppiStatus().addResult(resultObj)]
    else:
        # No proxy mode: A redirection HTTP response is generated.
        srvObj.httpRedirReply(reqPropsObj, httpRef, ipAddress, port)
        return

    # Send back reply with the result(s) queried and possibly processed.
    genReplyRetrieve(srvObj, reqPropsObj, httpRef, procResult)


def handleCmdRetrieve(srvObj,
                      reqPropsObj,
                      httpRef):
    """
    Handle a RETRIEVE command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of
                    actions done during the request handling
                    (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    T = TRACE()

    # If an internal file is retrieved we allow to handle the request also
    # when the system is Offline (for trouble-shooting purposes).
    if ((not reqPropsObj.hasHttpPar("internal")) and
        (not reqPropsObj.hasHttpPar("ng_log")) and
        (not reqPropsObj.hasHttpPar("cfg"))):
        srvObj.checkSetState("Command RETRIEVE", [NGAMS_ONLINE_STATE],
                             [NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE],
                             "", NGAMS_BUSY_SUBSTATE)

    # Check if processing is requested if this systems allows processing.
    if (reqPropsObj.hasHttpPar("processing") and \
        (not srvObj.getCfg().getAllowProcessingReq())):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Retrieve+Processing"])
        error(errMsg)
        srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
        raise Exception, errMsg

    _handleCmdRetrieve(srvObj, reqPropsObj, httpRef)
    srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
