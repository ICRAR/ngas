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
import logging
import os
import select
import shutil
import socket
import time

from ngamsLib import ngamsDppiStatus, ngamsHttpUtils
from ngamsLib import ngamsHighLevelLib, ngamsLib
from ngamsLib.ngamsCore import NGAMS_TEXT_MT, getFileSize, \
    TRACE, genLog, NGAMS_PROC_FILE, NGAMS_HTTP_SUCCESS, NGAMS_PROC_DATA, \
    NGAMS_HOST_LOCAL, \
    NGAMS_HOST_CLUSTER, NGAMS_HOST_REMOTE, checkCreatePath, NGAMS_RETRIEVE_CMD, \
    NGAMS_PROC_STREAM, NGAMS_ONLINE_STATE, NGAMS_IDLE_SUBSTATE, \
    NGAMS_BUSY_SUBSTATE, loadPlugInEntryPoint
import ngamsSrvUtils, ngamsFileUtils



logger = logging.getLogger(__name__)

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

    logger.info("Invoking FSPI.isFileOffline: %s to check file: %s", fspi, filename)
    isFileOffline = loadPlugInEntryPoint(fspi, 'isFileOffline')

    if isFileOffline(filename) == 0:
        return

    logger.info("Invoking FSPI.stageFiles: %s to check file: %s", fspi, filename)
    stageFiles = loadPlugInEntryPoint(fspi, 'stageFiles')

    try:
        st = time.time()
        stageFiles(filenames = [filename],
                    requestObj = reqPropsObj,
                    serverObj = srvObj)
        howlong = time.time() - st
        fileSize = getFileSize(filename)
        logger.debug('Staging rate = %.0f Bytes/s (%.0f seconds) for file %s', fileSize / howlong, howlong, filename)

    except socket.timeout:
        errMsg = 'Staging timed out: %s' % filename
        logger.warning(errMsg)
        srvObj.httpReply(reqPropsObj, httpRef, 504, errMsg, NGAMS_TEXT_MT)
        raise



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
        if dppi not in srvObj.getCfg().dppi_plugins:
            errMsg = genLog("NGAMS_ER_ILL_DPPI", [dppi])
            raise Exception, errMsg
        # Invoke the DPPI.
        logger.info("Invoking DPPI: %s to process file: %s", dppi, filename)
        plugInMethod = loadPlugInEntryPoint(dppi)
        statusObj = plugInMethod(srvObj, reqPropsObj, filename)
    else:
        logger.info("No processing requested - sending back file as is")
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
                      " after completed processing")
                logger.debug(msg, resObj.getProcDir())
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

        # See if client requested partial content
        # This applies (currently) to files only
        start_byte = 0
        if reqPropsObj.retrieve_offset > 0 and resObj.getObjDataType() == NGAMS_PROC_FILE:
            start_byte = reqPropsObj.retrieve_offset

        logger.info("Sending data back to requestor. Reference filename: %s. Size: %d. Starting byte: %d",
                     refFilename, dataSize, start_byte)
        srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, None, 0,
                            mimeType, (dataSize - start_byte))
        contDisp = "attachment; filename=\"%s\"" % refFilename
        logger.debug("Sending header: Content-Disposition: %s", contDisp)
        httpRef.send_header('Content-Disposition', contDisp)
        if start_byte:
            httpRef.send_header('Accept-Ranges', 'bytes')
            httpRef.send_header("Content-Range", "bytes %d-%d/%d" % (start_byte, dataSize - 1, dataSize))
        httpRef.wfile.write("\n")

        if reqPropsObj.hasHttpPar("send_buffer"):
            try:
                sendBufSize = int(reqPropsObj.getHttpPar("send_buffer"))
                httpRef.wfile._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF,sendBufSize)
            except Exception, ee:
                logger.warning('Fail to reset the send_buffer size: %s', str(ee))

        # Send back data from the memory buffer, from the result file, or
        # from HTTP socket connection.
        if resObj.getObjDataType() == NGAMS_PROC_DATA:
            logger.debug("Sending data in buffer to requestor ...")
            httpRef.wfile.write(resObj.getDataRef())
        elif resObj.getObjDataType() == NGAMS_PROC_FILE:
            logger.debug("Reading data block-wise from file and sending to requestor ...")
            # use kernel zero-copy file send if available
            dataref = resObj.getDataRef()
            with open(dataref, 'rb') as fd:
                st = time.time()
                sendfile(httpRef.wfile._sock, fd, start_byte)
                howlong = time.time() - st
                logger.debug("Retrieval transfer rate = %.0f Bytes/s for file %s",
                             dataSize / howlong, refFilename)
        else:
            # NGAMS_PROC_STREAM - read the data from the File Object in
            # blocks and send it directly to the requestor.
            logger.debug("Routing data from foreign location to requestor ...")
            dataSent = 0
            dataToSent = dataSize
            while (dataSent < dataToSent):
                tmpData = resObj.getDataRef().read(blockSize)
                httpRef.wfile.write(tmpData)
                dataSent += len(tmpData)

        logger.debug("HTTP reply sent to: %s", str(httpRef.client_address))
        reqPropsObj.setSentReply(1)

    finally:
        cleanUpAfterProc(statusObjList)


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
        raise Exception(errMsg)

    # Previously this command allowed to retrieve the current logging file,
    # the configuration file and any internal file. We don't do this anymore
    # Get query information.
    if 'ng_log' in reqPropsObj or 'cfg' in reqPropsObj or 'internal' in reqPropsObj:
        raise Exception("ng_log, cfg and internal parameters not supported anymore")

    # At least file_id must be specified if not an internal file has been
    # requested.
    if 'file_id' not in reqPropsObj or not reqPropsObj['file_id']:
        errMsg = genLog("NGAMS_ER_RETRIEVE_CMD")
        raise Exception(errMsg)
    fileId = reqPropsObj.getHttpPar("file_id")
    logger.debug("Handling request for file with ID: %s", fileId)
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
    elif location in (NGAMS_HOST_CLUSTER, NGAMS_HOST_REMOTE) and \
         srvObj.getCfg().getProxyMode():

        logger.debug("NG/AMS Server acting as proxy - requesting file with ID: %s " +\
                     "from NG/AMS Server on host/port: %s/%s",
                     fileId, host, str(port))

        # Act as proxy - get the file from the NGAS host specified and
        # send back the contents. The file is temporarily stored in the
        # Processing Area.
        procDir = ngamsHighLevelLib.genProcDirName(srvObj.getCfg())
        checkCreatePath(procDir)
        pars = []
        for par in reqPropsObj.getHttpParNames():
            pars.append([par, reqPropsObj.getHttpPar(par)])

        authHdr = ngamsSrvUtils.genIntAuthHdr(srvObj)
        timeout = float(reqPropsObj['timeout']) if 'timeout' in reqPropsObj else 60
        conn = ngamsHttpUtils.httpGet(ipAddress, port, NGAMS_RETRIEVE_CMD, pars=pars,
                                timeout=timeout, auth=authHdr)

        hdrs = {h[0]: h[1] for h in conn.getheaders()}
        dataSize = int(hdrs["content-length"])

        tmpPars = ngamsLib.parseHttpHdr(hdrs["content-disposition"])
        dataFilename = tmpPars["filename"]

        # Generate fake ngamsDppiStatus object.
        resultObj = ngamsDppiStatus.ngamsDppiResult(NGAMS_PROC_STREAM,
                                                    mimeType, conn,
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

    srvObj.checkSetState("Command RETRIEVE", [NGAMS_ONLINE_STATE],
                         [NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE],
                         "", NGAMS_BUSY_SUBSTATE)

    # Check if processing is requested if this systems allows processing.
    if (reqPropsObj.hasHttpPar("processing") and \
        (not srvObj.getCfg().getAllowProcessingReq())):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Retrieve+Processing"])
        srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
        raise Exception(errMsg)

    # See if client requested partial content and remember the starting offset
    retrieve_offset = 0
    range_hdr = reqPropsObj.getHttpHdr('range')
    if range_hdr:
        try:
            retrieve_offset = int(range_hdr[6:-1])
            if retrieve_offset < 0:
                raise
        except:
            raise ValueError("Invalid Range header, must have the form 'bytes=start-' (start offset only)")
    reqPropsObj.retrieve_offset = retrieve_offset

    _handleCmdRetrieve(srvObj, reqPropsObj, httpRef)
    srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
