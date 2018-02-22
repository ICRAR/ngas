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

import logging
import os
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
import ngamsSrvUtils, ngamsFileUtils, pysendfile


logger = logging.getLogger(__name__)

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
        httpRef.send_data(errMsg, NGAMS_TEXT_MT, code=504)
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
            raise Exception(errMsg)
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

    # Send back reply with the result queried.
    try:

        resObj = statusObjList[0].getResultObject(0)

        if resObj.getObjDataType() == NGAMS_PROC_FILE:
            # See if client requested partial content
            # This applies (currently) to files only
            start_byte = 0
            if reqPropsObj.retrieve_offset > 0:
                start_byte = reqPropsObj.retrieve_offset

            httpRef.send_file(resObj.getDataRef(), resObj.getMimeType(),
                              start_byte=start_byte, fname=resObj.getRefFilename())
        else:
            httpRef.send_data(resObj.getDataRef(), resObj.getMimeType(), fname=resObj.getRefFilename())

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
    quickLocation = True
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

        data = ngamsHttpUtils.sizeaware(conn, dataSize)
        httpRef.send_data(data, mimeType, fname=dataFilename)
        return

    else:
        # No proxy mode: A redirection HTTP response is generated.
        httpRef.redirect(ipAddress, port)
        return

    # Send back reply with the result(s) queried and possibly processed.
    genReplyRetrieve(srvObj, reqPropsObj, httpRef, procResult)


def handleCmd(srvObj,
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
