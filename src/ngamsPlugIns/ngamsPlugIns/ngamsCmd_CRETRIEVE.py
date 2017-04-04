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
"""
Function + code to handle the CRETRIEVE Command.
"""

import functools
import logging
import os

from ngamsLib.ngamsCore import genLog, getFileSize
from ngamsLib.ngamsCore import NGAMS_CONT_MT, NGAMS_HTTP_SUCCESS
from ngamsLib.ngamsCore import NGAMS_HOST_LOCAL, NGAMS_HOST_REMOTE, NGAMS_HOST_CLUSTER
from ngamsLib.ngamsCore import NGAMS_RETRIEVE_CMD, NGAMS_ONLINE_STATE, NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE
from ngamsLib import ngamsLib, ngamsMIMEMultipart
from ngamsServer import ngamsSrvUtils, ngamsFileUtils


logger = logging.getLogger(__name__)

def fopener(fname):
    return functools.partial(open, fname, 'rb')

def http_opener(host, port, file_id, file_version, srvObj):
    pars = [('file_id', file_id), ('file_version', file_version)]
    authHdr = ngamsSrvUtils.genIntAuthHdr(srvObj)
    return ngamsLib.httpGet(host, port, NGAMS_RETRIEVE_CMD, pars=pars,
                            timeout=30, auth=authHdr)

def finfo_from_database(fileInfo, srvObj, reqPropsObj):

    fileId = fileInfo.getFileId()
    fileVer = fileInfo.getFileVersion()

    # Locate the file best suiting the query and send it back if possible.
    location, _, ipAddress, port, mountPoint, filename, fileId,\
              fileVersion, mimeType =\
              ngamsFileUtils.locateArchiveFile(srvObj, fileId,
                                               fileVersion=fileVer,
                                               reqPropsObj=reqPropsObj)

    basename = os.path.basename(filename)

    if location == NGAMS_HOST_LOCAL:
        absname = os.path.normpath(os.path.join(mountPoint, filename))
        size = getFileSize(absname)
        opener = fopener(absname)
    elif location == NGAMS_HOST_CLUSTER or location == NGAMS_HOST_REMOTE:
        size = srvObj.getDb().getFileSize(fileId, fileVersion)
        opener = http_opener(ipAddress, port, fileId, fileVersion, srvObj)
    else:
        raise Exception("Unknown location type: %s" % (location,))

    return ngamsMIMEMultipart.file_info(mimeType, basename, size, opener)

def cinfo_from_database(cont, srvObj, reqPropsObj):
    finfos = [cinfo_from_database(c, srvObj, reqPropsObj) for c in cont.getContainers()] + \
             [finfo_from_database(f, srvObj, reqPropsObj) for f in cont.getFilesInfo()]
    return ngamsMIMEMultipart.container_info(cont.getContainerName(), finfos)


def _handleCmdCRetrieve(srvObj,
                       reqPropsObj,
                       httpRef):
    """
    Carry out the action of a CRETRIEVE command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of
                    actions done during the request handling
                    (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """

    # For data files, retrieval must be enabled otherwise the request is
    # rejected.
    if (not srvObj.getCfg().getAllowRetrieveReq()):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Retrieve"])
        raise Exception(errMsg)

    # We don't allow processing yet
    if 'processing' in reqPropsObj:
        raise Exception('CRETRIEVE command does not allow processing (yet)')

    # At least container_id or container_name must be specified
    containerName = containerId = None
    if reqPropsObj.hasHttpPar("container_id") and reqPropsObj.getHttpPar("container_id").strip():
        containerId = reqPropsObj.getHttpPar("container_id").strip()
    if not containerId and reqPropsObj.hasHttpPar("container_name") and reqPropsObj.getHttpPar("container_name").strip():
        containerName = reqPropsObj.getHttpPar("container_name").strip()
    if not containerId and not containerName:
        errMsg = genLog("NGAMS_ER_RETRIEVE_CMD")
        raise Exception(errMsg)

    # If container_name is specified, and maps to more than one container,
    # an error is issued
    if not containerId:
        containerId = srvObj.getDb().getContainerIdForUniqueName(containerName)

    logger.debug("Handling request for file with containerId: %s", containerId)

    # Build the container hierarchy, get all file references and send back the results
    container = srvObj.getDb().readHierarchy(containerId, True)
    cinfo = cinfo_from_database(container, srvObj, reqPropsObj)
    reader = ngamsMIMEMultipart.ContainerReader(cinfo)

    # Send the headers and then the data
    srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                        dataRef=None, dataInFile=0, contentType=NGAMS_CONT_MT,
                        contentLength=len(reader))
    httpRef.end_headers()

    readf = functools.partial(reader.read, 65536)
    wfd = reqPropsObj.getWriteFd()
    for buf in iter(readf, ''):
        wfd.write(buf)
    reqPropsObj.setSentReply(1)


def handleCmd(srvObj, reqPropsObj, httpRef):
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

    srvObj.checkSetState("Command CRETRIEVE", [NGAMS_ONLINE_STATE],
                         [NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE],
                         "", NGAMS_BUSY_SUBSTATE)

    try:
        _handleCmdCRetrieve(srvObj, reqPropsObj, httpRef)
    finally:
        srvObj.setSubState(NGAMS_IDLE_SUBSTATE)


# EOF
