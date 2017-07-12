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
"""
Function + code to handle the RSYNC Command.
"""

import logging
import os
import subprocess

from ngamsLib.ngamsCore import NGAMS_IDLE_SUBSTATE, genLog
from ngamsServer import ngamsFileUtils


logger = logging.getLogger(__name__)

# does NOT currently support proxying / redirecting. Is not as flexible as the RETRIEVE command.
# the file must exist on the host to which this command was sent.
# for more flexibility it should be derived from ngamsRetrieveCmd - I don't need this right now
# for the ALMA mirroring

def transferFile(srvObj, fileLocation, targetHost, targetLocation):
    options = '-P --append --inplace -e "ssh -o StrictHostKeyChecking=no"'
    if srvObj.getCfg().getVal("Mirroring[1].rsync_options"):
        options = srvObj.getCfg().getVal("Mirroring[1].rsync_options")
    fetchCommand = "rsync " + options + " " + fileLocation + " ngas@" + targetHost + ":" + targetLocation
    logger.info("rsync command: %s", fetchCommand)
    process = subprocess.Popen(fetchCommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if stdout:
        logger.info('RSYNC stdout: %s', stdout)
    if stderr:
        logger.error("RSYNC: %s", stderr)
    result = process.wait()
    logger.info("finished command with status: %d", result)

def _handleCmd(srvObj, reqPropsObj, httpRef):
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

    # For data files, retrieval must be enabled otherwise the request is
    # rejected.
    if (not srvObj.getCfg().getAllowRetrieveReq()):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["RSYNC"])
        raise Exception(errMsg)

    # At least file_id must be specified if not an internal file has been
    # requested.
    if (not reqPropsObj.hasHttpPar("file_id") or reqPropsObj.getHttpPar("file_id").strip() == ""):
        errMsg = "required parameter file_id is missing"
        raise Exception(errMsg)
    fileId = reqPropsObj.getHttpPar("file_id")
    if (not reqPropsObj.hasHttpPar("targetHost") or reqPropsObj.getHttpPar("targetHost").strip() == ""):
        errMsg = "rquired parameter targetHost is missing"
        raise Exception(errMsg)
    targetHost = reqPropsObj.getHttpPar("targetHost")
    if (not reqPropsObj.hasHttpPar("targetLocation") or reqPropsObj.getHttpPar("targetLocation").strip() == ""):
        errMsg = "rquired parameter targetLocation is missing"
        raise Exception(errMsg)
    targetLocation = reqPropsObj.getHttpPar("targetLocation")
    logger.info("Handling request for file with ID: %s", fileId)
    fileVersion = -1
    if (reqPropsObj.hasHttpPar("file_version")):
        fileVersion = int(reqPropsObj.getHttpPar("file_version"))

    diskId = ""
    hostId = ""
    domain = ""

    # First try the quick retrieve attempt, just try to get the first
    # (and best?) suitable file which is online and located on a node in the
    # same domain as the contacted node.
    logger.info('trying a quick location of the file')
    location, host, ipAddress, port, mountPoint, filename,\
                  fileVersion, mimeType =\
                  ngamsFileUtils.quickFileLocate(srvObj, reqPropsObj, fileId,
                                                 hostId, domain, diskId,
                                                 fileVersion)

    # Get the file and send back the contents from this NGAS host.
    fileLocation = os.path.normpath(os.path.join(mountPoint, filename))

    logger.info('req props: %s', str(reqPropsObj))

    # Send back reply with the result(s) queried and possibly processed.
    transferFile(srvObj, fileLocation, targetHost, targetLocation)


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

    # If an internal file is retrieved we allow to handle the request also
    # when the system is Offline (for trouble-shooting purposes).
    logger.info('RSYNC command received')
    try:
        _handleCmd(srvObj, reqPropsObj, httpRef)
    finally:
        srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
