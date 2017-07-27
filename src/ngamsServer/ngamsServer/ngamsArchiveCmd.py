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
# "@(#) $Id: ngamsArchiveCmd.py,v 1.10 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  13/05/2003  Created
#

"""
Function to handle the ARCHIVE command.
"""

import logging

from ngamsLib.ngamsCore import TRACE, genLog, NGAMS_ONLINE_STATE,\
    NGAMS_BUSY_SUBSTATE, NGAMS_IDLE_SUBSTATE, getHostName, NGAMS_HTTP_SUCCESS,\
    NGAMS_SUCCESS, NGAMS_NOTIF_ERROR, NGAMS_HTTP_GET
from ngamsLib import ngamsHighLevelLib, ngamsNotification, ngamsDiskUtils
import ngamsArchiveUtils


logger = logging.getLogger(__name__)

def archiveInitHandling(srvObj,
                        reqPropsObj,
                        httpRef):
    """
    Handle the initialization of the ARCHIVE Command.

    For a description of the signature: Check handleCmdArchive().

    Returns:   Mime-type of the request or None if the request has been
               handled and reply sent back (string|None).
    """
    T = TRACE()

    # Is this NG/AMS permitted to handle Archive Requests?
    if (not srvObj.getCfg().getAllowArchiveReq()):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Archive"])
        raise Exception(errMsg)
    srvObj.checkSetState("Archive Request", [NGAMS_ONLINE_STATE],
                         [NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE],
                         NGAMS_ONLINE_STATE, NGAMS_BUSY_SUBSTATE,
                         updateDb=False)

    # Ensure we have the mime-type.
    if (reqPropsObj.getMimeType() == ""):
        mimeType = ngamsHighLevelLib.\
                   determineMimeType(srvObj.getCfg(), reqPropsObj.getFileUri())
        reqPropsObj.setMimeType(mimeType)
    else:
        mimeType = reqPropsObj.getMimeType()

    # This is a request probing for capability of handling the request.
    if (reqPropsObj.hasHttpPar("probe")):
        if (int(reqPropsObj.getHttpPar("probe"))):
            try:
                ngamsDiskUtils.findTargetDisk(srvObj.getHostId(),
                                              srvObj.getDb(), srvObj.getCfg(),
                                              mimeType, sendNotification=0)
                msg = genLog("NGAMS_INFO_ARCH_REQ_OK",
                             [mimeType, getHostName()])
            except:
                msg = genLog("NGAMS_ER_ARCH_REQ_NOK",
                             [mimeType, getHostName()])
            srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                         NGAMS_SUCCESS, msg)
            return None

    # Check if the URI is correctly set.
    if (reqPropsObj.getFileUri() == ""):
        errMsg = genLog("NGAMS_ER_MISSING_URI")
        raise Exception(errMsg)

    # Act possibly as proxy for the Achive Request?
    # TODO: Support maybe HTTP redirection also for Archive Requests.
    try:
        archUnits = srvObj.getCfg().getStreamFromMimeType(mimeType).\
                    getHostIdList()
    except Exception:
        archUnits = []

    if archUnits:
        host_id, host, port = ngamsArchiveUtils.findTargetNode(srvObj, mimeType)
        if host_id != srvObj.getHostId():
            srvObj.forwardRequest(reqPropsObj, httpRef, host_id, host, port, mimeType=mimeType)
            return None

    return mimeType


def handleCmdArchive(srvObj,
                     reqPropsObj,
                     httpRef):
    """
    Handle an ARCHIVE command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    T = TRACE()

    # Execute the init procedure for the ARCHIVE Command.
    mimeType = archiveInitHandling(srvObj, reqPropsObj, httpRef)
    if (not mimeType):
        # Set ourselves to IDLE; otherwise we'll stay in BUSY even though we
        # are doing nothing
        srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
        return

    # Is this an async request?
    async = 'async' in reqPropsObj and int(reqPropsObj['async'])

    # Handle Archive Request Locally.
    if (reqPropsObj.getHttpMethod() == NGAMS_HTTP_GET):
        logger.info("Handling Archive Pull Request. Async = %d ...", async)
        try:
            if (reqPropsObj.getFileUri() == ""):
                raise Exception("No File URI/Filename specified!")

            handle = ngamsHighLevelLib.openCheckUri(reqPropsObj.getFileUri())
            # urllib.urlopen will attempt to get the content-length based on the URI
            # i.e. file, ftp, http
            reqPropsObj.setSize(handle.info()['Content-Length'])
            reqPropsObj.setReadFd(handle)
            diskInfoObj = ngamsArchiveUtils.dataHandler(srvObj, reqPropsObj,
                                                        httpRef)
            srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
            msg = "Successfully handled Archive Pull Request for data file " +\
                  "with URI: " + reqPropsObj.getSafeFileUri()
            logger.info(msg)
            # If it is specified not to reply immediately (= to async), we
            # send back a reply now.
            if not async:
                srvObj.ingestReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                                   NGAMS_SUCCESS, msg, diskInfoObj)
        except Exception as e:
            errMsg = genLog("NGAMS_ER_ARCHIVE_PULL_REQ",
                            [reqPropsObj.getSafeFileUri(), str(e)])
            ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(), NGAMS_NOTIF_ERROR,
                                     "PROBLEM HANDLING ARCHIVE PULL REQUEST",
                                     errMsg)
            raise Exception(errMsg)
    else:
        logger.info("Handling Archive Push Request. Async = %d ...", async)
        try:
            diskinfoObj = ngamsArchiveUtils.dataHandler(srvObj, reqPropsObj,
                                                        httpRef)
            msg = "Successfully handled Archive Push Request for " +\
                  "data file with URI: " + reqPropsObj.getSafeFileUri()
            logger.info(msg)
            srvObj.setSubState(NGAMS_IDLE_SUBSTATE)

            # If it was specified not to reply immediately (= to wait),
            # we send back a reply now.
            if not async:
                srvObj.ingestReply(reqPropsObj, httpRef,NGAMS_HTTP_SUCCESS,
                                   NGAMS_SUCCESS, msg, diskinfoObj)
        except Exception as e:
            errMsg = genLog("NGAMS_ER_ARCHIVE_PUSH_REQ",
                            [reqPropsObj.getSafeFileUri(), str(e)])
            ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(), NGAMS_NOTIF_ERROR,
                                     "PROBLEM ARCHIVE HANDLING", errMsg)
            raise Exception(errMsg)

    # Trigger Subscription Thread.
    srvObj.triggerSubscriptionThread()


# EOF
