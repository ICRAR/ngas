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


from ngams import *
import ngamsStatus, ngamsHighLevelLib, ngamsNotification, ngamsDiskUtils
import ngamsArchiveUtils, ngamsCacheControlThread


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
        raise Exception, errMsg
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
                ngamsDiskUtils.findTargetDisk(srvObj.getDb(), srvObj.getCfg(),
                                              mimeType, sendNotification=0)
                msg = genLog("NGAMS_INFO_ARCH_REQ_OK",
                             [mimeType, getHostName()])
            except Exception, e:
                msg = genLog("NGAMS_ER_ARCH_REQ_NOK",
                             [mimeType, getHostName()])
            srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                         NGAMS_SUCCESS, msg)
            return None

    # Check if the URI is correctly set.
    if (reqPropsObj.getFileUri() == ""):
        errMsg = genLog("NGAMS_ER_MISSING_URI")
        error(errMsg)
        raise Exception, errMsg

    # Act possibly as proxy for the Achive Request?
    try:
        archUnits = srvObj.getCfg().getStreamFromMimeType(mimeType).\
                    getHostIdList()
    except Exception, e:
        archUnits = []
    if (len(archUnits) > 0):
        targNodeName, targNode, targPort, targDiskObj =\
                      ngamsArchiveUtils.\
                      findTargetNode(srvObj.getDb(), srvObj.getCfg(),
                                     reqPropsObj.getMimeType())
        if ((targNodeName != getHostName()) or
            (int(targPort) != int(srvObj.getCfg().getPortNo()))):
            # Act as proxy, forward the request to the specified node.
            # TODO: Support maybe HTTP redirection also for Archive Requests.
            httpStatCode, httpStatMsg, httpHdrs, data =\
                          srvObj.forwardRequest(reqPropsObj, httpRef,
                                                targNodeName, targPort, 1,
                                                mimeType)
            # Request handled at remote host and reply sent to client.
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
    if (not mimeType): return

    # Handle Archive Request Locally.
    if (reqPropsObj.getHttpMethod() == NGAMS_HTTP_GET):
        info(1,"Handling Archive Pull Request ...")
        try:
            if (reqPropsObj.getFileUri() == ""):
                raise Exception, "No File URI/Filename specified!"
            reqPropsObj.setReadFd(ngamsHighLevelLib.\
                                  openCheckUri(reqPropsObj.getFileUri()))
            diskInfoObj = ngamsArchiveUtils.dataHandler(srvObj, reqPropsObj,
                                                        httpRef)
            srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
            msg = "Successfully handled Archive Pull Request for data file " +\
                  "with URI: " + reqPropsObj.getSafeFileUri()
            info(1,msg)
            # If it is specified not to reply immediately (= to wait), we
            # send back a reply now.
            if (reqPropsObj.getWait()):
                srvObj.ingestReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                                   NGAMS_SUCCESS, msg, diskInfoObj)
        except Exception, e:
            errMsg = genLog("NGAMS_ER_ARCHIVE_PULL_REQ",
                            [reqPropsObj.getSafeFileUri(), str(e)])
            error(errMsg)
            ngamsNotification.notify(srvObj.getCfg(), NGAMS_NOTIF_ERROR,
                                     "PROBLEM HANDLING ARCHIVE PULL REQUEST",
                                     errMsg)
            raise Exception, errMsg
    else:
        info(1,"Handling Archive Push Request ...")
        try:
            diskinfoObj = ngamsArchiveUtils.dataHandler(srvObj, reqPropsObj,
                                                        httpRef)
            msg = "Successfully handled Archive Push Request for " +\
                  "data file with URI: " + reqPropsObj.getSafeFileUri()
            info(4, msg)
            srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
            
            # If it was specified not to reply immediately (= to wait),
            # we send back a reply now.
            if (reqPropsObj.getWait()):
                srvObj.ingestReply(reqPropsObj, httpRef,NGAMS_HTTP_SUCCESS,
                                   NGAMS_SUCCESS, msg, diskinfoObj)
        except Exception, e:
            errMsg = genLog("NGAMS_ER_ARCHIVE_PUSH_REQ",
                            [reqPropsObj.getSafeFileUri(), str(e)])
            error(errMsg)
            ngamsNotification.notify(srvObj.getCfg(), NGAMS_NOTIF_ERROR,
                                     "PROBLEM ARCHIVE HANDLING", errMsg)
            raise Exception, errMsg

    # Trigger Subscription Thread.
    srvObj.triggerSubscriptionThread()


# EOF
