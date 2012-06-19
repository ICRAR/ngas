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
# "@(#) $Id: ngamsCacheDelCmd.py,v 1.2 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  31/07/2008  Created
#

"""
Contains code for handling the CACHEDEL Command.
"""

import pcc, PccUtTime
from ngams import *
import ngamsLib
import ngamsFileUtils, ngamsCacheControlThread


def cacheDel(srvObj,
             reqPropsObj,
             httpRef,
             diskId,
             fileId,
             fileVersion):
    """
    Schedule the file referenced for deletion from the NGAS Cache, or act
    as proxy and forward the CACHEDEL Command to the node concerned, or
    return an HTTP re-direction response.
    
    srvObj:       Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:  Request Property object to keep track of actions done
                  during the request handling (ngamsReqProps).

    httpRef:      Reference to the HTTP request handler
                  object (ngamsHttpRequestHandler).
                    
    diskId:       Disk ID of volume hosting the file (string).
 
    fileId:       File ID for file to consider (string).

    fileVersion:  Version of file (integer).

    Returns:      Void.
    """
    # Get the info for the file matching the query.
    fileLocInfo = ngamsFileUtils.locateArchiveFile(srvObj, fileId, fileVersion,
                                                   diskId)
    fileLocation  = fileLocInfo[0]
    fileHostId    = fileLocInfo[1]
    filePortNo    = fileLocInfo[3]
    if (fileLocation == NGAMS_HOST_LOCAL):
        msg = "Scheduling file for deletion from the cache according to " +\
              "CACHEDEL Command: %s/%s/%s"
        info(2, msg % (diskId, fileId, str(fileVersion)))
        sqlFileInfo = (diskId, fileId, fileVersion)
        ngamsCacheControlThread.scheduleFileForDeletion(srvObj, sqlFileInfo)
        srvObj.reply(reqPropsObj.setCompletionTime(), httpRef,
                     NGAMS_HTTP_SUCCESS, NGAMS_SUCCESS, 
                     "Handled CACHEDEL Command")
    elif (srvObj.getCfg().getProxyMode() or
          (fileLocInfo[0] == NGAMS_HOST_CLUSTER)):
        info(3, "File is remote or located within the private network of " +\
             "the contacted NGAS system -- this server acting as proxy " +\
             "and forwarding request to remote NGAS system: %s/%d" %\
             (fileHostId, filePortNo))
        httpStatCode, httpStatMsg, httpHdrs, data =\
                      srvObj.forwardRequest(reqPropsObj, httpRef,
                                            fileHostId, filePortNo)
    else:
        # Send back an HTTP re-direction response to the requestor.
        info(3, "File to be deleted from the NGAS Cache is stored on a " +\
             "remote host not within private network, Proxy Mode is off " +\
             "- sending back HTTP re-direction response")
        reqPropsObj.setCompletionTime(1)
        srvObj.updateRequestDb(reqPropsObj)
        srvObj.httpRedirReply(reqPropsObj, httpRef, fileHostId, filePortNo)

    srvObj.updateRequestDb(reqPropsObj)


def handleCmdCacheDel(srvObj,
                      reqPropsObj,
                      httpRef):
    """
    Handle CACHEDEL Command.
        
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).
        
    Returns:        Void.
    """
    T = TRACE()
    
    diskId = None
    fileId = None
    fileVersion = None
    for httpPar in reqPropsObj.getHttpParNames():
        if (httpPar == "disk_id"):
            diskId = reqPropsObj.getHttpPar("disk_id")
        elif (httpPar == "file_id"):
            fileId = reqPropsObj.getHttpPar("file_id")
        elif (httpPar == "file_version"):
            fileVersion = int(reqPropsObj.getHttpPar("file_version"))
        else:
            pass
    if ((not diskId) or (not fileId) or (not fileVersion)):
        msg = "Must specify disk_id/file_id/file_version for " +\
              "CACHEDEL Command"
        raise Exception, msg

    cacheDel(srvObj, reqPropsObj, httpRef, diskId, fileId, fileVersion)
     
# EOF
