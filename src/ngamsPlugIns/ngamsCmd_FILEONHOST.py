#    ICRAR - International Centre for Radio Astronomy Research
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
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      2013/07/04  Created
#
"""
Check if a file does exist on this host (no forwarding, no proxy, no database checking)

"""

import os

from ngamsLib.ngamsCore import NGAMS_HTTP_SUCCESS, NGAMS_TEXT_MT


def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Find out which threads are still dangling
        
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).
        
    Returns:        Void.
    """
    errMsg = ''
    if (reqPropsObj.hasHttpPar("file_path")):
        filePath = reqPropsObj.getHttpPar('file_path')
        if (os.path.exists(filePath)):
            errMsg = 'YES'
            srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, errMsg, NGAMS_TEXT_MT)
        else:
            errMsg = 'NO'
            srvObj.httpReply(reqPropsObj, httpRef, 404, errMsg, NGAMS_TEXT_MT)
    else:
        errMsg = 'INVALID PARAMS'
        srvObj.httpReply(reqPropsObj, httpRef, 500, errMsg, NGAMS_TEXT_MT)
        