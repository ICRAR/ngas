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
# "@(#) $Id: ngamsInitCmd.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  13/05/2003  Created
#

"""
Function + code to handle the INIT command.
"""

from ngams import *
import ngamsSrvUtils


def handleCmdInit(srvObj,
                  reqPropsObj,
                  httpRef):
    """
    Handle an INIT command.
        
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).
        
    Returns:        Void.   
    """
    srvObj.checkSetState("Command INIT", [NGAMS_ONLINE_STATE],
                         [NGAMS_IDLE_SUBSTATE])
    info(1,"Handling INIT command - re-initializing NG/AMS ...")
    ngamsSrvUtils.handleOffline(srvObj, reqPropsObj)
    ngamsSrvUtils.handleOnline(srvObj, reqPropsObj)
    info(1,"NG/AMS initialized!")
    srvObj.reply(reqPropsObj.setCompletionTime(), httpRef, NGAMS_HTTP_SUCCESS,
                 NGAMS_SUCCESS, "Successfully handled command INIT")
    srvObj.updateRequestDb(reqPropsObj)
    info(1,"Successfully handled command INIT!")


# EOF
