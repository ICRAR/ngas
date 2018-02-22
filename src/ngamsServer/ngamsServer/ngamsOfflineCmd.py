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
# "@(#) $Id: ngamsOfflineCmd.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  13/05/2003  Created
#
"""
Function + code to handle the OFFLINE command.
"""

import logging

from ngamsLib.ngamsCore import \
    NGAMS_ONLINE_STATE, NGAMS_IDLE_SUBSTATE, NGAMS_OFFLINE_STATE, \
    NGAMS_BUSY_SUBSTATE
import ngamsSrvUtils


logger = logging.getLogger(__name__)

def handleCmd(srvObj,
                     reqPropsObj,
                     httpRef):
    """
    Handle an OFFLINE command.

    srvObj:       Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:  Request Property object to keep track of
                  actions done during the request handling
                  (ngamsReqProps).

    httpRef:      Reference to the HTTP request handler
                  object (ngamsHttpRequestHandler).

    Returns:      Void.
    """
    if (not reqPropsObj.hasHttpPar("force")):
        srvObj.checkSetState("Command OFFLINE", [NGAMS_ONLINE_STATE],
                             [NGAMS_IDLE_SUBSTATE], NGAMS_OFFLINE_STATE)
    else:
        srvObj.checkSetState("Command OFFLINE", [NGAMS_ONLINE_STATE],
                             [NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE],
                             NGAMS_OFFLINE_STATE)
    ngamsSrvUtils.handleOffline(srvObj, reqPropsObj)

    return "Successfully handled command OFFLINE"


# EOF
