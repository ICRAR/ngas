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
# "@(#) $Id: ngamsUnsubscribeCmd.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  05/11/2002  Created
#

"""
This module contains functions used in connection with the
UNSUBSCRIBE Command.
"""

import pcc, PccUtTime
from ngams import *
import ngamsLib, ngamsSubscriber


def delSubscriber(srvObj,
                  subscrId):
    """
    Remove a Susbcriber from the internal list + from the DB.

    srvObj:      Reference to NG/AMS Server object (ngamsServer).

    subscrId:    Subscriber ID (string).

    Returns:     Void.
    """
    T = TRACE()
    
    err = 0
    try:
        srvObj.getDb().deleteSubscriber(subscrId)
    except Exception, e:
        warning("Error deleting Subscriber information from the DB. " +\
                "Subscriber ID: " + subscrId + ". Exception: " + str(e))
        err = 1
    try:
        del srvObj.getSubscriberDic()[subscrId]
    except Exception, e:
        warning("Error deleting Subscriber information kept internally. " +\
                "Subscriber ID: " + subscrId + ". Exception: " + str(e))
        err = 1
    if (not err):
        info(2,"Subscriber with ID: " + subscrId +\
             " successfully unsubscribed")


def handleCmdUnsubscribe(srvObj,
                         reqPropsObj,
                         httpRef):
    """
    Handle UNSUBSCRIBE command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    T = TRACE()
    
    if (reqPropsObj.hasHttpPar("url")):
        url = reqPropsObj.getHttpPar("url")
    else:
        errMsg = genLog("NGAMS_ER_CMD_SYNTAX",
                        [NGAMS_SUBSCRIBE_CMD, "Missing parameter: url"])
        raise Exception, errMsg
    delSubscriber(srvObj, ngamsLib.getSubscriberId(url))

    srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_SUCCESS,
                 "Handled UNSUBSCRIBE command")


# EOF
