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
# "@(#) $Id: ngamsUnsubscribeCmd.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  05/11/2002  Created
#

"""
This module implements the UNSUBSCRIBE command.
"""

import logging

from ngamsLib import ngamsLib


logger = logging.getLogger(__name__)

def handleCmd(srv, reqPropsObj, httpRef):
    """
    Handle UNSUBSCRIBE command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """

    # added by chen.wu@icrar.org
    if 'subscr_id' in reqPropsObj:
        subscriber_id = reqPropsObj["subscr_id"]
    elif 'url' in reqPropsObj:
        url = reqPropsObj["url"]
        subscriber_id = ngamsLib.getSubscriberId(url)
    else:
        raise Exception('Neither subscr_id nor url given')

    # Simply delete the subscriber and its deliveries from the database
    # If there are deliveries occuring at the moment for this subscriber
    # those will go through; only during the next subscription major cycle
    # these changes will be visible.
    srv.db.delete_subscriber_and_deliveires(subscriber_id)
