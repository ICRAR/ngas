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
# "@(#) $Id: ngamsSubscribeCmd.py,v 1.5 2009/11/26 12:23:42 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  05/11/2002  Created
#
"""
This module contains functions used in connection with the SUBSCRIBE Command.
"""

import logging
import time

import ngamsSubscriptionThread
from ngamsLib.ngamsCore import fromiso8601, toiso8601
from ngamsLib import ngamsSubscriber, ngamsLib


logger = logging.getLogger(__name__)

def handleCmd(srv, reqPropsObj, httpRef):
    """
    Handle SUBSCRIBE Command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """

    # URL is mandatory, and has some requirements
    if 'url' not in reqPropsObj:
        raise Exception('Missing parameter: url')
    url = reqPropsObj['url']
    ngamsSubscriber.validate_url(url)

    # Concurrent threads must be >= 1
    concur_thrds  = 1
    if 'concurrent_threads' in reqPropsObj:
        concur_thrds = int(reqPropsObj['concurrent_threads'])
    if concur_thrds < 1:
        raise Exception('Number of concurrent threads is < 1')

    # Priority must be an integer
    priority = 10
    if 'priority' in reqPropsObj:
        priority = int(reqPropsObj['priority'])

    # start_date must be in ISO 8601 format
    startDate = time.time()
    if 'start_date' in reqPropsObj:
        tmpStartDate = reqPropsObj.getHttpPar("start_date").strip()
        if tmpStartDate:
            startDate = fromiso8601(tmpStartDate, local=True)

    # Filter Plug-in + pars
    filterPi      = None
    filterPiPars  = None
    if 'filter_plug_in' in reqPropsObj:
        filterPi = reqPropsObj['filter_plug_in']
    if 'plug_in_pars' in reqPropsObj:
        filterPiPars = reqPropsObj['plug_in_pars']

    # Subscriber ID. If not given, we use the URL
    if 'subscr_id' in reqPropsObj:
        subscr_id = reqPropsObj['subscr_id']
    else:
        subscr_id = ngamsLib.getSubscriberId(url)

    logger.info("Creating subscription for files >= %s", toiso8601(startDate))
    args = (subscr_id, srv.getHostId(), srv.portNo, priority,
            url, startDate, concur_thrds, True, filterPi, filterPiPars)
    subscriber = ngamsSubscriber.ngamsSubscriber(*args)

    # Register the new subscriber and its deliveries
    srv.db.insertSubscriberEntry(subscriber)
    ngamsSubscriptionThread.add_deliveries_for_new_subscriber(srv, subscriber)

    # Trigger the Data Susbcription Thread to see the effects of this addition
    # soon
    srv.triggerSubscriptionThread()

    return "Handled SUBSCRIBE command"