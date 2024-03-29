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

from ngamsLib.ngamsCore import \
    genLog, NGAMS_SUBSCRIBE_CMD, fromiso8601, toiso8601, NGAMS_FAILURE,\
    NGAMS_SUCCESS
from ngamsLib import ngamsSubscriber, ngamsLib


logger = logging.getLogger(__name__)

def addSubscriber(srvObj, subscrObj):
    """
    Add a Subscriber to the list of Subscribers. The information about
    the Subscriber is also updated in the DB.

    srvObj:      Reference to NG/AMS Server object (ngamsServer).

    subscrObj:   Subscriber Object (ngamsSubscriber).

    Returns:     Void.
    """
    subscr_in_db = srvObj.getDb().insertSubscriberEntry(subscrObj)
    if subscr_in_db is subscrObj:
        srvObj.registerSubscriber(subscrObj)
        # Trigger the Data Susbcription Thread to make it check if there are
        # files to deliver to the new Subscriber.
        srvObj.addSubscriptionInfo([], [subscrObj]).triggerSubscriptionThread()
    elif subscr_in_db == subscrObj:
        return 'equal'
    else:
        return 'unequal'


def handleCmd(srvObj,
                       reqPropsObj,
                       httpRef):
    """
    Handle SUBSCRIBE Command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    """
    if (srvObj.getDataMoverOnlyActive() and len(srvObj.getSubscriberDic()) > 0):
        srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE,
                 "Data Mover NGAS server can have only one subscriber. Use command USUBSCRIBE to update an existing subscriber.")
        return
    """
    priority      = 10
    url           = ""
    startDate     = time.time()
    filterPi      = ""
    filterPiPars  = ""
    if (reqPropsObj.hasHttpPar("priority")):
        priority = reqPropsObj.getHttpPar("priority")
    if (reqPropsObj.hasHttpPar("url")):
        url = reqPropsObj.getHttpPar("url")
        ngamsSubscriber.validate_url(url)
    else:
        errMsg = genLog("NGAMS_ER_CMD_SYNTAX",
                        [NGAMS_SUBSCRIBE_CMD, "Missing parameter: url"])
        raise Exception(errMsg)
    if (reqPropsObj.hasHttpPar("start_date")):
        tmpStartDate = reqPropsObj.getHttpPar("start_date").strip()
        if tmpStartDate:
            startDate = fromiso8601(tmpStartDate, local=True)
    if (reqPropsObj.hasHttpPar("filter_plug_in")):
        filterPi = reqPropsObj.getHttpPar("filter_plug_in")
    if (reqPropsObj.hasHttpPar("plug_in_pars")):
        filterPiPars = reqPropsObj.getHttpPar("plug_in_pars")
    if (reqPropsObj.hasHttpPar("subscr_id")):
        id = reqPropsObj.getHttpPar("subscr_id")
    else:
        id = ngamsLib.getSubscriberId(url)

    logger.info("Creating subscription for files >= %s", toiso8601(startDate))
    subscrObj = ngamsSubscriber.ngamsSubscriber(srvObj.getHostId(),
                                                srvObj.getCfg().getPortNo(),
                                                priority, url, startDate,
                                                filterPi, filterPiPars, subscrId=id)
    # supports concurrent file transfer, added by chen.wu@icrar.org
    if (reqPropsObj.hasHttpPar("concurrent_threads")):
        concurthrds = reqPropsObj.getHttpPar("concurrent_threads")
        subscrObj.setConcurrentThreads(concurthrds)

    # If the Start Date given in before the Last Ingestion Date, we
    # reset the Last Ingestion Date
    #
    # TODO (rtobar, May 2021):
    #     This bit of logic seems to be in contention with the fact that adding
    #     a subscription with an ID that is already taken would be otherwise a
    #     failure. I'm leaving it here for the time being in order to avoid
    #     breaking anything, but it must probably be removed (or clarification
    #     should be sought otherwise).
    subscrStat = srvObj.getDb().getSubscriberStatus([subscrObj.getId()],
                                                    subscrObj.getHostId(),
                                                    subscrObj.getPortNo())
    if subscrStat and subscrStat[0][1]:
        lastIngDate = fromiso8601(subscrStat[0][1], local=True)
        if startDate < lastIngDate:
            subscrObj.setLastFileIngDate('')
        else:
            subscrObj.setLastFileIngDate(lastIngDate)

    # Register the Subscriber.
    existence_test = addSubscriber(srvObj, subscrObj)
    if existence_test == 'equal':
        return 201, NGAMS_SUCCESS, "Identical subscription with ID '%s' existed" % (id,)
    elif existence_test == 'unequal':
        return 409, NGAMS_FAILURE, "Different subscription with ID '%s' existed" % (id,)

    return "Handled SUBSCRIBE command"
