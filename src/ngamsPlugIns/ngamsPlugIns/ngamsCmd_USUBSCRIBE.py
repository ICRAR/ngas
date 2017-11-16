#
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
# Who       When        What
# --------  ----------  -------------------------------------------------------
# chen.wu@icrar.org   14-Mar-2012    created
"""
this command updates an existing subscriber's information
including priority, url, start_date, and num_concurrent_threads
"""

import logging

from ngamsLib import ngamsSubscriber


logger = logging.getLogger(__name__)

def handleCmd(srv, reqPropsObj, httpRef):
    """
    Handle the update subscriber (USUBSCRIBE) Command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """

    if 'subscr_id' not in reqPropsObj:
        raise Exception('USUBSCRIBE command failed: subscr_id is not specified')

    subscrId = reqPropsObj["subscr_id"]

    # Original Subscriber information
    subscriber = srv.db.get_subscriber(hostId=srv.getHostId(), subscrId=subscrId)
    if not subscriber:
        raise Exception("USUBSCRIBE command failed: Cannot find subscriber '%s'" % subscrId)

    # Get new values for subscriber and check if we need to change them
    to_update = {}
    if 'suspend' in reqPropsObj:
        suspend = int(reqPropsObj["suspend"])
        p_active = True if suspend == 1 else False
        if subscriber.active != p_active:
            to_update['active'] = p_active

    if 'priority' in reqPropsObj:
        p_priority = int(reqPropsObj["priority"])
        if p_priority != subscriber.priority:
            to_update['priority'] = p_priority

    if 'url' in reqPropsObj:
        p_url = reqPropsObj["url"]
        ngamsSubscriber.validate_url(p_url)
        if p_url != subscriber.url:
            to_update['url'] = p_url

    if 'concurrent_threads' in reqPropsObj:
        p_concurrent_threads = int(reqPropsObj["concurrent_threads"])
        if p_concurrent_threads != subscriber.concurrent_threads:
            to_update['concurrent_threads'] = p_concurrent_threads

    if not to_update:
        logger.info("Nothing new to update on subscriber %s", subscrId)
        return

    logger.info("Updating fields on subscriber %s: %r", subscrId, to_update)
    srv.getDb().updateSubscriber(subscrId, **to_update)
    srv.triggerSubscriptionThread()