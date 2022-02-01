#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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
"""Finds old requests and instructs the server to remove them"""

import logging
import time

from .common import checkStopJanitorThread
from multiprocessing import queue.Empty

logger = logging.getLogger(__name__)

def timed_out(t, timeout):
    now = time.time()
    return t is not None and (now - t) >= timeout

def run(srvObj, stopEvt):

    logger.debug("Checking/cleaning up Request DB ...")
    reqTimeOut = 86400
    
    try:
        req_ids = srvObj.janitor_communicate('get-request-ids', timeout=0.5)
    except queue.Empty:
        logger.exception('no response from the server in the allocated timeout')

    to_delete = []
    for reqId in req_ids:
        reqPropsObj = srvObj.janitor_communicate('get-request', reqId, timeout=0.5)
        checkStopJanitorThread(stopEvt)

        # Remove a Request Properties Object from the DB if
        #
        # 1. The request handling is completed for more than
        #    24 hours (86400s).
        # 2. The request status has not been updated for more
        #    than 24 hours (86400s).
        comp_time = reqPropsObj.getCompletionTime()
        last_update = reqPropsObj.getLastRequestStatUpdate()
        if timed_out(comp_time, reqTimeOut) or timed_out(last_update, reqTimeOut):
            logger.debug("Scheduling removal of request with ID from Request DBM: %s", reqId)
            to_delete.append(reqId)

    if to_delete:
        srvObj.janitor_communicate('delete-requests', to_delete, timeout=5)

    logger.debug("Request DB checked/cleaned up")
