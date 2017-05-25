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
"""This plug-in wakes up other servers that requested to be woken up by us"""

import logging
import time

from ngamsServer import ngamsSrvUtils


logger = logging.getLogger(__name__)

def run(srvObj, stopEvt, jan_to_srv_queue):

    hostId = srvObj.getHostId()
    now = time.time()

    for host, wakeup_time in srvObj.getDb().getWakeUpRequests(hostId):
        if now <= wakeup_time:
            continue
        logger.info("Waking up server %s", host)
        ngamsSrvUtils.wakeUpHost(srvObj, host)