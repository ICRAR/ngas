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

import time

from ngamsLib.ngamsCore import info
import ngamsServer


def ngamsJanitorChecktoWakeupOtherNGASHost(srvObj, stopEvt):
    """
    Check if this NG/AMS Server is requested to wake up another/other NGAS Host(s).

    srvObj:            Reference to NG/AMS server class object (ngamsServer).

    Returns:           Void.
    """
    hostId = srvObj.getHostId()
    timeNow = time.time()
    for wakeUpReq in srvObj.getDb().getWakeUpRequests(srvObj.getHostId()):
        # Check if the individual host is 'ripe' for being woken up.
        suspHost = wakeUpReq[0]
        if (timeNow > wakeUpReq[1]):
            info(2, "Found suspended NG/AMS Server: " + suspHost + " " + \
                 "that should be woken up by this NG/AMS Server: " + \
                 hostId + " ...")
            ngamsServer.ngamsSrvUtils.wakeUpHost(srvObj, suspHost)