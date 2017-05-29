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
"""This plug-in suspends this host if all conditions are met"""

import logging
import time

from ngamsLib import ngamsNotification
from ngamsLib.ngamsCore import NGAMS_NOTIF_ERROR, loadPlugInEntryPoint


logger = logging.getLogger(__name__)

def run(srvObj, stopEvt, jan_to_srv_queue):

    hostId = srvObj.getHostId()
    cfg = srvObj.getCfg()

    now = time.time()
    suspend = (cfg.getIdleSuspension() and
               not srvObj.getHandlingCmd() and
               not srvObj.getDb().getSrvDataChecking(hostId) and
               now - srvObj.getLastReqEndTime() >= cfg.getIdleSuspensionTime())

    if not suspend:
        return

    # Conditions are met for suspending this NGAS host.
    logger.info("NG/AMS Server %s suspending itself", hostId)

    # If Data Checking is on, we request a wake-up call.
    if cfg.getDataCheckActive():
        wakeUpSrv = cfg.getWakeUpServerHost()
        nextDataCheck = srvObj.getNextDataCheckTime()
        srvObj.reqWakeUpCall(wakeUpSrv, nextDataCheck)

    # Now, suspend this host.
    srvObj.getDb().markHostSuspended(hostId)
    suspPi = cfg.getSuspensionPlugIn()
    logger.debug("Invoking Suspension Plug-In: %s to " +\
         "suspend NG/AMS Server: %s", suspPi, hostId)
    try:
        plugInMethod = loadPlugInEntryPoint(suspPi)
        plugInMethod(srvObj)
    except Exception, e:
        errMsg = "Error suspending server with Suspension Plug-In %s"
        logger.exception(errMsg, suspPi)
        ngamsNotification.notify(hostId, cfg, NGAMS_NOTIF_ERROR,
                                 "ERROR INVOKING SUSPENSION "+\
                                 "PLUG-IN", errMsg % (suspPi,) + ": %s" % (str(e),))
        raise