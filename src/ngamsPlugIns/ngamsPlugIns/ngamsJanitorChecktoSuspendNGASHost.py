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
import logging
import time

from ngamsLib import ngamsNotification
from ngamsLib.ngamsCore import NGAMS_NOTIF_ERROR, loadPlugInEntryPoint


logger = logging.getLogger(__name__)

def ngamsJanitorChecktoSuspendNGASHost(srvObj, stopEvt):
    """
	Check if the conditions for suspending this NGAS Host are met.

   srvObj:            Reference to NG/AMS server class object (ngamsServer).

   Returns:           Void.
   """

    hostId = srvObj.getHostId()
    srvDataChecking = srvObj.getDb().getSrvDataChecking(hostId)
    if ((not srvDataChecking) and
        (srvObj.getCfg().getIdleSuspension()) and
        (not srvObj.getHandlingCmd())):
        timeNow = time.time()
        # Conditions are that the time since the last request was
        # handled exceeds the time for suspension defined.
        if ((timeNow - srvObj.getLastReqEndTime()) >=
            srvObj.getCfg().getIdleSuspensionTime()):
            # Conditions are met for suspending this NGAS host.
            logger.info("NG/AMS Server: %s suspending itself ...", hostId)

            # If Data Checking is on, we request a wake-up call.
            if (srvObj.getCfg().getDataCheckActive()):
                wakeUpSrv = srvObj.getCfg().getWakeUpServerHost()
                nextDataCheck = srvObj.getNextDataCheckTime()
                srvObj.reqWakeUpCall(wakeUpSrv, nextDataCheck)

            # Now, suspend this host.
            srvObj.getDb().markHostSuspended(srvObj.getHostId())
            suspPi = srvObj.getCfg().getSuspensionPlugIn()
            logger.debug("Invoking Suspension Plug-In: %s to " +\
                 "suspend NG/AMS Server: %s", suspPi, hostId)
            try:
                plugInMethod = loadPlugInEntryPoint(suspPi)
                plugInMethod(srvObj)
            except Exception, e:
                errMsg = "Error suspending NG/AMS Server: " +\
                         hostId + " using Suspension Plug-In: "+\
                         suspPi + ". Error: " + str(e)
                logger.error(errMsg)
                ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(),
                                         NGAMS_NOTIF_ERROR,
                                         "ERROR INVOKING SUSPENSION "+\
                                         "PLUG-IN", errMsg)