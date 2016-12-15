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
# "@(#) $Id: ngamsUserServiceThread.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  29/03/2007  Created
#
"""
This module contains the code for the User Service Thread, which execute
a plug-in provided by the user periodically.
"""

import logging
import time

from ngamsLib.ngamsCore import info, isoTime2Secs


logger = logging.getLogger(__name__)

NGAMS_USER_SERVICE_THR = "USER-SERVICE-THREAD"

def userServiceThread(srvObj, stopEvt, userServicePlugin):
    """
    The User Service Thread runs periodically a user provided plug-in
    (User Service Plug-In) which carries out tasks needed in a specific
    context.

    srvObj:      Reference to server object (ngamsServer).

    dummy:       Needed by the thread handling ...

    Returns:     Void.
    """

    plugin_name = userServicePlugin.__name__
    prefix = "NgamsCfg.SystemPlugIns[1]"

    plugin_pars = srvObj.getCfg().getVal(prefix + ".UserServicePlugInPars")
    period = srvObj.getCfg().getVal(prefix + ".UserServicePlugInPeriod")
    period = 300 if not period else isoTime2Secs(period)

    # Main loop.
    while (True):
        try:
            startTime = time.time()

            info(5,"Executing User Service Plug-In")
            userServicePlugin(srvObj, plugin_pars)
            stopTime = time.time()
            sleepTime = (period - (stopTime - startTime))

            if (sleepTime > 0):
                msg = "Executed User Service Plug-In: %s. Sleeping: %.3fs"
                info(4,msg % (plugin_name, sleepTime))

            # If signaled, return
            if stopEvt.wait(sleepTime):
                return

        except Exception, e:
            errMsg = "Error occurred during execution of the User " +\
                     "Service Thread"
            logger.exception(errMsg)
            # We make a small wait here to avoid that the process tries
            # too often to carry out the tasks that failed.
            time.sleep(2.0)


# EOF
