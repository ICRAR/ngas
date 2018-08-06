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
# "@(#) $Id: ngamsTestSuspensionPlugIn.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  23/01/2002  Created
#
"""
Test Suspension Plug-In to simulate the NGAS host suspension.

This is done in the following way:
  - The host is marked as suspended in the DB (done by the Janitor Thread).
  - The suspension test plug-in enters a loop, checking if the host is
    marked as suspended.
  - When marked as not being suspended (by ngamsTestWakeUpPlugIn), the
    plug-in exists.

See also ngamsTestWakeUpPlugIn.py.
"""

import logging
import time


logger = logging.getLogger(__name__)

def ngamsTestSuspensionPlugIn(srvObj):
    """
    Dummy Suspension Plug-In to test the handling of the NGAS host
    suspension.

    srvObj:         Reference to instance of the NG/AMS Server (ngamsServer).


    Returns:        Void.
    """
    hostId = srvObj.getHostId()
    startTime = time.time()
    while (srvObj.getDb().getSrvSuspended(hostId)): time.sleep(0.250)
    logger.debug("NGAS Node: %s woken up after %.3fs of suspension",
                 hostId, (time.time() - startTime))


# EOF
