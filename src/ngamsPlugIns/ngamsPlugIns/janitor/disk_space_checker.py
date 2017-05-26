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
"""Checks if there's enough disk space, sets the system offline if necessary"""

import logging

from ngamsLib.ngamsCore import NGAMS_HTTP_INT_AUTH_USER
from ngamsPClient import ngamsPClient


logger = logging.getLogger(__name__)

def run(srvObj, stopEvt, jan_to_srv_queue):
    """
    Check if there is enough disk space for the various
    directories defined.
    """

    try:
        srvObj.checkDiskSpaceSat()
    except Exception:
        logger.exception("Not enough disk space, bringing the system Offline")

        # Connect to the server and send an OFFLINE command
        host, port = srvObj.get_endpoint()
        auth = None
        if srvObj.getCfg().getAuthorize():
            auth = srvObj.getCfg().getAuthHttpHdrVal(NGAMS_HTTP_INT_AUTH_USER)

        client = ngamsPClient.ngamsPClient(host, port, timeout=30, auth=auth)
        client.offline(force=True)