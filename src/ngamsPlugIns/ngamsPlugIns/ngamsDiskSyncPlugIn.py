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
# "@(#) $Id: ngamsDiskSyncPlugIn.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  10/05/2001  Created.
#
"""
Module that contains a generic Disk Dync Plug-In for Linux.
"""

import logging
import subprocess
import threading

logger = logging.getLogger(__name__)

# Used to serialize the startup of server processes
# due to the subprocess module not handling threaded code well
# in python 2.7
_proc_startup_lock = threading.Lock()

def ngamsDiskSyncPlugIn(srvObj):
    """
    Disk Sync Plug-In to flush the cache of the 3ware controller.

    srvObj:        Reference to instance of NG/AMS Server class (ngamsServer).

    Returns:       Void.
    """
    # Sync filesystem to ensure file received on disk.
    logger.debug("Performing OS sync command ...")
    with _proc_startup_lock:
        subprocess.call("sync")


if __name__ == '__main__':
    """
    Main function.
    """
    pass


# EOF
