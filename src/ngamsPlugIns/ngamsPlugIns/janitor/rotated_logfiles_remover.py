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
"""This plug-in removes old logging rotation files"""

import glob
import logging
import os

from ngamsLib.ngamsCore import rmFile


logger = logging.getLogger(__name__)

def run(srvObj, stopEvt, jan_to_srv_queue):
    logger.debug("Check if there are rotated Local Log Files to remove ...")

    logFile = srvObj.getCfg().getLocalLogFile()
    pattern = os.path.join(os.path.dirname(logFile), 'LOG-ROTATE-*.nglog')
    files = glob.glob(pattern)
    files.sort()
    max_rotations = max(min(srvObj.getCfg().getLogRotateCache(), 100), 0)
    to_delete = files[max_rotations:]
    for f in to_delete:
        logger.info("Removing Rotated Local Log File: %s", f)
        rmFile(f)