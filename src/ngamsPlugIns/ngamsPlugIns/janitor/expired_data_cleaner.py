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
"""This plug-in removes expired data from a number of NGAS directories"""

import logging
import os

from ngamsLib import ngamsHighLevelLib
from ngamsLib.ngamsCore import NGAMS_SUBSCR_BACK_LOG_DIR, NGAMS_PROC_DIR
from ngamsLib.ngamsCore import isoTime2Secs
from ngamsServer.ngamsJanitorCommon import checkCleanDirs


logger = logging.getLogger(__name__)

def run(srvObj, stopEvt, jan_to_srv_queue):

    cfg = srvObj.getCfg()
    cleaning_info = (
        ("processing directory",
         os.path.join(cfg.getProcessingDirectory(), NGAMS_PROC_DIR),
         1800,
         0),
        ("subscription backlog buffer",
         os.path.join(cfg.getBackLogBufferDirectory(), NGAMS_SUBSCR_BACK_LOG_DIR),
         isoTime2Secs(cfg.getBackLogExpTime()),
         0),
        ("NGAS tmp directory",
         ngamsHighLevelLib.getTmpDir(cfg),
         12 * 3600,
         1)
    )

    for desc, d, t, use_last_access in cleaning_info:
        logger.info("Checking/cleaning up %s: %s", desc, d)
        checkCleanDirs(d, t, t, use_last_access)