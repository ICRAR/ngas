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
import os

from ngamsLib.ngamsCore import NGAMS_PROC_DIR
from ngamsServer.ngamsJanitorCommon import checkCleanDirs


logger = logging.getLogger(__name__)

def ngamsJanitorCheckProcessingDirectory(srvObj, stopEvt, jan_to_srv_queue):
    """
    Check and clean up Processing Directory

    srvObj:            Reference to NG/AMS server class object (ngamsServer).

    Returns:           Void.
    """
    logger.debug("Checking/cleaning up Processing Directory ...")
    procDir = os.path.normpath(srvObj.getCfg().\
                               getProcessingDirectory() +\
                               "/" + NGAMS_PROC_DIR)
    checkCleanDirs(procDir, 1800, 1800, 0)
    logger.debug("Processing Directory checked/cleaned up")