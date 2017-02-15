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

import os

from ngamsLib import ngamsHighLevelLib
from ngamsLib.ngamsCore import NGAMS_SUBSCR_BACK_LOG_DIR
from ngamsLib.ngamsCore import info, isoTime2Secs


def ngamsJanitorCheckSubscrBacklognTempDir(srvObj, stopEvt,checkCleanDirs):
    """
	Checks/cleans up Subscription Back-Log Buffer and
    Checks/cleans up NG/AMS Temp Directory of any leftover files.

   srvObj:            Reference to NG/AMS server class object (ngamsServer).

   Returns:           Void.
   """
    info(4, "Checking/cleaning up Subscription Back-Log Buffer ...")
    backLogDir = os.path. \
        normpath(srvObj.getCfg().getBackLogBufferDirectory() + \
                 "/" + NGAMS_SUBSCR_BACK_LOG_DIR)
    expTime = isoTime2Secs(srvObj.getCfg().getBackLogExpTime())
    checkCleanDirs(backLogDir, expTime, expTime, 0)
    info(4, "Subscription Back-Log Buffer checked/cleaned up")

    # => Check if there are left-over files in the NG/AMS Temp. Dir.
    info(4, "Checking/cleaning up NG/AMS Temp Directory ...")
    tmpDir = ngamsHighLevelLib.getTmpDir(srvObj.getCfg())
    expTime = (12 * 3600)
    checkCleanDirs(tmpDir, expTime, expTime, 1)
    info(4, "NG/AMS Temp Directory checked/cleaned up")