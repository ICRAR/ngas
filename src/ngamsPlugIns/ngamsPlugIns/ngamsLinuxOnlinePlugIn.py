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
# "@(#) $Id: ngamsLinuxOnlinePlugIn.py,v 1.7 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  10/05/2001  Created.
#
"""
Module containing a System Online Plug-In used by the ESO NGAS installations.
"""

import logging

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import TRACE, genLog
from . import ngamsLinuxSystemPlugInApi
from .eso import ngamsEscaladeUtils


logger = logging.getLogger(__name__)

def ngamsLinuxOnlinePlugIn(srvObj,
                           reqPropsObj = None):
    """
    Function mounts all NGAMS disks and loads the kernel module for the IDE
    controller card. It returns the NGAMS specific disk info dictionary.

    srvObj:        Reference to instance of the NG/AMS Server
                   class (ngamsServer).

    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).

    Returns:       Disk info dictionary (dictionary).
    """
    T = TRACE()

    rootMtPt = srvObj.getCfg().getRootDirectory()
    parDic = ngamsPlugInApi.\
             parseRawPlugInPars(srvObj.getCfg().getOnlinePlugInPars())
    if (parDic.has_key("module")):
        stat = ngamsLinuxSystemPlugInApi.insMod(parDic["module"])
    else:
        stat = 0
    if (stat == 0):
        if (parDic.has_key("module")):
            msg = "Kernel module " + parDic["module"] + " loaded"
            logger.info(msg)

        # Old format = unfortunately some Disk IDs of WDC/Maxtor were
        # generated wrongly due to a mistake by IBM, which lead to a wrong
        # implementation of the generation of the Disk ID.
        if (not parDic.has_key("old_format")):
            oldFormat = 0
            raise Warning("Missing Online Plug-In Parameter: old_format=0|1")
        else:
            oldFormat = int(parDic["old_format"])

        # The controllers Plug-In Parameter, specifies the number of controller
        # in the system.
        if (not parDic.has_key("controllers")):
            controllers = None
        else:
            controllers = parDic["controllers"]

        # Get start index for the 3ware disk devices.
        if (not parDic.has_key("dev_start_idx")):
            devStartIdx = "a"
        else:
            devStartIdx = parDic["dev_start_idx"]


        # AWI: added this to fix problem at the ATF

        # Get start index for NGAS disk devices
        if (not parDic.has_key("ngas_start_idx")):
            ngasStartIdx = devStartIdx
        else:
            ngasStartIdx = parDic["ngas_start_idx"]


        # Try first to umount possibly mounted disks (forced).
        ngamsLinuxSystemPlugInApi.umount(rootMtPt)

        # Select between 3ware WEB Interface and 3ware Command Line Tool.
        if (parDic["uri"].find("http") != -1):
            diskDic = ngamsEscaladeUtils.parseHtmlInfo(parDic["uri"], rootMtPt)
        else:
            diskDic = ngamsEscaladeUtils.\
                      parseCmdLineInfo(rootMtPt,
                                       controllers,
                                       oldFormat,
                                       slotIds = ["*"],
                                       devStartIdx = devStartIdx,
                                       ngasStartIdx = ngasStartIdx)

        #####ngamsLinuxSystemPlugInApi.removeFstabEntries(diskDic)
        ngamsLinuxSystemPlugInApi.ngamsMount(srvObj, diskDic,
                                             srvObj.getCfg().getSlotIds())
        return diskDic
    else:
        errMsg = "Problem executing ngamsLinuxOnlinePlugIn"
        errMsg = genLog("NGAMS_ER_ONLINE_PLUGIN", [errMsg])
        raise Exception(errMsg)