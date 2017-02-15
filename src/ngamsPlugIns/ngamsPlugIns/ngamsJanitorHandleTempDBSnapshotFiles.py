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

from ngamsLib.ngamsCore import error, info, genLog
import ngamsServer


def ngamsJanitorHandleTempDBSnapshotFiles(srvObj, stopEvt, updateDbSnapShots):
    """
    Check if there are any Temporary DB Snapshot Files to handle.

    srvObj:            Reference to NG/AMS server class object (ngamsServer).

    Returns:           Void.
    """

    try:
        updateDbSnapShots(srvObj, stopEvt)
    except Exception, e:
        error("Error encountered updating DB Snapshots: " + str(e))

    # => Check Back-Log Buffer (if appropriate).
    if (srvObj.getCfg().getAllowArchiveReq() and \
                srvObj.getCfg().getBackLogBuffering()):
        info(4, "Checking Back-Log Buffer ...")
        try:
            ngamsServer.ngamsArchiveUtils.checkBackLogBuffer(srvObj)
        except Exception, e:
            errMsg = genLog("NGAMS_ER_ARCH_BACK_LOG_BUF", [str(e)])
            error(errMsg)