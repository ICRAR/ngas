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
# "@(#) $Id: ngamsArchiveCmd.py,v 1.10 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  13/05/2003  Created
#

"""
Function to handle the ARCHIVE command.
"""

import logging

import ngamsArchiveUtils
from ngamsLib.ngamsCore import genLog, NGAMS_IDLE_SUBSTATE, \
    NGAMS_NOTIF_ERROR, NGAMS_NOTIF_NO_DISKS
from ngamsLib import ngamsNotification, ngamsDiskUtils


logger = logging.getLogger(__name__)

def get_target_disk(srvObj, mimeType, file_uri, size):
    try:
        return ngamsDiskUtils.findTargetDisk(srvObj.getHostId(),
                                             srvObj.getDb(), srvObj.getCfg(),
                                             mimeType, 0, caching=0,
                                             reqSpace=size)
    except Exception as e:
        errMsg = str(e) + ". Attempting to archive file: %s" % file_uri
        ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(), NGAMS_NOTIF_NO_DISKS,
                                  "NO DISKS AVAILABLE", errMsg)
        raise

def handleCmdArchive(srvObj,
                     reqPropsObj,
                     httpRef):
    """
    Handle an ARCHIVE command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """

    # Execute the init procedure for the ARCHIVE Command.
    do_probe = 'probe' in reqPropsObj and int(reqPropsObj["probe"])
    mimeType = ngamsArchiveUtils.archiveInitHandling(srvObj, reqPropsObj, httpRef,
                                   do_probe=do_probe, try_to_proxy=True)
    if (not mimeType):
        # Set ourselves to IDLE; otherwise we'll stay in BUSY even though we
        # are doing nothing
        srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
        return

    def find_target_disk():
        return get_target_disk(srvObj, mimeType, reqPropsObj.getSafeFileUri(),
                               reqPropsObj.getSize())

    ngamsArchiveUtils.dataHandler(srvObj, reqPropsObj, httpRef,
                                  find_target_disk=find_target_disk,
                                  pickle_request=True, sync_disk=True,
                                  do_replication=True)


# EOF
