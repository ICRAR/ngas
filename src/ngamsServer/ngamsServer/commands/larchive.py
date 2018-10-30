#
#    ALMA - Atacama Large Millimiter Array
#    (c) European Southern Observatory, 2002
#    Copyright by ESO (in the framework of the ALMA collaboration),
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
"""
NGAS Command Plug-In, implementing a Local Archive Command.

This works by calling archiveFromFile, which in turn takes care of all the handling

Usgae example with wget:

wget -O LARCHIVE.xml "http://192.168.1.123:7777/LARCHIVE?fileUri=/home/ngas/NGAS/log/LogFile.nglog"
"""

import logging
import time

from ngamsLib.ngamsCore import NGAMS_FAILURE, getDiskSpaceAvail, NGAMS_TEXT_MT
from ngamsServer import ngamsArchiveUtils


logger = logging.getLogger(__name__)


def handleCmd(srvObj,
              reqPropsObj,
              httpRef):
    """
    Handle the Quick Archive (QARCHIVE) Command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        (fileId, filePath) tuple.
    """

    # LARCHIVE was designed to work with the fileUri parameter, unlike the rest
    # of the archiving commands that work with a combination of the Content-Disposition
    # header and the filename parameter
    reqPropsObj.setFileUri(reqPropsObj.get('fileUri', ''))

    mimeType = ngamsArchiveUtils.archiveInitHandling(srvObj, reqPropsObj, httpRef, do_probe=False, try_to_proxy=False)
    resDapi, targDiskInfo = ngamsArchiveUtils.archiveFromFile(srvObj, reqPropsObj.getFileUri(), 0, mimeType, reqPropsObj)

    if (resDapi == NGAMS_FAILURE):
        errMsg = targDiskInfo
        httpRef.send_data(errMsg, NGAMS_TEXT_MT, code=500)
        return

    # Check if the disk is completed.
    # We use an approximate estimate for the remaning disk space to avoid
    # to read the DB.
    logger.debug("Check available space in disk")
    availSpace = getDiskSpaceAvail(targDiskInfo.getMountPoint(), smart=False)
    if (availSpace < srvObj.getCfg().getFreeSpaceDiskChangeMb()):
        targDiskInfo.setCompleted(1).setCompletionDate(time.time())
        targDiskInfo.write(srvObj.getDb())

    ngamsArchiveUtils.finish_archive_request(srvObj, reqPropsObj, httpRef, resDapi, targDiskInfo)