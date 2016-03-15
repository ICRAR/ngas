#
#    ICRAR - International Centre for Radio Astronomy Research
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
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      2013-08-22  Created
#
"""
This command add existing files to cache database. This command is useful
when a normal NGAS server becomes a cache server 
"""

import os, time

from ngamsLib import ngamsDb
from ngamsLib.ngamsCore import NGAMS_HTTP_SUCCESS, NGAMS_TEXT_MT, info
from ngamsServer import ngamsCacheControlThread


def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Find out which threads are still dangling
        
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).
        
    Returns:        Void.
    
    """
    myhostId = srvObj.getHostId()
    if (not srvObj.getCachingActive()):
        srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, '%s is not a Cache Server!' % myhostId, NGAMS_TEXT_MT)
        return
    
    srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, 'Adding files to the Cache db now\n', NGAMS_TEXT_MT)
    
    up_until = '2013-08-22T21:56:04.284'
    cursorObj = srvObj.getDb().getFileSummary2(hostId = myhostId, upto_ing_date = up_until)
    c = 0
    
    while (1):
        fileList = cursorObj.fetch(100)
        if (fileList == []): break
        for fileInfo in fileList:
            fileId = fileInfo[ngamsDb.ngamsDbCore.SUM2_FILE_ID]
            filename = os.path.normpath(fileInfo[ngamsDb.ngamsDbCore.SUM2_MT_PT] +\
                                              os.sep +\
                                              fileInfo[ngamsDb.ngamsDbCore.SUM2_FILENAME])
            fileVersion = fileInfo[ngamsDb.ngamsDbCore.SUM2_VERSION]
            diskId = fileInfo[ngamsDb.ngamsDbCore.SUM2_DISK_ID]
            ngamsCacheControlThread.addEntryNewFilesDbm(srvObj, diskId, fileId,
                                                   fileVersion, filename)
            c += 1
        info(3, 'Added %d files into the DB. sleep another 100 ms now...' % c)    
        time.sleep(0.1)
    info(3, 'In total, added %d files into the DB. Done' % c)
    del cursorObj