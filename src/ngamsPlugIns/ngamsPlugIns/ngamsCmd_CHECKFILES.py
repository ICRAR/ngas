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
#
#******************************************************************************
#
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      2014/03/28  Created
"""
Check if all files belonging to this server
are still available on the file system
"""

import os, datetime, threading

from ngamsLib.ngamsCore import NGAMS_HTTP_SUCCESS, NGAMS_TEXT_MT
from ngamsLib.ngamsDb import ngamsDb


chkFileThrd = None
is_chkFileThrd_running = False
total_tocheck = 0
num_checked = 0
num_wrong = 0

def _checkFileThread(srvObj, reqPropsObj, httpRef):
    global num_checked, is_chkFileThrd_running, num_wrong
    is_chkFileThrd_running = True
    wrong_files = []

    cursorObj = srvObj.getDb().getFileSummary2(hostId = srvObj.getHostId())
    while (1):
        fileList = cursorObj.fetch(100)
        if (fileList == []): break
        for fileInfo in fileList:
            complFileUri = os.path.realpath(fileInfo[ngamsDb.ngamsDbCore.SUM2_MT_PT] +\
                                                  os.sep +\
                                                  fileInfo[ngamsDb.ngamsDbCore.SUM2_FILENAME])
            if (not os.path.exists(complFileUri)):
                ing_date = fileInfo[ngamsDb.ngamsDbCore.SUM2_ING_DATE]
                fileId = fileInfo[ngamsDb.ngamsDbCore.SUM2_FILE_ID]
                diskId = fileInfo[ngamsDb.ngamsDbCore.SUM2_DISK_ID]
                file_ver = fileInfo[ngamsDb.ngamsDbCore.SUM2_VERSION]
                wrong_files.append((ing_date, complFileUri, fileId, diskId, file_ver))
                num_wrong += 1

            num_checked += 1

    del cursorObj
    if (num_wrong):
        work_dir = srvObj.getCfg().getRootDirectory() + '/tmp'
        fname = '%s/CheckFileResult_%s' % (work_dir, chkFileThrd.getName())
        f = open(fname,'w')
        fsql = open(fname + "_del.sql", 'w')
        for item in wrong_files:
            f.write('%s\t\t%s\n' % (item[0], item[1]))
            fsql.write("DELETE FROM ngas_files WHERE file_id = '%s' AND disk_id = '%s' AND file_version = %d;\n" % (item[2], item[3], item[4]))
        f.close()
        fsql.close()




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
    global chkFileThrd
    global is_chkFileThrd_running
    if (is_chkFileThrd_running):
        if (chkFileThrd):
            srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, 'Thread %s has checked %d files, and %d files are missing\n' % (chkFileThrd.getName(), num_checked, num_wrong), NGAMS_TEXT_MT)
        else:
            is_chkFileThrd_running = False
            raise Exception('CheckFile thread\'s instance is gone!')
    else:
        args = (srvObj, reqPropsObj, httpRef)
        dt = datetime.datetime.now()
        thrdName = 'CHK_FILE_THRD_' + dt.strftime('%Y%m%dT%H%M%S') + '.' + str(dt.microsecond / 1000)
        chkFileThrd = threading.Thread(None, _checkFileThread, thrdName, args)
        chkFileThrd.setDaemon(0)
        chkFileThrd.start()
        srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, 'Thread %s is successfully launched to check files.\n' % thrdName, NGAMS_TEXT_MT)
