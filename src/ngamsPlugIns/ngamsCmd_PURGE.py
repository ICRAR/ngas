
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
# cwu      2013-12-21  Created
#

"""
Retain only the last version of the file(s), and purge all of its/their previous versions 
Does not support multiple threads

"""
import threading, datetime

from ngams import *
import ngamsDiscardCmd

QUERY_PREV_VER = "SELECT a.disk_id, a.file_id, a.file_version FROM ngas_files a, "+\
                 "(SELECT file_id, MAX(file_version) AS max_ver FROM ngas_files, ngas_disks WHERE ngas_files.disk_id = ngas_disks.disk_id AND ngas_disks.host_id = '%s' GROUP BY file_id) c, " % getHostId() +\
                 "ngas_disks b "+\
                 "WHERE a.file_id = c.file_id AND a.file_version < c.max_ver AND a.disk_id = b.disk_id AND b.host_id = '%s'" % getHostId()

purgeThrd = None
is_purgeThrd_running = False
total_todo = 0
num_done = 0

def _purgeThread(srvObj, reqPropsObj, httpRef):
    global is_purgeThrd_running, total_todo, num_done
    is_purgeThrd_running = True
    try:  
        resDel = srvObj.getDb().query(QUERY_PREV_VER)
        if (resDel == [[]]):
            raise Exception('Could not find any files to discard / retain')
        else:
            fileDelList = resDel[0]
            total_todo = len(fileDelList)
            for fileDelInfo in fileDelList:
                try:
                    ngamsDiscardCmd._discardFile(srvObj, fileDelInfo[0], fileDelInfo[1], int(fileDelInfo[2]), execute = 1)
                    num_done += 1
                except Exception, e1:
                    if (str(e1).find('DISCARD Command can only be executed locally') > -1):
                        #warning(str(e1))
                        continue
                    else:
                        raise e1
    except Exception, eee:
        errMsg = 'Fail to execute the retainThread: Exception %s' % str(eee)
        error(errMsg)
    finally:
        is_purgeThrd_running = False
        total_todo = 0
        num_done = 0
    
def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Purge all old versions on this host given a file id
        
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).
        
    Returns:        Void.
    """
    # need to check if an existing worker thread is running, if so, return an error
    # TODO - should provide an option to force stop the thread, if it is still running
    
    global purgeThrd
    global is_purgeThrd_running
    if (is_purgeThrd_running):
        if (purgeThrd):
            srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, 'Thread %s has successfully purged %d out of %d files.\n' % (purgeThrd.getName(), num_done, total_todo), NGAMS_TEXT_MT)
        else:
            is_purgeThrd_running = False
            raise Exception('Purge thread\'s instance is gone!')
    else:
        args = (srvObj, reqPropsObj, httpRef)
        dt = datetime.datetime.now()
        thrdName = 'PURGE_THREAD_' + dt.strftime('%Y%m%dT%H%M%S') + '.' + str(dt.microsecond / 1000)
        purgeThrd = threading.Thread(None, _purgeThread, thrdName, args)
        purgeThrd.setDaemon(0)
        purgeThrd.start()
        srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, 'Thread %s is successfully launched to purge files.\n' % thrdName, NGAMS_TEXT_MT)