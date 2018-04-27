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
Retain only files look like this:

OBSID_32MHZBW_XX/YY_r0.0_v1.0

e.g.
1060792928_200-231MHz_XX_r0.0_v1.0.fits

I estimate that we will need  67*2*30*24*7*5/1000 ~ 3.4 TB of disk space.
(67 MB per file, 2 pols, 30 files an hour, 24 hours, 7 pointings, 5 freqs)

"""

import datetime
import logging
import threading

from ngamsLib.ngamsCore import NGAMS_TEXT_MT
from ngamsServer import ngamsDiscardCmd


logger = logging.getLogger(__name__)

QUERY_ALL_FILES = "SELECT a.disk_id, a.file_id, a.file_version FROM ngas_files a, "+\
                 "ngas_disks b WHERE a.disk_id = b.disk_id AND b.host_id = {0}"

purgeThrd = None
is_purgeThrd_running = False
total_todo = 0
num_done = 0

def _shouldRetain(fileId):
    try:
        tokens = fileId.split('_')
        if (len(tokens) != 5):
            return False
        if (tokens[2] == 'XX' or tokens[2] == 'YY'):
            if (tokens[3] == 'r0.0' and tokens[4] == 'v1.0.fits'):
                freqs = tokens[1].split('-')
                bandwidth = (int(freqs[1][:-3]) - int(freqs[0]) + 1)
                if (bandwidth > 31 and bandwidth < 34):
                    return True
                else:
                    return False
            else:
                return False
        else:
            return False
    except Exception:
        errMsg = '_shouldRetain in rri purge thread failed'
        logger.exception(errMsg)
        return True

def _purgeThread(srvObj, reqPropsObj, httpRef):
    global is_purgeThrd_running, total_todo, num_done
    is_purgeThrd_running = True
    try:
        logger.debug("host_id = %s", srvObj.getHostId())
        resDel = srvObj.getDb().query2(QUERY_ALL_FILES, args=(srvObj.getHostId(),))
        if not resDel:
            raise Exception('Could not find any files to discard / retain')
        else:
            total_todo = len(resDel)
            for fileDelInfo in resDel:
                try:
                    if (_shouldRetain(fileDelInfo[1])):
                        continue
                    ngamsDiscardCmd._discardFile(srvObj, fileDelInfo[0], fileDelInfo[1], int(fileDelInfo[2]), execute = 1)
                    num_done += 1
                except Exception as e1:
                    if (str(e1).find('DISCARD Command can only be executed locally') > -1):
                        #warning(str(e1))
                        continue
                    else:
                        raise e1
    except Exception:
        errMsg = 'Fail to execute the rri purge thread'
        logger.exception(errMsg)
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
            msg= 'Thread %s has successfully rri_purged %d out of %d files.\n' % (purgeThrd.getName(), num_done, total_todo)
            httpRef.send_data(msg, NGAMS_TEXT_MT)
        else:
            is_purgeThrd_running = False
            raise Exception('RRI Purge thread\'s instance is gone!')
    else:
        args = (srvObj, reqPropsObj, httpRef)
        dt = datetime.datetime.now()
        thrdName = 'RRI_PURGE_THREAD_' + dt.strftime('%Y%m%dT%H%M%S') + '.' + str(dt.microsecond / 1000)
        purgeThrd = threading.Thread(None, _purgeThread, thrdName, args)
        purgeThrd.setDaemon(0)
        purgeThrd.start()
        msg = 'Thread %s is successfully launched to rri_purge files.\n' % thrdName
        httpRef.send_data(msg, NGAMS_TEXT_MT)