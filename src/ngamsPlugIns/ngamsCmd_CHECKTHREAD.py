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
#

#******************************************************************************
#
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      2013/05/04  Created
#

"""
Check how many current threads are running, and print their names

"""
import threading
from ngams import *

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
    re = ''
    n = 0
    delete = 0
    if (reqPropsObj.hasHttpPar("delete") and 
        1 == int(reqPropsObj.getHttpPar("delete")) and
        reqPropsObj.hasHttpPar("threadname")):        
        delete = 1
        threadname = int(reqPropsObj.getHttpPar("threadname"))
        
    for thrObj in threading.enumerate():
        try:
            if (thrObj.isAlive()): 
                if (delete):
                    th = thrObj.getName().split('-')[0]
                    if (th == 'Thread'):
                        id = thrObj.getName().split('-')[1]
                        id = int(id)
                        if (id <= threadname):
                            thrObj._Thread__stop()
                            continue
                re += thrObj.getName() + '\n'
                n += 1
        except Exception, e:
            re += str(e)
    
    srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, 'In total ' + str(n) + ' threads\n: ' + re + '\n', NGAMS_TEXT_MT)
