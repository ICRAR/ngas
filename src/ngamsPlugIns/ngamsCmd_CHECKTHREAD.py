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
Check how many current threads are running, and their names

"""
import threading
from ngams import *

def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Handle the trigger subscription Command.
        
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).
        
    Returns:        Void.
    """
    #srvObj.triggerSubscriptionThread()
    #lc = srvObj.getSubcrBackLogCount()
    re = ''
    n = 0
    for thrObj in threading.enumerate():
        try:
            if (thrObj.isAlive()): 
                re += str(thrObj) + '\n'
                n += 1
        except Exception, e:
            re += str(e)
    
    srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, 'In total ' + str(n) + ' threads: ' + re + '\n', NGAMS_TEXT_MT)
