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
# Who       When        What
# --------  ----------  -------------------------------------------------------
# CWU      03/JUNE/2013  Created
#

"""
NGAS Command Plug-In, implementing a Quick Archive PLUS Command

This is an extension of the original QARCHIVE command, basically does 
"post-archive" processing on the file

"""

from ngams import *

import urllib2

import ngamsCmd_QARCHIVE

def handleCmd(srvObj,
              reqPropsObj,
              httpRef):
    """
    Handle the Quick Archive PLUS (QAPLUS) Command.
        
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).
        
    Returns:        Void.
    """
    (fileId, filePath, ingestRate) = ngamsCmd_QARCHIVE.handleCmd(srvObj, reqPropsObj, httpRef)
    jobManHost = srvObj.getCfg().getNGASJobMANHost()
    try:
        urllib2.urlopen('http://%s/ingest?file_id=%s&file_path=%s&to_host=%s&ingest_rate=%.2f' % (jobManHost, fileId, filePath, getHostId(), ingestRate))
    except Exception, err:
        error('Fail to send file ingestion event to server %s, Exception: %s' %(jobManHost, str(err)))
    