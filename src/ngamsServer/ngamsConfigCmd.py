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

#******************************************************************************
#
# "@(#) $Id: ngamsConfigCmd.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  04/04/2002  Created
#

"""
Contains code for handling the CONFIG command.
"""

import pcc, PccUtTime
from ngams import *
import ngamsLib


def handleCmdConfig(srvObj,
                    reqPropsObj,
                    httpRef):
    """
    Handle CONFIG command.
        
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).
        
    Returns:        Void.
    """
    T = TRACE()
    
    logSysLog        = None
    logSysLogPrefix  = None
    logLocalLogFile  = None
    logLocalLogLevel = None
    logVerboseLevel  = None
    logBufferSize    = None
    for httpPar in reqPropsObj.getHttpParNames():
        if (httpPar == "log_sys_Log"):
            logSysLog = reqPropsObj.getHttpPar("log_sys_Log")
        elif (httpPar == "log_sys_log_prefix"):
            logSysLogPrefix = reqPropsObj.getHttpPar("log_sys_log_prefix")
        elif (httpPar == "log_local_log_file"):
            logLocalLogFile = reqPropsObj.getHttpPar("log_local_log_file")
        elif (httpPar == "log_local_log_level"):
            logLocalLogLevel = reqPropsObj.getHttpPar("log_local_log_level")
        elif (httpPar == "log_verbose_level"):
            logVerboseLevel = reqPropsObj.getHttpPar("log_verbose_level")
        elif (httpPar == "log_buffer_size"):
            logBufferSize = reqPropsObj.getHttpPar("log_buffer_size")
        else:
            pass

    # Handle the configuration change.
    if ((logSysLog != None) or (logSysLogPrefix != None) or
        (logLocalLogFile != None) or (logLocalLogLevel != None) or
        (logVerboseLevel != None)):
        setLogCond(logSysLog, logSysLogPrefix, logLocalLogLevel,
                   logLocalLogFile, logVerboseLevel)
    if (logBufferSize != None):
        setLogCache(int(logBufferSize))

    srvObj.reply(reqPropsObj.setCompletionTime(), httpRef, NGAMS_HTTP_SUCCESS,
                 NGAMS_SUCCESS, "Handled CONFIG command")
    srvObj.updateRequestDb(reqPropsObj)


# EOF
