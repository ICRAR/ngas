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
# "@(#) $Id: ngamsAuthUtils.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  27/09/2002  Created
#

"""
This module utilities used to authorization.
"""

import base64

from ngams import *
import ngamsLib, ngamsHostInfo


def genUnAuthResponse(srvObj,
                      reqPropsObj,
                      httpRef):
    """
    Generate a HTTP unauthorized response (401 Unauthorized) according
    to the authorization scheme used.
    
    srvObj:        Reference to NG/AMS Server class instance (ngamsServer).
 
    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).

    httpRef:       Reference to the HTTP request handler
                   object (ngamsHttpRequestHandler).
                       
    Returns:       Void.
    """
    T = TRACE()
    
    ngamsLib.flushHttpCh(reqPropsObj.getReadFd(), 32768,
                         reqPropsObj.getSize())
    reqPropsObj.setBytesReceived(reqPropsObj.getSize())
    hostInfo = srvObj.getDb().getHostInfoFromHostIds([getHostId()])[0]
    hostInfoObj = ngamsHostInfo.ngamsHostInfo().unpackFromSqlQuery(hostInfo)
    authRealm = "Basic realm=\"ngas-clients@%s.%s\"" %\
                (getHostName(), hostInfoObj.getDomain())
    srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_UNAUTH, 
                 NGAMS_FAILURE, genLog("NGAMS_ER_UNAUTH_REQ"),
                 [["WWW-Authenticate", authRealm]])


def authorize(srvObj,
              reqPropsObj,
              httpRef):
    """
    Handle client authorization if configured. If authorization is enabled
    and the request could not be authorized, the proper response is generated.

    The exact behavior is:

      o The request didn't contain the 'Authorization' HTTP header:
      A challenge is sent back to the client (HTTP error code 401) +
      an exception is raised.

      o The request contained an illegal 'Authorization' HTTP header:
      An HTTP response with HTTP error code 401 is sent back +
      an exception is raised.

      o The request contained a legal 'Authorization' HTTP header:
      The function returns silently.

    srvObj:        Reference to NG/AMS Server class instance (ngamsServer).
 
    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).

    httpRef:       Reference to the HTTP request handler
                   object (ngamsHttpRequestHandler).

    Returns:       Void.
    """
    T = TRACE()
    
    if (not srvObj.getCfg().getAuthorize()): return

    # For now only Basic HTTP Authentication is implemented.
    if (reqPropsObj.getAuthorization()):
        try:
            scheme, reqUserPwdEnc = reqPropsObj.getAuthorization().\
                                    strip().split(" ")
            reqUserPwd = base64.decodestring(reqUserPwdEnc)
            reqUser, reqPwd = reqUserPwd.split(":")
        except Exception, e:
            errMsg = genLog("NGAMS_ER_UNAUTH_REQ") + " Error: %s" % str(e)
            raise Exception, errMsg

        # Get the user from the configuration.
        password = srvObj.getCfg().getAuthUserInfo(reqUser)
        if (password):
            decPassword = base64.decodestring(password)
        else:
            decPassword = None
    
        # Check if this user is defined and if the password matches.
        errMsg = ""
        if (not decPassword):
            errMsg = "Unknown user specified - rejecting request"
        elif (reqPwd != decPassword):
            errMsg = "Wrong password for user: " + reqUser

        if (errMsg):
            errMsg = genLog("NGAMS_ER_UNAUTH_REQ") + " Command: %s" %\
                     reqPropsObj.getCmd()
            warning(errMsg)
            
            # Generate HTTP unauthorized response.
            genUnAuthResponse(srvObj, reqPropsObj, httpRef)
            raise Exception, errMsg
    else:
        # Challenge the client.
        msg = genLog("NGAMS_ER_UNAUTH_REQ") + " Challenging client"
        warning(msg)
        genUnAuthResponse(srvObj, reqPropsObj, httpRef)
        raise Exception, msg


# EOF

