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
# "@(#) $Id: ngamsHelpCmd.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  04/04/2002  Created
#

"""
Contains code for handling the HELP command.

TODO: The HELP command is not yet implemented!
"""

import pydoc
import pcc, PccUtTime
from ngams import *
import ngamsLib, ngamsStatus


def handleCmdHelp(srvObj,
                  reqPropsObj,
                  httpRef):
    """
    Handle HELP command.
        
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).
        
    Returns:        Void.
    """
    T = TRACE()

    ##########################################################################
    # COMMAND NOT YET IMPLEMENTED - REMOVE THIS BLOCK WHEN IMPLEMENTED
    ##########################################################################
    status = ngamsStatus.ngamsStatus()
    status.\
             setDate(PccUtTime.TimeStamp().getTimeStamp()).\
             setVersion(getNgamsVersion()).\
             setHostId(getHostId()).setStatus(NGAMS_FAILURE).\
             setMessage("Command HELP not implemented").\
             setState(srvObj.getState()).setSubState(srvObj.getSubState())
    msg = status.genXmlDoc()
    srvObj.httpReplyGen(reqPropsObj.setCompletionTime(), httpRef,
                        NGAMS_HTTP_SUCCESS, msg)
    srvObj.updateRequestDb(reqPropsObj)
    return
    ##########################################################################

    # Get the information requested.
    msg = ""
    doc = ""
    if (reqPropsObj.hasHttpPar("doc")):
        doc = reqPropsObj.getHttpPar("doc")
        if (doc == ""):
            msg = pydoc.HTMLDoc().index(NGAMS_SRC_DIR + "/..")
        elif (os.path.isdir("doc")):
            msg = pydoc.HTMLDoc().index(doc)
        elif (doc.find(".py") != -1):
            msg = pydoc.HTMLDoc().docmodule(NGAMS_SRC_DIR + "/" + doc)
        else:
            pass
    else:
        status = ngamsStatus.ngamsStatus()
        status.\
                 setDate(PccUtTime.TimeStamp().getTimeStamp()).\
                 setVersion(getNgamsVersion()).\
                 setHostId(getHostId()).setStatus(NGAMS_SUCCESS).\
                 setMessage("Successfully handled command HELP").\
                 setState(srvObj.getState()).setSubState(srvObj.getSubState())
        msg = status.genXmlDoc()
        
    srvObj.httpReplyGen(reqPropsObj.setCompletionTime(), httpRef,
                        NGAMS_HTTP_SUCCESS, msg)
    srvObj.updateRequestDb(reqPropsObj)


# EOF
