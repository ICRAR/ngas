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
# cwu      2013/09/22  Created
#
"""
Change a particular configuration parameter without having to restart
NGAS server

WARNING - this only changes the in-memory XML doc, but not the actual
          configuration file!!

example usage:

curl 146.118.84.67:7778/UPDATECONFIGPAR?config_param=NgamsCfg.Server%5B1%5D.BlockSize\&config_value=262144

"""

import logging
import traceback

from ngamsLib.ngamsCore import NGAMS_HTTP_SUCCESS, NGAMS_TEXT_MT


logger = logging.getLogger(__name__)

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
    if (reqPropsObj.hasHttpPar("config_param") and
        reqPropsObj.hasHttpPar("config_value")):

        config_param = reqPropsObj.getHttpPar('config_param') #e.g. NgamsCfg.Server[1].BlockSize
        config_value = reqPropsObj.getHttpPar('config_value')

        logger.debug('config_param = %s', config_param)

        try:
            srvObj.getCfg().storeVal(config_param, config_value)
            warnMsg = "WARNING - this only changes the in-memory XML doc, but not the actual configuration file!!"
            errMsg = "Successfully changed the parameter '%s' to its new value '%s'. %s \n" % (config_param, srvObj.getCfg().getVal(config_param), warnMsg)
            srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, errMsg, NGAMS_TEXT_MT)
        except Exception, ee:
            errMsg = traceback.format_exc()
            srvObj.httpReply(reqPropsObj, httpRef, 500, errMsg, NGAMS_TEXT_MT)
    else:
        errMsg = 'INVALID PARAMS, need both config_param and config_value\n'
        srvObj.httpReply(reqPropsObj, httpRef, 500, errMsg, NGAMS_TEXT_MT)