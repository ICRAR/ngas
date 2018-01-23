#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
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
# "@(#) $Id: ngamsSrvTestBrokSockRetrieve.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/10/2004  Created
#
"""
Special version of the NG/AMS Server class used to send back a non-sense
(corrupted/illegal) HTTP response.
"""

import sys

from ngamsLib.ngamsCore import TRACE
from ngamsServer import ngamsServer


def cleanUpAfterProc(dummy):
    pass

def genReplyRetrieveFail(srvObj,
                         reqPropsObj,
                         httpRef,
                         statusObjList):
    """
    Used to override ngamsRetrieveCmd.genReplyRetrieve(). This implementation
    simluates a broken socket connection while data is being sent back to the
    client while handling a Retrieve Request.

    See ngamsRetrieveCmd.genReplyRetrieve() for further details. The two
    methods are (and should continue to be identical apart from the section
    indicated below with 'TEST:').
    """
    T = TRACE()

    # Send back reply with the result queried.
    try:
        resObj = statusObjList[0].getResultObject(0)

        mimeType = resObj.getMimeType()
        dataSize = resObj.getDataSize()
        refFilename = resObj.getRefFilename()

        contDisp = 'attachment; filename="%s"' % refFilename
        hdrs = {'Content-Disposition': contDisp,
                'Content-Length': str(dataSize),
                'Content-Type': mimeType}
        httpRef.send_response(200, hdrs=hdrs)
        httpRef.end_headers()

        #############################################################
        # TEST: SIMULATE BROKEN SOCKET BY TERMINATING WHEN HALF OF
        #       THE DATA HAS BEEN SENT.
        #############################################################
        srvObj.killServer()
        #############################################################

    except Exception, e:
        cleanUpAfterProc(statusObjList)
        raise e


class ngamsServerTestBrokSockRetrieve(ngamsServer.ngamsServer):
    """
    Special version of the NG/AMS Server class used to send back a non-sense
    (corrupted/illegal) HTTP response.
    """

    def __init__(self):
        """
        Constructor method.
        """
        ngamsServer.ngamsServer.__init__(self)
        # Replace the function ngamsRetrieveCmd.genReplyRetrieve() used to
        # reply to the RETRIEVE Command with the test version provoking a
        # broken socket situation.
        from ngamsServer import ngamsRetrieveCmd
        ngamsRetrieveCmd.genReplyRetrieve = genReplyRetrieveFail


if __name__ == '__main__':
    """
    Main program executing the special test NG/AMS Server
    """
    ngamsTestSrv = ngamsServerTestBrokSockRetrieve()
    ngamsTestSrv.init(sys.argv)

# EOF
