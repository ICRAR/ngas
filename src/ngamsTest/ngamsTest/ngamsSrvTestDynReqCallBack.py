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
# "@(#) $Id: ngamsSrvTestDynReqCallBack.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/10/2004  Created
#
"""
Child class of ngamsServer where the test case code can define how requests
are handled.
"""

import os
import sys
import time

from ngamsLib.ngamsCore import NGAMS_EXIT_CMD, NGAMS_OFFLINE_CMD, \
    NGAMS_STATUS_CMD, NGAMS_ARCHIVE_CMD
from ngamsServer import ngamsServer
from ngamsTestLib import loadFile


class ngamsServerTestDynReqCallBack(ngamsServer.ngamsServer):
    """
    Child class of ngamsServer where the test case code can define how requests
    are handled.
    """

    def reqCallBack(self, *args, **kwargs):
        """
        Override ngamsServer.reqCallBack(). Simply load the name of the
        request handler to execute in the file written by the test case.
        """
        if os.path.exists("tmp/reqCallBack_tmp"):
            reqHandleCode = loadFile("tmp/reqCallBack_tmp")
            reqHandleCode = getattr(self, reqHandleCode)
            reqHandleCode(*args, **kwargs)
        else:
            super(ngamsServerTestDynReqCallBack, self).reqCallBack(*args, **kwargs)

    def handleHttpRequest(self, *args, **kwargs):
        if os.path.exists("tmp/handleHttpRequest_tmp"):
            reqHandleCode = loadFile("tmp/handleHttpRequest_tmp")
            reqHandleCode = getattr(self, reqHandleCode)
            reqHandleCode(*args, **kwargs)
        else:
            super(ngamsServerTestDynReqCallBack, self).handleHttpRequest(*args, **kwargs)

    def handleHttpRequest_Block5secs(self, *args, **kwargs):
        """
        Allow to execute EXIT, OFFLINE, STATUS. Block other commands for 5 secs
        """
        path = kwargs['path'] if 'path' in kwargs else args[4]
        cmd = path.strip().split('?')[0]
        if cmd not in (NGAMS_EXIT_CMD, NGAMS_OFFLINE_CMD, NGAMS_STATUS_CMD):
            time.sleep(5)
        super(ngamsServerTestDynReqCallBack, self).handleHttpRequest(*args, **kwargs)


    def reqCallBack_BlockCmds1(self, *args, **kwargs):
        """
        Allow to execute EXIT, OFFLINE, STATUS. Block other commands.
        """
        path = kwargs['path'] if 'path' in kwargs else args[3]
        cmd = path.strip().split('?')[0]
        if cmd in (NGAMS_EXIT_CMD, NGAMS_OFFLINE_CMD, NGAMS_STATUS_CMD):
            super(ngamsServerTestDynReqCallBack, self).reqCallBack(*args, **kwargs)
        else:
            time.sleep(10e6)


    def reqCallBack_AccArchiveBlock2(self, *args, **kwargs):
        """
        Allow to execute ARCHIVE Commands (+ EXIT, OFFLINE, STATUS).
        Block RETRIEVE Commands + other commands.
        """
        path = kwargs['path'] if 'path' in kwargs else args[3]
        cmd = path.strip().split('?')[0]
        if cmd in (NGAMS_ARCHIVE_CMD, NGAMS_EXIT_CMD, NGAMS_OFFLINE_CMD, NGAMS_STATUS_CMD):
            super(ngamsServerTestDynReqCallBack, self).reqCallBack(*args, **kwargs)
        else:
            time.sleep(10e6)


    def reqCallBack_IllegalResp(self, *args, **kwargs):
        """
        Override ngamsServer.reqCallBack(). This version simply writes an
        illegal HTTP response.

        Only commands that are handled are: EXIT, OFFLINE, STATUS.
        """
        path = kwargs['path'] if 'path' in kwargs else args[3]
        req_handler = kwargs['httpRef'] if 'httpRef' in kwargs else args[0]

        cmd = path.strip().split('?')[0]

        if cmd in (NGAMS_EXIT_CMD, NGAMS_OFFLINE_CMD, NGAMS_STATUS_CMD):
            super(ngamsServerTestDynReqCallBack, self).reqCallBack(*args, **kwargs)
        else:
            # Sending back illegal HTTP response
            time.sleep(0.500)
            resp = loadFile("tmp/ngamsServerTestIllegalResp_tmp")
            req_handler.wfile.write(resp)


    def reqCallBack_SrvCrash1(self, *args, **kwargs):
        """
        Override ngamsServer.reqCallBack(). This version simply kills itself.
        The input socket it first flushed.

        Only command that is handled is: STATUS.
        """
        path = kwargs['path'] if 'path' in kwargs else args[3]
        cmd = path.strip().split('?')[0]

        if cmd == NGAMS_STATUS_CMD:
            super(ngamsServerTestDynReqCallBack, self).reqCallBack(*args, **kwargs)
        else:
            time.sleep(0.250)
            self.killServer()
            sys.exit(0)


    def reqCallBack_SrvCrash2(self, *args, **kwargs):
        """
        Override ngamsServer.reqCallBack(). This version simply kills itself.
        The input socket it first flushed.

        Only command that is handled is: STATUS.
        """
        path = kwargs['path'] if 'path' in kwargs else args[3]
        cmd = path.strip().split('?')[0]

        if cmd in (NGAMS_ARCHIVE_CMD, NGAMS_STATUS_CMD):
            super(ngamsServerTestDynReqCallBack, self).reqCallBack(*args, **kwargs)
        else:
            time.sleep(0.250)
            self.killServer()
            sys.exit(0)



if __name__ == '__main__':
    """
    Main program executing the special test NG/AMS Server
    """
    ngamsTestSrv = ngamsServerTestDynReqCallBack()
    ngamsTestSrv.init(sys.argv)

# EOF
