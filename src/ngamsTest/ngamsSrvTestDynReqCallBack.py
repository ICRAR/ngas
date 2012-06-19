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

import os, sys, time
from   ngams import *
import ngamsServer
from   ngamsTestLib import *


class ngamsServerTestDynReqCallBack(ngamsServer.ngamsServer):
    """
    Child class of ngamsServer where the test case code can define how requests
    are handled.
    """

    def __init__(self):
        """
        Constructor method.
        """
        ngamsServer.ngamsServer.__init__(self)


    def reqCallBack(self,
                    httpRef,
                    clientAddress,
                    method,
                    path,
                    requestVersion,
                    headers,
                    writeFd,
                    readFd):
        """
        Override ngamsServer.reqCallBack(). Simply load the name of the
        request handler to execute in the file written by the test case.
        """
        T = TRACE(1)
        
        reqHandleCode = loadFile("tmp/reqCallBack_tmp")
        eval("self." + reqHandleCode +\
             "(httpRef, clientAddress, method, path, "+\
             "requestVersion, headers, writeFd, readFd)")

        
    def reqCallBack_BlockCmds1(self,
                               httpRef,
                               clientAddress,
                               method,
                               path,
                               requestVersion,
                               headers,
                               writeFd,
                               readFd):
        """
        Allow to execute EXIT, OFFLINE, STATUS. Block other commands.
        """
        T = TRACE(1)
        
        info(1,"Handling command: %s ..." % path.strip().split("?")[0])
        if ((path.strip().find(NGAMS_EXIT_CMD) == 0) or
            (path.strip().find(NGAMS_OFFLINE_CMD) == 0) or
            (path.strip().find(NGAMS_STATUS_CMD) == 0)):
            ngamsServer.ngamsServer.reqCallBack(self, httpRef, clientAddress,
                                                method, path, requestVersion,
                                                headers, writeFd, readFd)
        else:
            info(1,"Blocking request ...")
            time.sleep(10e6)


    def reqCallBack_AccArchiveBlock2(self,
                                     httpRef,
                                     clientAddress,
                                     method,
                                     path,
                                     requestVersion,
                                     headers,
                                     writeFd,
                                     readFd):
        """
        Allow to execute ARCHIVE Commands (+ EXIT, OFFLINE, STATUS).
        Block RETRIEVE Commands + other commands.
        """
        T = TRACE(1)
        
        if ((path.strip().find(NGAMS_ARCHIVE_CMD) == 0) or
            (path.strip().find(NGAMS_EXIT_CMD) == 0) or
            (path.strip().find(NGAMS_OFFLINE_CMD) == 0) or
            (path.strip().find(NGAMS_STATUS_CMD) == 0)):
            info(1,"Handling command: %s ..." % path.strip().split("?")[0])
            ngamsServer.ngamsServer.reqCallBack(self, httpRef, clientAddress,
                                                method, path, requestVersion,
                                                headers, writeFd, readFd)
        else:
            info(1,"Blocking request ...")
            time.sleep(10e6)


    def reqCallBack_IllegalResp(self,
                                httpRef,
                                clientAddress,
                                method,
                                path,
                                requestVersion,
                                headers,
                                writeFd,
                                readFd):
        """
        Override ngamsServer.reqCallBack(). This version simply writes an
        illegal HTTP response.

        Only commands that are handled are: EXIT, OFFLINE, STATUS.
        """
        T = TRACE(1)
        
        if ((path.strip().find(NGAMS_EXIT_CMD) == 0) or
            (path.strip().find(NGAMS_OFFLINE_CMD) == 0) or
            (path.strip().find(NGAMS_STATUS_CMD) == 0)):
            info(1,"Handling command: %s ..." % path.strip().split("?")[0])
            ngamsServer.ngamsServer.reqCallBack(self, httpRef, clientAddress,
                                                method, path, requestVersion,
                                                headers, writeFd, readFd)
        else:
            info(1,"Sending back illegal HTTP response ...")
            time.sleep(0.500)
            readFd.flush()
            resp = loadFile("tmp/ngamsServerTestIllegalResp_tmp")
            writeFd.write(resp)

 
    def reqCallBack_SrvCrash1(self,
                              httpRef,
                              clientAddress,
                              method,
                              path,
                              requestVersion,
                              headers,
                              writeFd,
                              readFd):
        """
        Override ngamsServer.reqCallBack(). This version simply kills itself.
        The input socket it first flushed.

        Only command that is handled is: STATUS.
        """
        T = TRACE(1)
        
        if (path.strip().find(NGAMS_STATUS_CMD) == 0):
            info(1,"Handling command: %s ..." % path.strip().split("?")[0])
            ngamsServer.ngamsServer.reqCallBack(self, httpRef, clientAddress,
                                                method, path, requestVersion,
                                                headers, writeFd, readFd)
        else:
            info(1,"Server killing itself ...")
            time.sleep(0.250)
            readFd.flush()
            self.killServer()
            sys.exit(0)


    def reqCallBack_SrvCrash2(self,
                              httpRef,
                              clientAddress,
                              method,
                              path,
                              requestVersion,
                              headers,
                              writeFd,
                              readFd):
        """
        Override ngamsServer.reqCallBack(). This version simply kills itself.
        The input socket it first flushed.

        Only command that is handled is: STATUS.
        """
        T = TRACE(1)

        if ((path.strip().find(NGAMS_ARCHIVE_CMD) == 0) or
            (path.strip().find(NGAMS_STATUS_CMD) == 0)):
            info(1,"Handling command: %s ..." % path.strip().split("?")[0])
            ngamsServer.ngamsServer.reqCallBack(self, httpRef, clientAddress,
                                                method, path, requestVersion,
                                                headers, writeFd, readFd)
        else:
            info(1,"Server killing itself ...")
            time.sleep(0.250)
            readFd.flush()
            self.killServer()
            sys.exit(0)


 
if __name__ == '__main__':
    """
    Main program executing the special test NG/AMS Server
    """
    ngamsTestSrv = ngamsServerTestDynReqCallBack()
    ngamsTestSrv.init(sys.argv)

# EOF
