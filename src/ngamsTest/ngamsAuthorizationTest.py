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
# "@(#) $Id: ngamsAuthorizationTest.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  25/06/2004  Created
#

"""
This module contains the Test Suite for the Authorization Feature of NG/AMS.
"""

import os, sys
from   ngams import *
from   ngamsTestLib import *


class ngamsAuthorizationTest(ngamsTestSuite):
    """
    Synopsis:
    Test the Authorization Service implemented by NG/AMS.

    Description:
    The NG/AMS Server implements the basic HTTP authorization. In this
    Test Suite the proper functioning of this feature is exercised. This
    both with authorization enabled and disabled and valid/non-valid
    authorization code.

    Missing Test Cases:
    - Test internal authorization when:
        - Cloning files between two nodes.
        - Retrieving files between two nodes.
        - Other combinations of retrieve requests.
        - Other commands where a node may act as proxy.
    """

    def test_NoAuth_1(self):
        """
        Synopsis:
        Issue a request with Authorization disabled.
        
        Description:
        When HTTP Authorization is disabled requests can be submitted
        without issuing the authorization code.

        This Test Case exercises this case.

        Expected Result:
        The STATUS Command issued without authorization, should be accepted and
        executed by the NG/AMS Server.

        Test Steps:
        - Start standard server with HTTP auth. disabled.
        - Issue STATUS Command.
        - Check that the command was successfully executed.

        Remarks:
        ...
        """
        self.prepExtSrv()
        statObj = sendPclCmd().status()
        refStatFile = "ref/ngamsAuthorizationTest_test_NoAuth_1_1_ref"
        tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status " +\
                          "returned for Status Request")

 
    def test_UnAuthReq_1(self):
        """
        Synopsis:
        Request rejected when HTTP Auth. enabled and no Auth. Code issued
        
        Description:
        This Test Cases exercises the case where HTTP Authorization is
        enabled in the NG/AMS Server, but where no HTTP Authorization code
        is issued with a request. 

        Expected Result:
        The NG/AMS Server will return a 'failed authorization response'
        (challenging the client), which in this case means that the request
        is rejected.

        Test Steps:
        - Start server with HTTP auth. enabled + a number of users defined.
        - Issue a STATUS Command without the HTTP auth. code.
        - Check that an NGAMS_ER_UNAUTH_REQ error code is returned.

        Remarks:
        Should also check if the HTTP response code is correct.
        """
        self.prepExtSrv(cfgProps=[["NgamsCfg.Authorization[1].Enable","1"]])
        statObj = sendPclCmd().status()
        refStatFile = "ref/ngamsAuthorizationTest_test_UnAuthReq_1_1_ref"
        tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status " +\
                          "returned for Status Request")
       

    def test_UnAuthReq_2(self):
        """
        Synopsis:
        HTTP auth. enabled, illegal HTTP auth. code issued.
        
        Description:
        The purpose of this test is to check that the NG/AMS Server
        rejects a request if HTTP auth. is enabled and in invalid HTTP
        auth. code is submitted with the request.

        Expected Result:
        The server should detect the invalid HTTP auth. code and reject the
        request.

        Test Steps:
        - Start server with HTTP auth. enabled + a number of users defined.
        - Issue STATUS Command with an invalid auth. code.
        - Check that an NGAMS_ER_UNAUTH_REQ error code is returned.

        Remarks:
        Should also check if the HTTP response code is correct.
        """
        self.prepExtSrv(cfgProps=[["NgamsCfg.Authorization[1].Enable","1"]])
        statObj = sendPclCmd(auth="SUxMRUdBTDpDT0RF").status()
        refStatFile = "ref/ngamsAuthorizationTest_test_UnAuthReq_2_1_ref"
        tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status " +\
                          "returned for Status Request")


    def test_AuthReq_1(self):
        """
        Synopsis:
        Successful HTTP auth.
        
        Description:
        Test that a request is accepted when Authorization is
        enabled and the proper Authorization Code is given with the query.

        Expected Result:
        The request is submitted with a valid HTTP auth. code. The NG/AMS
        Server thus accepts and executes the request.

        Test Steps:
        - Start server with HTTP auth. enabled + a number of users defined.
        - Issue STATUS Command with a valid auth. code.
        - Check that the command is successfully executed.

        Remarks:
        ...
        """
        self.prepExtSrv(cfgProps=[["NgamsCfg.Authorization[1].Enable","1"]])
        statObj = sendPclCmd(auth="bmdhczpuZ2Fz").status()
        refStatFile = "ref/ngamsAuthorizationTest_test_AuthReq_1_1_ref"
        tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status " +\
                          "returned for Status Request")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsAuthorizationTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
