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
# "@(#) $Id: ngamsOfflineCmdTest.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  20/11/2003  Created
#

"""
This module contains the Test Suite for the OFFLINE Command.
"""

import os, sys
from   ngams import *
from   ngamsTestLib import *


class ngamsOfflineCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite for the OFFLINE Command.

    Description:
    The purpose of the Test Suite is to exercise the OFFLINE Command.
    Both normal case and abnormal cases should be tested. Latter includes:

      - Sending OFFLINE when server is busy.
      - Sending OFFLINE when server is busy and force specified.

    Missing Test Cases:
    - Should be reviewed and the missing Test Cases added.
    """

    def test_StdOffline_1(self):
        """
        Synopsis:
        test standard execution of OFFLINE Command.
        
        Description:
        The purpose of the Test Case is to specify the normal execution of the
        OFFLINE Command when the server is Online/Idle and the command is
        accepted as expected and the server brought to Offline State.

        Expected Result:
        The server in Online State should accept the OFFLINE Command and should
        go Offline.

        Test Steps:
        - Start server (Auto Online=1).
        - Submit OFFLINE Command.
        - Check the response from the server.

        Remarks:
        TODO: Check that the server is in Offline State.
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_OFFLINE_CMD,
                                 genStatFile = 1)
        refStatFile = "ref/ngamsOfflineCmdTest_test_StdOffline_1_1_ref"
        self.checkFilesEq(refStatFile, tmpStatFile,
                          "Incorrect status returned for OFFLINE command") 
        info(1,"TODO: Check that NG/AMS Server is in Offline State")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsOfflineCmdTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
