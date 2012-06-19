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
# "@(#) $Id: ngamsInitCmdTest.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  20/11/2003  Created
#

"""
This module contains the Test Suite for the INIT Command.
"""

import os, sys
from   ngams import *
from   ngamsTestLib import *


class ngamsInitCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite for the INIT Command.

    Description:
    The purpose of the Test Suite is to exercise the INIT Command.

    Missing Test Cases:
    - Missing Test Cases for abnormal conditions.
    - Test normal case when loading cfg. from the DB.
    """

    def test_handleCmdInit_1(self):
        """
        Synopsis:
        Normal execution of INIT Command/cfg. in file.
        
        Description:
        The purpose of the Test Case is to verify that the server
        re-initializes and reloads the associated cfg. file when the INIT
        Command is received.

        Expected Result:
        When the INIT Command is received by a server in Online State, the
        server should reload the configuration and go into Online State.

        Test Steps:
        - Start server.
        - Issue an INIT Command.
        - Check that response from the server is as expected.

        Remarks:
        TODO: Should change some parameters to verify that the cfg. file is
              actually re-loaded.
        """
        self.prepExtSrv(8888, 1, 1, 1)
        info(1,"TODO: Change some cfg. parameter")
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_INIT_CMD)
        refStatFile = "ref/ngamsInitCmdTest_test_handleCmdInit_1_1_ref"
        self.checkFilesEq(refStatFile, tmpStatFile,
                          "Incorrect status returned for INIT Command")
        info(1,"TODO: Check that server has initialized with new parameter")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsInitCmdTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
