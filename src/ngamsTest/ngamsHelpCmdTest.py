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
# "@(#) $Id: ngamsHelpCmdTest.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/11/2003  Created
#

"""
This module contains the Test Suite for the HELP Command.
"""


import os, sys
from   ngams import *
from   ngamsTestLib import *


class ngamsHelpCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite of the HELP Command.

    Description:
    The purpose of the Test Suite is to exercise the HELP Command.

    Missing Test Cases:
    NOTE: The HELP Command is not yet implemented. When implemented this
          Test Suite should be reviewed and the missing Test Cases added.
    """

    def test_NoPars_1(self):
        """
        Synopsis:
        Issue HELP Command with no parameters/Offline State.
        
        Description:
        Check that the response to the HELP Command is as expected (taking
        into account that the HELP Command is not yet implemented).

        Expected Result:
        The server should send back an Error Response indicating that the
        HELP Command is not yet implemented.
        
        Test Steps:
        - Start server.
        - Issue HELP Command.
        - Check that output is as expected (=command rejected).
            
        Remarks:
        This Test Case should be modified when the HELP Command has been
        implemented.
        """
        self.prepExtSrv(8888, 1, 1, 0)
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_HELP_CMD)
        refStatFile = "ref/ngamsHelpCmdTest_test_NoPars_1_1_ref"
        self.checkFilesEq(refStatFile, tmpStatFile,
                          "Incorrect status returned for HELP command")


    def test_NoPars_2(self):
        """
        Synopsis:
        Issue HELP Command with no parameters/Online State.
        
        Description:
        Check that the response to the HELP Command is as expected (taking
        into account that the HELP Command is not yet implemented).
        
        Expected Result:
        The server should send back an Error Response indicating that the
        HELP Command is not yet implemented.

        Test Steps:
        - Start server.
        - Issue HELP Command.
        - Check that output is as expected (command rejected).
            
        Remarks:
        This Test Case should be modified when the HELP Command has been
        implemented.
        """   
        self.prepExtSrv(8888, 1, 1, 1)
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_HELP_CMD)
        refStatFile = "ref/ngamsHelpCmdTest_test_NoPars_2_1_ref"
        self.checkFilesEq(refStatFile, tmpStatFile,
                          "Incorrect status returned for HELP command")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsHelpCmdTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
