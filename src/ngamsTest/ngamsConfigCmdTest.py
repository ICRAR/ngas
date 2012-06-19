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
# "@(#) $Id: ngamsConfigCmdTest.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  19/04/2002  Created
#

"""
This module contains the Test Suite for the CONFIG Command.
"""

import sys, unittest, socket
from   ngams import *
from   ngamsTestLib import *
import ngamsHighLevelLib, ngamsConfig


class ngamsConfigCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite testing the CONFIG Command.

    Description:
    The purpose of this Test Suite is to test the handling of the CONFIG
    Command in the NG/AMS Server.

    Missing Test Cases:
    - Test proxy mode for CONFIG Command.
    - Many Test Cases are missing. A review of the Test Suite and of the
      CONFIG Command should be carried out and the necessary Test Cases added.
    """

    def test_ChangeLocLogLev_1(self):
        """
        Synopsis:
        Change the Local (Log File) Log Level Online.
        
        Description:
        The purpose of this Test Case is to test that the Local Log Level
        can be changed while the NG/AMS Server is Online.

        Expected Result:
        The Log Level should be increased and more logs produced in the Local
        Log File.

        Test Steps:
        - Start normal NG/AMS Server (Log Level=3).
        - Send a CONFIG Command to the server changing the Log Level to 4.
        - Check the response to the CONFIG Command is correct.

        Remarks:
        TODO: Check in Log File that low level logs are produced.
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        statObj = ngamsPClient.ngamsPClient().\
                  sendCmdGen(getHostName(), 8888, NGAMS_CONFIG_CMD,
                             pars = [["log_local_log_level", "4"]])
        tmpStatFile = "tmp/ngamsConfigCmdTest_test_ChangeLocLogLev_1_1_tmp"
        refStatFile = "ref/ngamsConfigCmdTest_test_ChangeLocLogLev_1_1_ref"
        saveInFile(tmpStatFile, filterOutLines(statObj.dumpBuf(),
                                               ["Date", "Version", "HostId"]))
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect info in " +\
                          "CONFIG Command XML Status " +
                          "Document/log_local_log_level")        

def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsConfigCmdTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
