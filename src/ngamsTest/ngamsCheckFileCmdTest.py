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
# "@(#) $Id: ngamsCheckFileCmdTest.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  10/11/2003  Created
#

"""
This module contains the Test Suite for the CHECKFILE Command.
"""

import os, sys
from   ngams import *
from   ngamsTestLib import *


class ngamsCheckFileCmdTest(ngamsTestSuite):
    """
    Synopsis:
    CHECKFILE Command.

    Description:
    This Test Suites exercises the CHECKFILE Command. It verifies the
    nominal behavior and the behavior under abnormal conditions.

    Missing Test Cases:
    - Various combinations of disk_id/file_id/file_version (legal/illegal).
    """

    def test_NormalExec_1(self):
        """
        Synopsis:
        Normal execution of CHECKFILE Command.
        
        Description:
        The purpose of this Text Case is to test the normal/standard
        execution of the CHECKFILE Command.

        Expected Result:
        The CHECKFILE Command should be accepted and executed successfully
        by the server on an existing file.

        Test Steps:
        - Start NG/AMS Server.
        - Archive a file successfully.
        - Check the file specifying Disk ID/File Id/File Version.
        - Check the the result returned by the NG/AMS Server is as expected.

        Remarks:
        ...
        """
        self.prepExtSrv()
        sendPclCmd().archive("src/SmallFile.fits")
        diskId = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        fileId = "TEST.2001-05-08T15:25:00.123"
        statObj = sendPclCmd().sendCmdGen(getHostName(), 8888,
                                          NGAMS_CHECKFILE_CMD,
                                          pars = [["disk_id", diskId],
                                                  ["file_id", fileId],
                                                  ["file_version", "1"]])
        refStatFile = "ref/ngamsCheckFileCmdTest_test_NormalExec_1_1_ref"
        refStatFile = saveInFile(None, loadFile(refStatFile) % getHostName())
        tmpStatFile = saveInFile(None, statObj.getMessage())
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect info in " +\
                          "STATUS Command XML Status Document")


    def test_ErrHandling_1(self):
        """
        Synopsis:
        Check error handling in connection with CHECKFILE Command.
        
        Description:
        The purpose of the Test Case is to check the correct behavior/handling
        of the NG/AMS Server when it comes to handle the following cases:

          - Disk not existing.
          - File not existing.
          - File Version not existing. 

        Expected Result:
        The NG/AMS Server should detect the invalid parameters and should
        return a proper error code.

        Test Steps:
        - Start normal NG/AMS Server.
        - Archive file.
        - Issue CHECKFILE Command with a non-existing Disk ID + check that
          a proper error response is returned.
         - Issue CHECKFILE Command with a non-existing File ID + check that
          a proper error response is returned.
         - Issue CHECKFILE Command with a non-existing File Version + check
           that a proper error response is returned.

        Remarks:
        ...
        """
        self.prepExtSrv()
        sendPclCmd().archive("src/SmallFile.fits")

        # 1) Disk not existing, 2) File not existing, 3) File Version
        # not existing.
        testDataList = [["Disk ID Non-Existing", 
                         "___tmp-ngamsTest-NGAS-FitsStorage1-Main-1___",
                         "TEST.2001-05-08T15:25:00.123",
                         "1",
                         "ref/ngamsCheckFileCmdTest_test_ErrHandling_1_1_ref"],
                        ["File ID Non-Existing",
                         "tmp-ngamsTest-NGAS-FitsStorage1-Main-1",
                         "___TEST.2001-05-08T15:25:00.123___",
                         "1",
                         "ref/ngamsCheckFileCmdTest_test_ErrHandling_1_2_ref"],
                        ["File Version Non-Existing",
                         "tmp-ngamsTest-NGAS-FitsStorage1-Main-1",
                         "TEST.2001-05-08T15:25:00.123",
                         "100",
                         "ref/ngamsCheckFileCmdTest_test_ErrHandling_1_3_ref"]]
        for testData in testDataList:
            statObj = sendPclCmd().\
                      sendCmdGen(getHostName(), 8888,
                                 NGAMS_CHECKFILE_CMD,
                                 pars = [["disk_id", testData[1]],
                                         ["file_id", testData[2]],
                                         ["file_version", testData[3]]])
            tmpStatFile = saveInFile(None, statObj.getMessage())
            self.checkFilesEq(testData[4], tmpStatFile, "Incorrect info in " +\
                              "STATUS Command XML Status Document/" +\
                              testData[0])


    def test_ProxyMode_01(self):
        """
        Synopsis:
        Test that the proxy mode works for the CHECKFILE Command/cluster.
        
        Description:
        The purpose of the test is to verify that the proxy mode works
        properly for the CHECKFILE Command.

        In practice, this means that if a file to be check is stored on
        another NGAS Node than the contacted one, the contacted node (acting
        as cluster master) should locate the file and forward the request to
        the node hosting the file.

        Expected Result:
        When issuing the CHECKFILE Command to the cluster master node, it
        should figure out that the file specified is stored on a sub-node.
        The master node will forward the request to the sub-node, which will
        carry out the check and return the response, which is then forwarded
        to the client.

        Test Steps:
        - Start simulated cluster with 1 MNU + 1 NCU.
        - Archive file onto the NCU.
        - Archive file onto the NMU.
        - Issue a CHECKFILE Command to the NMU specifying the file on the NCU.
        - Verify that the proper file has been checked.

        Remarks:
        ...

        Test Data:
        ...
        """
        self.prepCluster("src/ngamsCfg.xml",
                         [[8000, None, None, getClusterName()],
                          [8011, None, None, getClusterName()]])
        sendPclCmd(port=8000).archive("src/SmallFile.fits")
        stat = sendPclCmd(port=8011).archive("src/SmallFile.fits")
        diskId  = "tmp-ngamsTest-NGAS:8011-FitsStorage1-Main-1"
        fileId  = "TEST.2001-05-08T15:25:00.123"
        fileVer = 2
        httpPars=[["disk_id", diskId], ["file_id", fileId],
                  ["file_version", fileVer]]
        tmpStatFile = sendExtCmd(getHostName(), 8000, NGAMS_CHECKFILE_CMD,
                                 pars=httpPars, replaceLocalHost=1)
        refStatFile = "ref/ngamsCheckFileCmdTest_test_ProxyMode_01_01_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect handling of "+\
                          "CHECKFILE Command detected")
        

def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsCheckFileCmdTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
