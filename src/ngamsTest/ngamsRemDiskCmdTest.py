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
# "@(#) $Id: ngamsRemDiskCmdTest.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  23/04/2002  Created
#

"""
This module contains the Test Suite for the REMDISK Command.
"""

import os, sys, glob
from   ngams import *
import ngamsLib, ngamsDiskInfo, ngamsStatus, ngamsHighLevelLib, ngamsPClient
from   ngamsTestLib import *


class ngamsRemDiskCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Test handling of REMDISK Command.

    Description:
    The purpose of the Test Suite is to exercise the REMDISK Command under
    normal and abnormal conditions. It is crucial to verify that it is not
    possible to remove disks containing files available in less than 3
    copies in the archive.

    Missing Test Cases:
    - Test Suite should be reviewed and missing Test Cases added. In particular
      Test Cases exercising abnormal cases should be added.

    - File info can be deleted, file on disk not.
    """

    def test_RemDiskCmd_1(self):
        """
        Synopsis:
        Normal execution.
        
        Description:
        Test the normal execution of the REMDISK Command, where a disk,
        containing files with at least 3 independent copies is request
        REMDISK'ed.

        Expected Result:
        When executing the REMDISK Command on the cloned disk with execute=0,
        the server should accept the command and report this to the client.

        When re-issuing the REMDISK Command on the cloned disk with execute=1,
        the server should accept the command, execute the REMDISK procedure on
        the disk and report this to the client.

        Test Steps:
        - Start server.
        - Archive a file.
        - Clone the disk hosting the archived file.
        - Issue REMDISK Command to remove the cloned disk (execute=0).
        - Check response from the server.
        - Issue REMDISK Command to remove the cloned disk (execute=1).
        - Check response from the server.

        Remarks:
        TODO!: It is not checked that the contents on the disk and the info
               in the DB in ngas_files and ngas_disks is properly updated.
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)

        # Archive a file + clone it to be able to execute the REMDISK Command.
        diskId = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        client.archive("src/SmallFile.fits")
        status = client.clone("", diskId, -1)
        waitReqCompl(client, status.getRequestId())

        # Remove the cloned disk (execute=0), should be successfull.
        status = client.remDisk(diskId, 0)
        refStatFile = "ref/ngamsRemDiskCmdTest_test_RemDiskDisk_1_1_ref"
        tmpStatFile = "tmp/ngamsRemDiskCmdTest_test_RemDiskDisk_1_1_tmp"
        saveInFile(tmpStatFile, filterDbStatus1(status.dumpBuf(0, 1, 1)))
        self.checkFilesEq(refStatFile, tmpStatFile, 
                          "Incorrect status for REMDISK Command/no execution")

        # Remove the cloned disk (execute=1), should be successfull.
        status = client.remDisk(diskId, 1)
        refStatFile = "ref/ngamsRemDiskCmdTest_test_RemDiskDisk_1_2_ref"
        tmpStatFile = "tmp/ngamsRemDiskCmdTest_test_RemDiskDisk_1_2_tmp"
        saveInFile(tmpStatFile, filterDbStatus1(status.dumpBuf(0, 1, 1)))
        self.checkFilesEq(refStatFile, tmpStatFile, 
                          "Incorrect status for REMDISK Command/execution")


    def test_RemDiskCmd_2(self):
        """
        Synopsis:
        Missing file copies, REMDISK Command rejected.
        
        Description:
        If there are not enough copies of files stored on a disk requested
        to be REMDISK'ed, the server should reject the request indicating
        the problem in the response.

        If an Email Notification List has been specified, the files in
        question, should be reported via email.

        Expected Result:
        The REMDISK Command should be rejected by the server, which should
        detect that there are files on the disk requested to be REMDISK'ed,
        which are not available in the system in 3 independent copies.

        Test Steps:
        - Start server.
        - Archive file.
        - Submit REMDISK to remove Main Disk hosting the archived file
          (execute=0).
        - Check on the response that the command is rejected by the server.
        - Submit REMDISK to remove Main Disk hosting the archived file
          (execute=1).
        - Check on the response that the command is rejected by the server.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)
        client.archive("src/SmallFile.fits")
        
        # Remove the cloned disk (execute=0), should fail.
        diskId = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        status = client.remDisk(diskId, 0)
        refStatFile = "ref/ngamsRemDiskCmdTest_test_RemDiskDisk_2_1_ref"
        tmpStatFile = "tmp/ngamsRemDiskCmdTest_test_RemDiskDisk_2_1_tmp"
        saveInFile(tmpStatFile, filterDbStatus1(status.dumpBuf(0, 1, 1)))
        self.checkFilesEq(refStatFile, tmpStatFile, 
                          "Incorrect status for REMDISK Command/no execution")

        # Remove the cloned disk (execute=1), should fail.
        status = client.remDisk(diskId, 1)
        refStatFile = "ref/ngamsRemDiskCmdTest_test_RemDiskDisk_2_2_ref"
        tmpStatFile = "tmp/ngamsRemDiskCmdTest_test_RemDiskDisk_2_2_tmp"
        saveInFile(tmpStatFile, filterDbStatus1(status.dumpBuf(0, 1, 1)))
        self.checkFilesEq(refStatFile, tmpStatFile, 
                          "Incorrect status for REMDISK Command/execution")


    def test_query_plan_exec_0_1(self):
        """
        Synopsis:
        Check SQL query plan for a REMDISK?execute=0/normal case.
        
        Description:
        The purpose of the test is to verify that the SQL query plan is
        as expected for the nominal case of the REMDISK Command when
        execute=0.

        Expected Result:
        The RemDisk Request should be analyzed by the server and the REMDISK
        Command accepted for execution.

        Test Steps:
        - Start NG/AMS Server (log level=5).
        - Archive 5 files to server.
        - Clone disk hosting archived files/wait for request to complete.
        - Execute REMDISK Command on cloned/target disk/execute=0.
        - Bring server to Offline.
        - Extract query plan for the request.
        - Compare query plan with reference query plan.

        Remarks:
        ...

        Test Data:
        TODO: Implement mechanism in ngamsGenTestReport to insert a file
              containing test data preceeded with a heading.
        """
        self.prepExtSrv(test=0)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)
        for n in range(5): client.archive("src/SmallFile.fits")
        client.clone("", "tmp-ngamsTest-NGAS-FitsStorage1-Main-1", -1)
        client.remDisk("tmp-ngamsTest-NGAS-FitsStorage2-Main-3", execute=0)
        client.offline()
        refQueryPlan = "ref/ngamsRemDiskCmdTest_test_query_plan_exec_0_1_1.ref"
        logFile = "/tmp/ngamsTest/NGAS/log/LogFile.nglog"
        threadId = getThreadId(logFile, ["REMDISK", "HTTP"])
        self.checkQueryPlanLogFile(logFile, threadId, refQueryPlan)


    def test_query_plan_exec_1_1(self):
        """
        Synopsis:
        Check SQL query plan for a REMDISK?execute=1/normal case.
        
        Description:
        The purpose of the test is to verify that the SQL query plan is
        as expected for the nominal case of the REMDISK Command when
        execute=1.

        Expected Result:
        The RemDisk Request should be analyzed by the server and the REMDISK
        Command accepted for execution and executed.

        Test Steps:
        - Start NG/AMS Server (log level=5).
        - Archive 5 files to server.
        - Clone disk hosting archived files/wait for request to complete.
        - Execute REMDISK Command on cloned/target disk/execute=1.
        - Bring server to Offline.
        - Extract query plan for the request.
        - Compare query plan with reference query plan.

        Remarks:
        ...
        
        Test Data:
        ...
        """
        self.prepExtSrv(test=0)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)
        for n in range(5): client.archive("src/SmallFile.fits")
        client.clone("", "tmp-ngamsTest-NGAS-FitsStorage1-Main-1", -1)
        client.remDisk("tmp-ngamsTest-NGAS-FitsStorage2-Main-3", execute=1)
        client.offline()
        refQueryPlan = "ref/ngamsRemDiskCmdTest_test_query_plan_exec_1_1_1.ref"
        logFile = "/tmp/ngamsTest/NGAS/log/LogFile.nglog"
        threadId = getThreadId(logFile, ["REMDISK", "HTTP"])
        self.checkQueryPlanLogFile(logFile, threadId, refQueryPlan)


    def test_ProxyMode_01(self):
        """
        Synopsis:
        Test that the proxy mode is not possible for the REMDISK Command.
        
        Description:
        It is not possible to let a contacted NGAS Node act as proxy for the
        REMDISK Command (for security reasons). I.e., the node where data
        should be removed, must be contacted directly/explicitly.

        Expected Result:
        When issuing the REMDISK Command to the cluster master node, it
        should figure out that the disk specified is located in a sub-node
        and the request should be rejected.

        Test Steps:
        - Start simulated cluster with 1 MNU + 1 NCU.
        - Issue a REMDISK Command to the NMU specifying a disk in the NCU
          (execute=0).
        - Verify that the mistake is detected and the request rejected.
        - Issue a REMDISK Command to the NMU specifying a disk in the NCU
          (execute=1).
        - Verify that the mistake is detected and the request rejected.

        Remarks:
        ...

        Test Data:
        ...
        """
        self.prepCluster("src/ngamsCfg.xml",
                         [[8000, None, None, getClusterName()],
                          [8011, None, None, getClusterName()]])
        diskId  = "tmp-ngamsTest-NGAS:8011-FitsStorage1-Main-1"
        for execute in [0, 1]:
            httpPars=[["disk_id", diskId], ["execute", execute]]
            tmpStatFile = sendExtCmd(getHostName(), 8000, NGAMS_REMDISK_CMD,
                                     pars=httpPars)
            refStatFile = "ref/ngamsRemDiskCmdTest_test_ProxyMode_01_01_ref"
            self.checkFilesEq(refStatFile, tmpStatFile,
                              "Incorrect handling of REMDISK Command detected")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsRemDiskCmdTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
