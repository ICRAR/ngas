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
# "@(#) $Id: ngamsRemFileCmdTest.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/11/2003  Created
#

"""
This module contains the Test Suite for the REMFILE Command.
"""

import os, sys
from   ngams import *
import ngamsFileInfo
from   ngamsTestLib import *


class ngamsRemFileCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Handling of REMFILE Command.

    Description:
    The purpose of the Test Suite is to exercise the REMFILE Command under
    normal and abnormal conditions. In particular it is verified that it is
    not possible to REMFILE files, which are available in less than 3 copies
    in the archive.

    Missing Test Cases:
    - The Test Suite should be reviewed and important, missing Test Cases
      added. In particular.
    - Test Cases for abnormal behavior should be added.
    - DB info can be removed, file info not.
    """

    def test_RemFileCmd_1(self):
        """
        Synopsis:
        Test normal execution of the REMFILE Command.
        
        Description:
        Test the normal execution of the REMFILE Command, whereby a file is
        requested to the REMFILE'd which is available in at least 3 copies.

        Expected Result:
        The server should identify that the file requested to be removed is
        available in at least 3 copies and should accept the request.

        Test Steps:
        - Start server.
        - Archive file.
        - Clone Main Disk hosting archived file.
        - Submit REMFILE Command requesting to remove cloned file (execute=0).
        - Check on response from server that the request has been accepted.
        - Submit REMFILE Command requesting to remove cloned file (execute=1).
        - Check on response from server that the request has been accepted.

        Remarks:
        TODO!: It is not checked that the info for the file is actually
               removed from the DB and from the disk.
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)

        # Archive a file + clone it to be able to execute the REMFILE Command.
        client.archive("src/SmallFile.fits")
        fileId = "TEST.2001-05-08T15:25:00.123"
        diskId = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        status = client.clone(fileId, diskId, 1)
        waitReqCompl(client, status.getRequestId())

        # Remove the cloned file (execute=0), should be successfull.
        status = client.remFile(diskId, fileId, 1, 0)
        refStatFile = "ref/ngamsRemFileCmdTest_test_RemFileCmd_1_1_ref"
        tmpStatFile = "tmp/ngamsRemFileCmdTest_test_RemFileCmd_1_1_tmp"
        saveInFile(tmpStatFile, filterDbStatus1(status.dumpBuf(0, 1, 1)))
        self.checkFilesEq(refStatFile, tmpStatFile, 
                          "Incorrect status for REMFILE Command/no execution")

        # Remove the cloned file (execute=1), should be successfull.
        status = client.remFile(diskId, fileId, 1, 1)
        refStatFile = "ref/ngamsRemFileCmdTest_test_RemFileCmd_1_2_ref"
        tmpStatFile = "tmp/ngamsRemFileCmdTest_test_RemFileCmd_1_2_tmp"
        saveInFile(tmpStatFile, filterDbStatus1(status.dumpBuf(0, 1, 1)))
        self.checkFilesEq(refStatFile, tmpStatFile, 
                          "Incorrect status for REMFILE Command/execution")


    def test_RemFileCmd_2(self):
        """
        Synopsis:
        Missing file copies, REMFILE Command rejected.
        
        Description:
        The purpose of the Test Case is to verify that the REMFILE Command
        is rejected when it is attempted to remove files available in less
        than 3 copies in the archive. This both when specifying execute=0 and
        execute=1.

        Expected Result:
        The server should detect that the file requested to be REMFILE'd is
        not available on the system in at least 3 independent copies.

        Test Steps:
        - Start server.
        - Archive file.
        - Submit REMFILE Command to remove copy of file on Main Disk
          (execute=0).
        - Check on response from the server that the request was rejected.
        - Submit REMFILE Command to remove copy of file on Main Disk
          (execute=1).
        - Check on response from the server that the request was rejected.

        Remarks:
        TODO!: It is not check if an Email Notification is sent out listing
               files, which could not be removed because they were available
               in less than 3 copies.
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)

        # Archive file.
        client.archive("src/SmallFile.fits")

        # Remove the archived file (execute=0), should fail.
        fileId = "TEST.2001-05-08T15:25:00.123"
        diskId = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        status = client.remFile(diskId, fileId, 1, 0)
        refStatFile = "ref/ngamsRemFileCmdTest_test_RemFileCmd_2_1_ref"
        tmpStatFile = "tmp/ngamsRemFileCmdTest_test_RemFileCmd_2_1_tmp"
        saveInFile(tmpStatFile, filterDbStatus1(status.dumpBuf(0, 1, 1)))
        self.checkFilesEq(refStatFile, tmpStatFile, 
                          "Incorrect status for REMFILE Command/no execution")

        # Remove the cloned file (execute=1), should fail.
        status = client.remFile(diskId, fileId, 1, 1)
        refStatFile = "ref/ngamsRemFileCmdTest_test_RemFileCmd_2_2_ref"
        tmpStatFile = "tmp/ngamsRemFileCmdTest_test_RemFileCmd_2_2_tmp"
        saveInFile(tmpStatFile, filterDbStatus1(status.dumpBuf(0, 1, 1)))
        self.checkFilesEq(refStatFile, tmpStatFile, 
                          "Incorrect status for REMFILE Command/execution")


    def test_ProxyMode_01(self):
        """
        Synopsis:
        Test that the proxy mode is not possible for the REMFILE Command.
        
        Description:
        It is not possible to let a contacted NGAS Node act as proxy for the
        REMFILE Command (for security reasons). I.e., the node where data
        should be removed, must be contacted directly/explicitly.

        Expected Result:
        When issuing the REMFILE Command to the cluster master node, it
        should figure out that the file specified is stored on a sub-node
        and the request should be rejected.

        Test Steps:
        - Start simulated cluster with 1 MNU + 1 NCU.
        - Archive file onto the NMU.
        - Archive file onto the NCU.
        - Clone file onto another within the NCU.
        - Issue a REMFILE Command to the NMU specifying the file on the NCU
          (execute=0).
        - Verify that the mistake is detected and the request rejected.
        - Issue a REMFILE Command to the NMU specifying the file on the NCU
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
        sendPclCmd(port=8000).archive("src/SmallFile.fits")
        stat = sendPclCmd(port=8011).archive("src/SmallFile.fits")
        diskId  = "tmp-ngamsTest-NGAS:8011-FitsStorage1-Main-1"
        fileId  = "TEST.2001-05-08T15:25:00.123"
        fileVer = 2
        sendPclCmd(port=8011).clone(fileId, diskId, fileVer)
        for execute in [0, 1]:
            httpPars=[["disk_id", diskId], ["file_id", fileId],
                      ["file_version", fileVer], ["execute", execute]]
            tmpStatFile = sendExtCmd(getHostName(), 8000, NGAMS_REMFILE_CMD,
                                     pars=httpPars)
            refStatFile = "ref/ngamsRemFileCmdTest_test_ProxyMode_01_01_ref"
            self.checkFilesEq(refStatFile, tmpStatFile,
                              "Incorrect handling of REMFILE Command detected")


    def test_FileVerHandling_01(self):
        """
        Synopsis:
        Test that REMFILE removes proper File Version.
        
        Description:
        The purpose of the test is to verify that if several versions of a
        file are found in the archive, the proper version is deleted.

        Expected Result:
        The chosen version of the file, should be selected and should be
        removed.

        Test Steps:
        - Start server.
        - Archive a file 3 times into the archive.
        - Clone the file onto another disk.
        - Remove version 2 of the cloned file.

        Remarks:
        ...

        Test Data:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        for n in range(3): stat = sendPclCmd().archive("src/SmallFile.fits")
        diskId1 = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        fileId  = "TEST.2001-05-08T15:25:00.123"
        fileVer = 2
        sendPclCmd().sendCmd(NGAMS_CLONE_CMD, pars=[["disk_id", diskId1]])
        diskId2 = "tmp-ngamsTest-NGAS-FitsStorage2-Main-3"
        filePath = "/tmp/ngamsTest/NGAS/FitsStorage2-Main-3/saf/" +\
                   "2001-05-08/2/TEST.2001-05-08T15:25:00.123.fits.Z"
        for execute in [0, 1]:
            httpPars=[["disk_id", diskId2], ["file_id", fileId],
                      ["file_version", fileVer], ["execute", execute]]
            tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_REMFILE_CMD,
                                     pars=httpPars)
            refStatFile = "ref/ngamsRemFileCmdTest_test_" +\
                          "FileVerHandling_01_0%d_ref" % (execute + 1)
            self.checkFilesEq(refStatFile, tmpStatFile,
                              "Incorrect handling of REMFILE Command detected")
            fileInfo = ngamsFileInfo.ngamsFileInfo()
            try:
                fileInfo.read(dbObj, fileId, fileVer, diskId2)
            except:
                pass
            if (execute == 0):
                if (not os.path.exists(filePath)):
                    self.fail("File removed unexpectedly")
            else:
                if (os.path.exists(filePath)):
                    self.fail("File not removed as expected")
            refStatFile = "ref/ngamsRemFileCmdTest_test_" +\
                          "FileVerHandling_01_0%d_ref" % (execute + 3)
            tmpStatFile = saveInFile(None, filterDbStatus1(fileInfo.dumpBuf()))
            self.checkFilesEq(refStatFile, tmpStatFile, 
                              "Incorrect status for REMFILE Command/execution")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsRemFileCmdTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
