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
# "@(#) $Id: ngamsDiscardCmdTest.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  02/04/2004  Created
#

"""
This module contains the Test Suite for the SUBSCRIBE Command.
"""

import os, sys
from   ngams import *
from   ngamsTestLib import *
import ngamsPClient


try:
    illStatDoc  = "Incorrect info in DISCARD Command XML Status Document/%s."
    srcFitsFile = ngamsGetSrcDir() + "/ngamsTest/src/SmallFile.fits"
except:
    pass

class ngamsDiscardCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Test the DISCARD Command.

    Description:
    The purpose of the Test Suite is to test the DISCARD Command.

    Missing Test Cases:
      - Should be reviewed and missing Test Cases added. In particular the
        abnormal functioning and misusage should be better tested.

       - Remove file available in 3 copies (execute=0,1/Path).
       - Issue command specifying no parameters.
    """

    def test_NonExistingFile_1(self):
        """
        Synopsis:
        File not existing +/-execute/Disk ID/File ID/File Version.
        
        Description:
        The purpose of the test is to verify the behavior of the DISCARD
        Command, when it is tried to DISCARD a file, which is not available
        on the contacted NGAS Node. This is both tried for execute=0 and
        execute=1.

        The file is referred to by its File ID.

        Expected Result:
        The NG/AMS Server should detect that the specified file is not
        found on the contacted NGAS Node and should generate an error message
        indicating this.

        Test Steps:
        - Start NG/AMS Server.
        - Submit DISCARD Command, specifying a non-existing file/execute=0.
        - Check that the error message from the NG/AMS Server is as expected.
        - Submit DISCARD Command, specifying a non-existing file/execute=1.
        - Check that the error message from the NG/AMS Server is as expected.

        Remarks:
        ...       
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        mDiskId = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        pars = [["disk_id", mDiskId], ["file_id", "NonExistingFileId"],
                ["file_version", "1"]]
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_DISCARD_CMD,
                                 pars + [["execute", "0"]])
        refStatFile = "ref/ngamsDiscardCmdTest_test_NonExistingFile_1_1_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, illStatDoc % "1")
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_DISCARD_CMD,
                                 pars + [["execute", "1"]])
        self.checkFilesEq(refStatFile, tmpStatFile, illStatDoc % "2")


    def test_NonExistingFile_2(self):
        """
        Synopsis:
        File not existing referred to by path, +/- execute.
        
        Description:
        The purpose of the test is to verify the behavior of the DISCARD
        Command when it is tried to DISCARD a file referred to by it path
        name.

        Expected Result:
        The NG/AMS Server should detect that the file referred to by a path
        name is not available on the contacted NGAS Node and should send back
        an Error Response. This goes both when execute=0 and execute=1.

        Test Steps:
        - Start NG/AMS Server.
        - Submit a DISCARD Command, specifying a non-existing file path
          (execute=0).
        - Check that the command is rejected by NG/AMS.
        - Submit a DISCARD Command, specifying a non-existing file path
          (execute=0).
        - Check that the command is rejected by NG/AMS.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        pars = [["path", "/tmp/ngamsTest/NonExisting"]]
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_DISCARD_CMD,
                                 pars + [["execute", "0"]])
        refStatFile = "ref/ngamsDiscardCmdTest_test_NonExistingFile_2_1_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, illStatDoc % "1")
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_DISCARD_CMD,
                                 pars + [["execute", "1"]])
        self.checkFilesEq(refStatFile, tmpStatFile, illStatDoc % "2")

        
    def test_NormalExec_1(self):
        """
        Synopsis:
        File existing execute=0,1/Disk ID/File ID/File Version.
        
        Description:
        The purpose of the test is to  verify that the normal functioning
        of the DISCARD Command when it is tried to DISCARD a file referred to
        by its corresponding Disk ID, File ID and File Version.

        Expected Result:
        The server should find the file, and should return a positive response
        when issuing the DISCARD Command with execute=0. Subsequently, when
        submitting the command with execute=1, the file should disappear
        from the NGAS DB and from the disk.

        Test Steps:
        - Start NG/AMS Server.
        - Archive a file.
        - Issue a DISCARD Command specifying the Disk ID/File ID/File Version
          of the file/execute=0.
        - Check that the response from the NG/AMS Server reports that only
          one copy is available of the file.
        - Issue a DISCARD Command specifying the Disk ID/File ID/File Version
          of the file/execute=1.
        - Check that the response from the NG/AMS Server reports that the file
          was DISCARDED.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        ngamsPClient.ngamsPClient(getHostName(), 8888).pushFile(srcFitsFile)
        mDiskId = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        pars = [["disk_id", mDiskId],
                ["file_id", "TEST.2001-05-08T15:25:00.123"],
                ["file_version", "1"]]
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_DISCARD_CMD,
                                 pars + [["execute", "0"]])
        refStatFile = "ref/ngamsDiscardCmdTest_test_NormalExec_1_1_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, illStatDoc % "1")
        info(1,"TODO!: Check that file info is not removed from the DB")
        info(1,"TODO!: Check that file is not removed from the disk")
        refStatFile = "ref/ngamsDiscardCmdTest_test_NormalExec_1_2_ref"
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_DISCARD_CMD,
                                 pars + [["execute", "1"]])
        self.checkFilesEq(refStatFile, tmpStatFile, illStatDoc % "2")
        info(1,"TODO!: Check that file info is removed from the DB")
        info(1,"TODO!: Check that file is removed from the disk")


    def test_NormalExec_2(self):
        """
        Synopsis:
        File existing, remove via path, +/-execute.
        
        Description:
        The purpose of the test is to verify the normal functioning of the
        DISCARD Command when DISCARD'ing files referred to by their path.
        It is tried both to execute the command with execute=0 and execute=1.

        The file is not registered in the NGAS DB.

        This simulates the situation where spurious files are removed from the
        NGAS system only referred to by their path.

        Expected Result:
        When the DISCARD Command is issued, execute=0, the NG/AMS Server should
        find the file and return a response indicating that the file is only
        available in 1 copy. When submitting the DISCARD Command with
        execute=1, the file should disappear from the disk.

        Test Steps:
        - Start NG/AMS Server.
        - Copy a test file onto a disk of the NGAS System.
        - Issue a DISCARD Command specifying the copied file (execute=0).
        - Check that the response indicates that the DISCARD Request is
          granted.
        - Reissue the DISCARD Command specifying the copied file (execute=1).
        - Check that the response indicates that the file has been removed.
        - Check that the file has disappeared from the disk.
        
        Remarks:
        TODO!: Implement check to verify that file has been removed from the
               disk.
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        trgFile = "/tmp/ngamsTest/NGAS/FitsStorage3-Main-5/saf/SmallFile.fits"
        cpFile(srcFitsFile, trgFile)
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_DISCARD_CMD,
                                 [["path", trgFile], ["execute", "0"]])
        refStatFile = "ref/ngamsDiscardCmdTest_test_NormalExec_2_1_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, illStatDoc % "1")
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_DISCARD_CMD,
                                 [["path", trgFile], ["execute", "1"]])
        refStatFile = "ref/ngamsDiscardCmdTest_test_NormalExec_2_2_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, illStatDoc % "2")
        info(1,"TODO!: Check that file disappeared from the disk!")



    def test_NormalExec_3(self):
        """
        Synopsis:
        File existing/3 copies execute=0,1/Disk ID/File ID/File Version.
        
        Description:
        The purpose of the test is to  verify that the normal functioning
        of the DISCARD Command when it is tried to DISCARD a file referred to
        by its corresponding Disk ID, File ID and File Version. The file is
        available in three copies.

        Expected Result:
        The server should find the file, and should return a positive response
        when issuing the DISCARD Command with execute=0. Subsequently, when
        submitting the command with execute=1, the file should disappear
        from the NGAS DB and from the disk.

        Test Steps:
        - Start NG/AMS Server.
        - Archive a file.
        - Clone the file onto another disk.
        - Issue a DISCARD Command specifying the Disk ID/File ID/File Version
          of the file/execute=0.
        - Check that the response from the NG/AMS Server reports that only
          one copy is available of the file.
        - Issue a DISCARD Command specifying the Disk ID/File ID/File Version
          of the file/execute=1.
        - Check that the response from the NG/AMS Server reports that the file
          was DISCARDED.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        sendPclCmd().archive("src/SmallFile.fits")
        mDiskId = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        fileId  = "TEST.2001-05-08T15:25:00.123"
        sendPclCmd().clone(fileId, mDiskId, 1)
        for execute in [0, 1]:
            httpPars = [["disk_id", mDiskId], ["file_id", fileId],
                        ["file_version", "1"], ["execute", execute]]
            tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_DISCARD_CMD,
                                     pars=httpPars)
            refStatFile = "ref/ngamsDiscardCmdTest_test_NormalExec_3_%d_ref"%\
                          (execute + 1)
            self.checkFilesEq(refStatFile, tmpStatFile, "Unexpected result "+\
                              "of DISCARD Command (execute=%d)" % execute)


    def test_IllegalPars_1(self):
        """
        Synopsis:
        Missing parameters 1) Disk ID, 2) File ID, 3) File Version missing.
        
        Description:
        The purpose of the test is to verify the error handling when an
        illegal combination of parameters is submitted to the NG/AMS Server
        with the DISCARD Command.

        It is only allowed to execute the DISCARD Command referring to
        Disk ID/File ID/File Version if all three parameters are specified.

        Expected Result:
        When submitting the DISCARD Command with either of the 3 parameters
        missing, the command should be rejected and an Error Response returned
        indicating the problem.

        Test Steps:
        - Start NG/AMS Server.
        - Submit DISCARD Command with File ID/File Version.
        - Check that the request is rejected, indicating that the Disk ID is
          missing.
        - Submit DISCARD Command with Disk ID/File Version.
        - Check that the request is rejected, indicating that the File ID
          is missing.
        - Submit DISCARD Command with Disk ID/File ID.
        - Check that the request is rejected, indicating that the File Version
          is missing.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        # Disk ID Missing:
        pars = [["file_id", "FileID"], ["file_version", "1"]]
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_DISCARD_CMD, pars)
        refStatFile = "ref/ngamsDiscardCmdTest_test_IllegalPars_1_1_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, illStatDoc % "1")
        # File ID Missing:
        pars = [["disk_id", "DiskID"], ["file_version", "1"]]
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_DISCARD_CMD, pars)
        refStatFile = "ref/ngamsDiscardCmdTest_test_IllegalPars_1_2_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, illStatDoc % "2")
        # File Version Missing:
        pars = [["file_id", "FileID"], ["disk_id", "DiskId"]]
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_DISCARD_CMD, pars)
        refStatFile = "ref/ngamsDiscardCmdTest_test_IllegalPars_1_3_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, illStatDoc % "3")


    def test_ProxyMode_01(self):
        """
        Synopsis:
        Test that the proxy mode is not possible for the DISCARD Command.
        
        Description:
        It is not possible to let a contacted NGAS Node act as proxy for the
        DISCARD Command (for security reasons). I.e., the node where data
        should be removed, must be contacted directly/explicitly.

        Expected Result:
        When issuing the DISCARD Command to the cluster master node, it
        should figure out that the file specified is stored on a sub-node
        and the request should be rejected.

        Test Steps:
        - Start simulated cluster with 1 MNU + 1 NCU.
        - Archive file onto the NCU.
        - Archive file onto the NMU.
        - Issue a DISCARD Command to the NMU specifying the file on the NCU
          (execute=0).
        - Verify that the mistake is detected and the request rejected.
        - Issue a DISCARD Command to the NMU specifying the file on the NCU
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
        for execute in [0, 1]:
            httpPars=[["disk_id", diskId], ["file_id", fileId],
                      ["file_version", fileVer], ["execute", execute]]
            tmpStatFile = sendExtCmd(getHostName(), 8000, NGAMS_DISCARD_CMD,
                                     pars=httpPars)
            refStatFile = "ref/ngamsDiscardCmdTest_test_ProxyMode_01_01_ref"
            self.checkFilesEq(refStatFile, tmpStatFile,
                              "Incorrect handling of DISCARD Command detected")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsDiscardCmdTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
