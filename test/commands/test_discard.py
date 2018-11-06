#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
#    Copyright by UWA (in the framework of the ICRAR)
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
import os

from ngamsLib.ngamsCore import NGAMS_DISCARD_CMD, checkCreatePath

from ..ngamsTestLib import ngamsTestSuite


illStatDoc  = "Incorrect info in DISCARD Command XML Status Document/%d."

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

    def _assert_discard(self, ref_file, pars, client=None, port=None):
        client = client or self.client
        for execute in (0, 1):
            status = client.get_status(NGAMS_DISCARD_CMD, pars + [["execute", str(execute)]])
            if '%d' in ref_file:
                _ref_file = ref_file % (execute + 1)
            else:
                _ref_file = ref_file
            self.assert_status_ref_file(_ref_file, status, msg=illStatDoc % execute,
                                        port=port)


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
        self.prepExtSrv()
        mDiskId = self.ngas_disk_id("FitsStorage1/Main/1")
        pars = [["disk_id", mDiskId], ["file_id", 'NonExistingFileId'],
                ["file_version", "1"]]
        ref_file = "ref/ngamsDiscardCmdTest_test_NonExistingFile_1_1_ref"
        self._assert_discard(ref_file, pars)


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
        self.prepExtSrv()
        pars = [["path", "/tmp/ngamsTest/NonExisting"]]
        ref_file = "ref/ngamsDiscardCmdTest_test_NonExistingFile_2_1_ref"
        self._assert_discard(ref_file, pars)


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
        self.prepExtSrv()
        self.archive("src/SmallFile.fits")
        mDiskId = self.ngas_disk_id("FitsStorage1/Main/1")
        pars = [["disk_id", mDiskId],
                ["file_id", "TEST.2001-05-08T15:25:00.123"],
                ["file_version", "1"]]
        ref_file = "ref/ngamsDiscardCmdTest_test_NormalExec_1_%d_ref"
        self._assert_discard(ref_file, pars)

        # TODO!: Check that file info is removed/not removed from DB and disk

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
        self.prepExtSrv()
        trgFile = self.ngas_path("FitsStorage3-Main-5/saf/SmallFile.fits")
        checkCreatePath(os.path.dirname(trgFile))
        self.cp('src/SmallFile.fits', trgFile)
        pars = [["path", trgFile]]
        ref_file = "ref/ngamsDiscardCmdTest_test_NormalExec_2_%d_ref"
        self._assert_discard(ref_file, pars)
        # TODO!: Check that file disappeared from the disk!


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
        self.prepExtSrv()
        self.archive("src/SmallFile.fits")
        mDiskId = self.ngas_disk_id("FitsStorage1/Main/1")
        fileId  = "TEST.2001-05-08T15:25:00.123"
        self.client.clone(fileId, mDiskId, 1)
        ref_file = "ref/ngamsDiscardCmdTest_test_NormalExec_3_%d_ref"
        pars = [["disk_id", mDiskId], ["file_id", fileId], ["file_version", "1"]]
        self._assert_discard(ref_file, pars)


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
        self.prepExtSrv()

        # disk ID, file ID and file version are missing on each corresopnding row
        invalid_params = [
            [["file_id", "FileID"], ["file_version", "1"]],
            [["disk_id", "DiskID"], ["file_version", "1"]],
            [["file_id", "FileID"], ["disk_id", "DiskId"]]
        ]
        ref_file = "ref/ngamsDiscardCmdTest_test_IllegalPars_1_%d_ref"
        for i, pars in enumerate(invalid_params, start=1):
            status = self.client.get_status(NGAMS_DISCARD_CMD, pars)
            self.assert_status_ref_file(ref_file % i, status, msg=illStatDoc)


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
        self.prepCluster((8000, 8011))
        self.archive(8000, "src/SmallFile.fits")
        self.archive(8011, "src/SmallFile.fits")

        diskId  = self.ngas_disk_id("FitsStorage1/Main/1", port=8011)
        ref_file = "ref/ngamsDiscardCmdTest_test_ProxyMode_01_01_ref"
        pars = [["disk_id", diskId], ["file_id", "TEST.2001-05-08T15:25:00.123"], ["file_version", 2]]
        self._assert_discard(ref_file, pars, client=self.client(8000))