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
# "@(#) $Id: ngasCheckFileCopiesTest.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  08/08/2005  Created
#

"""
This module contains the Test Suite for the NGAS Utilities Tool
ngasCheckFileCopies.
"""

import os, sys
from   ngams import *
import ngasUtils, ngasUtilsLib
from   ngamsTestLib import *
from   ngasUtilsTestLib import *

DISK_ID = "tmp-ngamsTest-NGAS:8001-FitsStorage1-Rep-2"


def _prepTestEnv(testSuiteObj):
    """
    Prepare the test environment.

    testSuiteObj:   Test suite object (ngamsTestSuite)

    Returns:        Void.
    """
    testSuiteObj.prepCluster(BASE_CFG_1,
                             [[8000, None, None, getClusterName()],
                              [8001, None, None, getClusterName()]])
    for n in range(10): sendPclCmd(port=8001).archive(TEST_FILE_1)
    sendPclCmd().sendCmdGen(getHostName(), 8000, NGAMS_CLONE_CMD,
                            pars=[["disk_id", DISK_ID]])


def _invokeCheckFileCopies(diskId,
                           recvNotifEmail = 1,
                           cfgFile = BASE_CFG_1,
                           accessCode = "X190ZXN0X18="):
    """
    Execute the ngasCheckFileCopies tool as a shell command.

    diskId:            ID of disk concerned (string).

    recvNotifEmail:    Receive email notification (integer/0|1).

    cfgFile:           Configuration file used for the test (string).

    accessCode:        Access code to use the NGAS Utilities (string).

    Returns:           Info written on stdout and notification email (tuple).
    """
    if (recvNotifEmail): flushEmailQueue()
    prepNgasResFile(cfgFile)
    cmd = "/opsw/packages/bin/ngasCheckFileCopies -diskId %s" % diskId
    stat, out = commands.getstatusoutput(cmd)
    out = out.replace(getHostName(), "localhost")
    if (recvNotifEmail):
        notifEmail = getEmailMsg()
        notifEmail = notifEmail.replace(getHostName(), "localhost")
    else:
        notifEmail = None
    return (stat, out, notifEmail)


def _verifyEmailAndStdout(testSuiteObj,
                          testName,
                          verifyEmail = 1,
                          cfgFile = BASE_CFG_1,
                          diskId = DISK_ID):
    """
    Invoke ngasCheckFileCopies and check that the results are as expected.

    testSuiteObj:   Test suite object (ngamsTestSuite)

    testName:       Name of the test case (string).

    verifyEmail:    Verify also the contents of the email notification mail
                    (integer/0|1).

    cfgFile:        Configuration file used for the test (string).

    Returns:        Void.
    """
    stat, out, notifEmail = _invokeCheckFileCopies(diskId, verifyEmail,cfgFile)
    refStatFile = "ref/ngasCheckFileCopiesTest_%s_1" % testName
    if (verifyEmail):
        tmpStatFile = saveInFile(None, notifEmail)
        testSuiteObj.checkFilesEq(refStatFile, tmpStatFile,
                                  "Incorrect/missing " +\
                                  "notification email msg")
    refStatFile = "ref/ngasCheckFilesCopies_%s_2" % testName
    year = PccUtTime.TimeStamp().getTimeStamp().split("-")[0].strip()
    out = filterOutLines(out, discardTags=["%s-" % year])
    tmpStatFile = saveInFile(None, out)
    testSuiteObj.checkFilesEq(refStatFile, tmpStatFile,
                              "Incorrect/missing info on stdout")


class ngasCheckFileCopiesTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite for the ngasCheckFileCopies Tool.

    Description:
    The Test Suite excersizes the ngasCheckFileCopies Tool and checks the
    behavior under normal and abnormal conditions.

    Missing Test Cases:
    ...
    """

    def test_NormalExec_1(self):
        """
        Synopsis:
        A normal run. No files found to have missing file copies.

        Description:
        The purpose of this tests, exercises the nominal case, where the tool
        is invoked on a list of files, which are all available in 3 copies or
        more.

        Expected Result:
        The tool should execute normally and should check that all files
        on the disk, are available in the system in at least 3 copies.

        Test Steps:
        - Start cluster (2 nodes).
        - Archive files onto system 1.
        - Clone the disk hosting the files onto system 2.
        - Invoke tool to check the mail disk onto which files where archived.
        - Check the output from ngasCheckFileCopies.

        Remarks:
        ...
        """
        _prepTestEnv(self)
        _verifyEmailAndStdout(self, "test_NormalExec_1")


    def test_NormalExec_2(self):
        """
        Synopsis:
        A normal run. Files found to have missing file copies + dubious files.

        Description:
        The purpose of the test is to verify that the tool detects when there
        are missing file copies/dubious files in connection with the disk
        checked.

        Expected Result:
        The tool should detect that there are files with too few copies and
        dubious files and should report this accordingly to the specified
        recipients.

        Test Steps:
        - Start server.
        - Archive file onto server.
        - Discard some of the files.
        - Set some files to ignore.
        - Set some files to checking.
        - Set some files bad.
        - Invoke ngasCheckFilesCopies Tool.
        - Verify that the Email Notification generated indicates that files
          were found, which have missing copies.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        for n in range(15): sendPclCmd(port=8888).archive(TEST_FILE_1)
        mainDiskId = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        repDiskId  = "tmp-ngamsTest-NGAS-FitsStorage1-Rep-2"
        fileId     = "TEST.2001-05-08T15:25:00.123"
        for fileVer in [1,3,11,14]:
            stat = sendPclCmd().sendCmdGen(getHostName(), 8888,
                                           NGAMS_DISCARD_CMD,
                                           pars=[["disk_id", repDiskId],
                                                 ["file_id", fileId],
                                                 ["file_version", fileVer],
                                                 ["execute", 1]])
        # Make some dubious files.
        for fileVer, fileStatus, fileIgnore in [[4,  "01000000", 0],
                                                [8,  "10000000", 1],
                                                [10, "11000000", 0],
                                                [13, "00000000", 1]]:
            dbObj.setFileStatus(fileId, fileVer, repDiskId, fileStatus)
            query = "UPDATE ngas_files SET file_ignore=%d WHERE disk_id='%s' " +\
                    "AND file_id='%s' AND file_version=%d"
            dbObj.query(query % (fileIgnore, repDiskId, fileId, fileVer))
        _verifyEmailAndStdout(self, "test_NormalExec_2", diskId=mainDiskId)


    def test_AbnormalExec_1(self):
        """
        Synopsis:
        Tool executed on non-existing Disk ID.

        Description:
        This test case exercise the situation where a Disk ID is given for a
        non-existing disk.

        Expected Result:
        An appropriate error message should be generated on the shell.

        Test Steps:
        - Invoke the tool giving a non-existing Disk ID.
        - Test the output on stderr.

        Remarks:
        ...
        """
        _verifyEmailAndStdout(self, "test_AbnormalExec_1",
                              verifyEmail=1, diskId="NON-EXISTING")




    def test_AbnormalExec_2(self):
        """
        Synopsis:
        ...

        Description:
        ...

        Expected Result:
        ...

        Test Steps:
        - ...

        Remarks:
        ...
        """
        pass


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngasCheckFileCopiesTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
