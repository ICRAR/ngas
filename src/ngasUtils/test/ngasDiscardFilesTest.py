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
# "@(#) $Id: ngasDiscardFilesTest.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  08/08/2005  Created
#

"""
This module contains the Test Suite for the NGAS Utilities Tool
ngasDiscardFiles.
"""

import os, sys, random
from   ngams import *
import ngasUtils, ngasUtilsLib
from   ngamsTestLib import *
from   ngasUtilsTestLib import *


SRC_DISK_ID = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"

FILE_ID_1 = "TEST.2001-05-08T15:25:00.123"

FILE_INFO_1 = [["tmp-ngamsTest-NGAS-FitsStorage1-Main-1", "2"],
               ["tmp-ngamsTest-NGAS-FitsStorage1-Main-1", "5"],
               ["tmp-ngamsTest-NGAS-FitsStorage1-Rep-2",  "3"],
               ["tmp-ngamsTest-NGAS-FitsStorage1-Rep-2",  "5"],
               ["tmp-ngamsTest-NGAS-FitsStorage1-Rep-2",  "8"]]


def _invokeDiscardFiles(fileList,
                        dccMsg,
                        execute,
                        recvNotifEmail = 1,
                        cfgFile = BASE_CFG_1,
                        accessCode = "__test__"):
    """
    Execute the ngasDiscardFiles tool as a shell command.

    fileList:          Name of file containing the file list (string|None).

    dccMsg:            Name of file containing the DCC message (string|None).

    execute:           If set to 1 specify the execute parameter (integer/0|1).

    recvNotifEmail:    Receive email notification (integer/0|1).

    cfgFile:           Configuration file used for the test (string).

    accessCode:        Access code to use the NGAS Utilities (string).

    Returns:           Info written on stdout and notification email (tuple).
    """
    if (recvNotifEmail): flushEmailQueue()
    prepNgasResFile(cfgFile, ngasPort=8888)
    cmd = "/opsw/packages/bin/ngasDiscardFiles -accessCode %s -%s %s "
    if (fileList):
        cmd = cmd % (accessCode, "fileList", fileList)
    else:
        cmd = cmd % (accessCode, "dccMsg", dccMsg)
    if (execute): cmd += " -execute"
    stat, out = commands.getstatusoutput(cmd)
    if (recvNotifEmail):
        notifEmail = getEmailMsg()
        notifEmail = notifEmail.replace(getHostName(), "localhost")
    else:
        notifEmail = None
    return (stat, out, notifEmail)


def _verifyEmailAndStdout(testSuiteObj,
                          testName,
                          fileList,
                          dccMsg,
                          execute,
                          verifyEmail = 1,
                          cfgFile = BASE_CFG_1):
    """
    Invoke ngasDiscardFiles and check that the results are as expected.

    testSuiteObj:   Test suite object (ngamsTestSuite)

    testName:       Name of the test case (string).

    fileList:       Name of file list or None (string|None).

    dccMsg:         Name of file hosting the DCC message (string|None).

    execute:        If set to 1 the command is executed (integer/0|1).

    verifyEmail:    Verify also the contents of the email notification mail
                    (integer/0|1).

    cfgFile:        Configuration file used for the test (string).

    Returns:        Void.
    """
    stat, out, notifEmail = _invokeDiscardFiles(fileList, dccMsg, execute,
                                                verifyEmail, cfgFile,
                                                accessCode="__test__")
    refStatFile = "ref/ngasDiscardFiles_%s_1" % testName
    out = out.replace(getHostName(), "localhost")
    tmpStatFile = saveInFile(None, out)
    testSuiteObj.checkFilesEq(refStatFile, tmpStatFile,
                              "Incorrect/missing info on stdout")
    if (verifyEmail):
        refStatFile = "ref/ngasDiscardFiles_%s_2" % testName
        tmpStatFile = saveInFile(None, notifEmail)
        testSuiteObj.checkFilesEq(refStatFile, tmpStatFile,
                                  "Incorrect/missing " +\
                                  "notification email msg")

def _checkFilesDiscarded(testObj,
                         fileInfo,
                         fileId,
                         dbObj):
    """
    Check that the files, which should have been discarded really have been
    removed from the DB and the disk.

    testObj:     Reference to test suite object (ngamsTestSuite).

    fileInfo:    List with sub-lists containing Disk ID and File Version
                 (list).

    fileId:      File ID used for the test (string).

    dbObj:       DB connection object (ngamsDb).

    Returns:     Void.
    """
    # Verify that files on disk are gone.
    for fi in fileInfo:
        basePath = fi[0].replace("tmp-ngamsTest-NGAS-",
                                 "/tmp/ngamsTest/NGAS/")
        path = "%s/saf/2001-05-08/%s/%s.fits.Z" % (basePath, fi[1], fileId)
        if (os.path.exists(path)):
            testObj.fail("Archive file: %s not removed after discard" % path)

    # Verify that the info in the DB is gone.
    if (dbObj):
        for fi in fileInfo:
            if (dbObj.fileInDb(fi[0], fileId, fi[1])):
                testObj.fail("Archive file: %s/%s/%s not removed from " +\
                             "DB after discard as expected" %\
                             (fi[0], fileId, fi[1]))

def _createFileList(fileInfo,
                    fileId = None):
    """
    Create a file list file with the info from the fileInfo parmeter.

    fileInfo:  List with information for the file list (list).

    fileId:    File ID if relevant (string|None).

    Returns:   Name of generated file list (string).
    """
    fileListBuf = ""
    for fi in fileInfo:
        if (len(fi) == 2):
            fileListBuf += "%s %s %s\n" % (fi[0], fileId, fi[1])
        else:
            fileListBuf += "%s\n" % fi
    return saveInFile(None, fileListBuf)


class ngasDiscardFilesTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite for the ngasDiscardFiles Tool.

    Description:
    The Test Suite exercises the ngasDiscardFiles Tool and checks the
    behavior under normal and abnormal conditions.

    Missing Test Cases:
    ...
    """

    def test_NormalExec_1(self):
        """
        Synopsis:
        Normal execution of the tool, files are discarded (Disk ID, File ID,
        File Version).

        Description:
        The test should verify that the standard way of usage works as it
        should. A list of files is referenced to by their Disk ID, File ID
        and File Version and are discarded. The tool is execute with/without
        execute=1.

        Expected Result:
        After the first invocation, the tool should produce a report indicating
        the status. When invoking it again, the files in the list should
        disappear.

        Test Steps:
        - Start server.
        - Archive a set of files on the server.
        - Invoke the tool to delete some of these files, execute=0.
        - Check output produced by the tool.
        - Invoke the tool to delete some of these files, execute=1.
        - Check output produced by the tool (stdout + email).
        - Check that the specified files disappeared.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        for n in range(10): sendPclCmd(port=8888).archive(TEST_FILE_1)
        sendPclCmd().sendCmdGen(getHostName(), 8888, NGAMS_CLONE_CMD,
                                pars=[["disk_id", SRC_DISK_ID]])
        fileList = _createFileList(FILE_INFO_1, FILE_ID_1)
        # Invoke with execute=0.
        execute = 0
        _verifyEmailAndStdout(self, "test_NormalExec_1_1", fileList, None,
                              execute)
        # Invoke with execute=1.
        execute = 1
        _verifyEmailAndStdout(self, "test_NormalExec_1_2", fileList, None,
                              execute)
        # Check that files + DB info is gone.
        _checkFilesDiscarded(self, FILE_INFO_1, FILE_ID_1, dbObj)


    def test_NormalExec_2(self):
        """
        Synopsis:
        Normal execution of the tool, files are discarded (Disk ID, File ID,
        File Version). Not all files in three copies + file specified not
        found.

        Description:
        The test should verify that the standard way of usage works as it
        should. A list of files is referenced to by their Disk ID, File ID
        and File Version and are discarded. The tool is execute with/without
        execute=1.

        Expected Result:
        After the first invocation, the tool should produce a

        Test Steps:
        - Start server.
        - Archive a set of files on the server.
        - Remove some files with the DISCARD Command.
        - Invoke the tool to delete some of these files, execute=0.
        - Check output produced by the tool.
        - Invoke the tool to delete some of these files, execute=1.
        - Check output produced by the tool (stdout + email).
        - Check that the specified files disappeared.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        for n in range(10): sendPclCmd(port=8888).archive(TEST_FILE_1)
        stat = sendPclCmd().sendCmdGen(getHostName(), 8888, NGAMS_CLONE_CMD,
                                       pars=[["disk_id", SRC_DISK_ID]])
        diskId = "tmp-ngamsTest-NGAS-FitsStorage1-Rep-2"
        for fileVer in [1, 3, 7]:
            stat = sendPclCmd().sendCmdGen(getHostName(), 8888,
                                           NGAMS_DISCARD_CMD,
                                           pars=[["disk_id", diskId],
                                                 ["file_id", FILE_ID_1],
                                                 ["file_version", fileVer],
                                                 ["execute", 1]])
        fileList = _createFileList(FILE_INFO_1, FILE_ID_1)
        # Invoke with execute=0.
        execute = 0
        _verifyEmailAndStdout(self, "test_NormalExec_2_1", fileList, None,
                              execute)
        # Invoke with execute=1.
        execute = 1
        _verifyEmailAndStdout(self, "test_NormalExec_2_2", fileList, None,
                              execute)
        # Check that files + DB info is gone.
        _checkFilesDiscarded(self, FILE_INFO_1, FILE_ID_1, dbObj)


    def test_NormalExec_3(self):
        """
        Synopsis:
        Normal execution of the tool, files are discarded (complete path
        given).

        Description:
        The purpose of the test is to verify that files specified by their
        path, can be removed from the system. It is also tested that if a file
        is not found, the tool continues to execute.

        Expected Result:
        The files specified should be moved as expected.

        Test Steps:
        - Start server.
        - Archive some files, clone them.
        - Create some spurious files.
        - Create a file list with files referred to by their complete path.
          Some of the files do not exist.
        - Invoke ngasDiscardFiles, execute=0, check outputs.
        - Invoke ngasDiscardFiles, execute=1, check outputs.
        - Check that the files are gone.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        for n in range(10): sendPclCmd(port=8888).archive(TEST_FILE_1)
        stat = sendPclCmd().sendCmdGen(getHostName(), 8888, NGAMS_CLONE_CMD,
                                       pars=[["disk_id", SRC_DISK_ID]])
        basePath = "/tmp/ngamsTest/NGAS/"
        files1 = ["FitsStorage1-Main-1/ShouldNotBeHere1",
                  "FitsStorage1-Main-1/.db/ShouldNotBeHere2",
                  "FitsStorage1-Main-1/saf/2001-05-08/ShouldNotBeHere3",
                  "FitsStorage1-Rep-2/saf/ShouldNotBeHere4",
                  "FitsStorage2-Main-3/staging/ShouldNotBeHere5"]
        files2 = ["FitsStorage1-Main-1/IsNotHere1",
                  "FitsStorage1-Main-1/saf/2001-05-08/IsNotHere2",
                  "FitsStorage2-Main-3/staging/IsNotHere3"]
        filePaths = []
        for file in files1:
            filePaths.append(basePath + file)
            os.system("touch %s" % filePaths[-1])
        for file in files2: filePaths.append(basePath + file)
        fileList = _createFileList(filePaths)
        # Invoke with execute=0.
        execute = 0
        _verifyEmailAndStdout(self, "test_NormalExec_3_1", fileList, None,
                              execute)
        # Invoke with execute=1.
        execute = 1
        _verifyEmailAndStdout(self, "test_NormalExec_3_2", fileList, None,
                              execute)
        # Check that files are gone.
        for file in filePaths:
            if (os.path.exists(file)):
                msg = "File: %s supposed to be discarded still found on " +\
                      "storage media"
                self.fail(msg % file)


    def test_NormalExec_4(self):
        """
        Synopsis:
        Normal execution of the tool, files are discarded (Disk ID, File ID,
        File Version mixed with complete path references).

        Description:
        The purpose of the test is to verify the proper functioning of the
        tool when a mix of files referenced to by the Disk ID/File ID/File
        Version is given together with complete path references.

        Some of the files referenced in either case do not exist.

        Expected Result:
        The files that can be discarded are selected and discarded. The
        other files are just reported.

        Test Steps:
        - Start server.
        - Archive files.
        - Clone files.
        - Discard some files.
        - Create spurious files.
        - Create File List with Disk ID/File ID/File Version + complete path
          references.
        - Execute ngasDiscardFiles with execute=0, verify outputs.
        - Execute ngasDiscardFiles with execute=1, verify outputs.
        - Check that files gone from DB.
        - Check that files referred to by the complete path are gone from the
          disk.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        for n in range(10): sendPclCmd(port=8888).archive(TEST_FILE_1)
        stat = sendPclCmd().sendCmdGen(getHostName(), 8888, NGAMS_CLONE_CMD,
                                       pars=[["disk_id", SRC_DISK_ID]])

        diskId = "tmp-ngamsTest-NGAS-FitsStorage1-Rep-2"
        for fileVer in [1, 3, 7]:
            stat = sendPclCmd().sendCmdGen(getHostName(), 8888,
                                           NGAMS_DISCARD_CMD,
                                           pars=[["disk_id", diskId],
                                                 ["file_id", FILE_ID_1],
                                                 ["file_version", fileVer],
                                                 ["execute", 1]])
        basePath = "/tmp/ngamsTest/NGAS/"
        files1 = ["FitsStorage1-Main-1/ShouldNotBeHere1",
                  "FitsStorage1-Main-1/.db/ShouldNotBeHere2",
                  "FitsStorage1-Main-1/saf/2001-05-08/ShouldNotBeHere3",
                  "FitsStorage1-Rep-2/saf/ShouldNotBeHere4",
                  "FitsStorage2-Main-3/staging/ShouldNotBeHere5"]
        files2 = ["FitsStorage1-Main-1/IsNotHere1",
                  "FitsStorage1-Main-1/saf/2001-05-08/IsNotHere2",
                  "FitsStorage2-Main-3/staging/IsNotHere3"]
        filePaths = []
        for file in files1:
            filePaths.append(basePath + file)
            os.system("touch %s" % filePaths[-1])
        for file in files2: filePaths.append(basePath + file)
        fileList = _createFileList((FILE_INFO_1 + filePaths), FILE_ID_1)
        # Invoke with execute=0.
        execute = 0
        _verifyEmailAndStdout(self, "test_NormalExec_4_1", fileList, None,
                              execute)
        # Invoke with execute=1.
        execute = 1
        _verifyEmailAndStdout(self, "test_NormalExec_4_2", fileList, None,
                              execute)
        # Check that files + DB info is gone.
        _checkFilesDiscarded(self, FILE_INFO_1, FILE_ID_1, dbObj)
        # Check that files are gone.
        for file in filePaths:
            if (os.path.exists(file)):
                msg = "File: %s supposed to be discarded still found on " +\
                      "storage media"
                self.fail(msg % file)


    def test_NormalExec_5(self):
        """
        Synopsis:
        Normal execution of the tool, files are discarded. The input is a
        DCC message/'Non Registered Files'.

        Description:
        The purpose of the test is to verify that the tool handles properly
        the case where a DCC message is given as file reference.

        Expected Result:
        The tool should find the files and should remove these.

        Test Steps:
        - Start server.
        - Archive some files onto it.
        - Clone the files.
        - Create spurious files on some disks.
        - Create file list with the spurious files + non-existing, spurious
          files.
        - Invoke ngasDiscardFiles on a DCC Message reporting the spurious files
          as non-existing files, execute=0, check outputs.
        - Invoke ngasDiscardFiles on a DCC, execute=1, check outputs.
        - Check that the spurious files are gone.

        Remarks:
        IMPL: Would be better to generate a real DCC message via the DCC.
        """
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        for n in range(10): sendPclCmd(port=8888).archive(TEST_FILE_1)
        stat = sendPclCmd().sendCmdGen(getHostName(), 8888, NGAMS_CLONE_CMD,
                                       pars=[["disk_id", SRC_DISK_ID]])
        basePath = "/tmp/ngamsTest/NGAS/"
        files1 = ["FitsStorage1-Main-1/ShouldNotBeHere1",
                  "FitsStorage1-Main-1/.db/ShouldNotBeHere2",
                  "FitsStorage1-Main-1/saf/2001-05-08/ShouldNotBeHere3",
                  "FitsStorage1-Rep-2/saf/ShouldNotBeHere4",
                  "FitsStorage2-Main-3/staging/ShouldNotBeHere5"]
        files2 = ["FitsStorage1-Main-1/IsNotHere1",
                  "FitsStorage1-Main-1/saf/2001-05-08/IsNotHere2",
                  "FitsStorage2-Main-3/staging/IsNotHere3"]
        filePaths = []
        for file in files1:
            filePaths.append(basePath + file)
            os.system("touch %s" % filePaths[-1])
        for file in files2: filePaths.append(basePath + file)
        dccMsg = "src/ngasDiscardFilesTest_NormalExec_5_DCC_MSG.eml"
        # Invoke with execute=0.
        execute = 0
        _verifyEmailAndStdout(self,"test_NormalExec_5_1",None,dccMsg,execute)
        # Invoke with execute=1.
        execute = 1
        _verifyEmailAndStdout(self,"test_NormalExec_5_2",None,dccMsg,execute)
        # Check that files are gone.
        for file in filePaths:
            if (os.path.exists(file)):
                msg = "File: %s supposed to be discarded still found on " +\
                      "storage media"
                self.fail(msg % file)


    def test_NormalExec_6(self):
        """
        Synopsis:
        Normal execution of the tool, files are discarded. The input is a
        DCC message/'File in DB missing on disk'.

        Description:
        The purpose of the test is to verify that the tool handles properly
        the case where a DCC message is given as file reference.

        Expected Result:
        The tool should find the files and should remove these.

        Test Steps:
        - Start server.
        - Archive some files onto it.
        - Clone the files.
        - Remove some of the archived files (os rm).
        - Invoke ngasDiscardFiles on a DCC Message reporting the files removed
          above as files registered in the DB but not found on disk.
        - Invoke ngasDiscardFiles, execute=1, check outputs.
        - Check that the spurious files are gone.

        Remarks:
        IMPL: Would be better to generate a real DCC message via the DCC.
        """
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        for n in range(10): sendPclCmd(port=8888).archive(TEST_FILE_1)
        stat = sendPclCmd().sendCmdGen(getHostName(), 8888, NGAMS_CLONE_CMD,
                                       pars=[["disk_id", SRC_DISK_ID]])
        i = 0
        for fi in FILE_INFO_1:
            basePath = fi[0].replace("tmp-ngamsTest-NGAS-",
                                     "/tmp/ngamsTest/NGAS/")
            path = "%s/saf/2001-05-08/%s/%s.fits.Z"%(basePath,fi[1],FILE_ID_1)
            if ((i % 2) == 0): os.system("rm -f %s" % path)
            i += 1
        dccMsg = "src/ngasDiscardFilesTest_NormalExec_6_DCC_MSG.eml"
        # Invoke with execute=0.
        execute = 0
        _verifyEmailAndStdout(self, "test_NormalExec_6_1", None,dccMsg,execute)
        # Invoke with execute=1.
        execute = 1
        _verifyEmailAndStdout(self, "test_NormalExec_6_2", None,dccMsg,execute)
        # Check that files + DB info is gone.
        fileInfo = [["tmp-ngamsTest-NGAS-FitsStorage1-Main-1", "2"],
                    ["tmp-ngamsTest-NGAS-FitsStorage1-Rep-2", "3"],
                    ["tmp-ngamsTest-NGAS-FitsStorage1-Rep-2", "8"]]
        _checkFilesDiscarded(self, fileInfo, FILE_ID_1, dbObj)


    def test_NormalExec_7(self):
        """
        Synopsis:
        Normal execution of the tool, files are discarded. The input is a
        DCC message/'File in DB missing on disk' + 'Non registrered files'.

        Description:
        The purpose of the test is to verify that the tool handles properly
        the case where a DCC message is given as file reference.

        Expected Result:
        The tool should find the files and should remove these.

        Test Steps:
        - Start server.


        Remarks:
        IMPL: Not yet implemented.
        """
        pass


    def test_NormalExec_8(self):
        """
        Synopsis:
        Normal execution of the tool, files are discarded. Some files are
        marked to be ignored.

        Description:
        Test that files marked as 'ignore' can be discarded using the tool.

        Expected Result:
        All files scheduled for discartion should be removed from the system

        Test Steps:
        - Start server.
        - Archive a set of files on the server.
        - Mark some of the files to be discarded as ignore.
        - Invoke the tool to delete some of these files, execute=0.
        - Check output produced by the tool.
        - Invoke the tool to delete some of these files, execute=1.
        - Check output produced by the tool (stdout + email).
        - Check that the specified files disappeared.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        for n in range(10): sendPclCmd(port=8888).archive(TEST_FILE_1)
        sendPclCmd().sendCmdGen(getHostName(), 8888, NGAMS_CLONE_CMD,
                                pars=[["disk_id", SRC_DISK_ID]])
        diskId = "tmp-ngamsTest-NGAS-FitsStorage1-Rep-2"
        i = 0
        for fi in FILE_INFO_1:
            if ((i % 2) == 0):
                query = "UPDATE ngas_files SET file_ignore=1 WHERE "+\
                        "disk_id='%s' AND file_id='%s' AND file_version=%s"
                dbObj.query(query % (fi[0], FILE_ID_1, fi[1]))
            i += 1
        fileList = _createFileList(FILE_INFO_1, FILE_ID_1)
        # Invoke with execute=0.
        execute = 0
        _verifyEmailAndStdout(self, "test_NormalExec_8_1", fileList, None,
                              execute)
        # Invoke with execute=1.
        execute = 1
        _verifyEmailAndStdout(self, "test_NormalExec_8_2", fileList, None,
                              execute)
        # Check that files + DB info is gone.
        _checkFilesDiscarded(self, FILE_INFO_1, FILE_ID_1, dbObj)


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngasDiscardFilesTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
