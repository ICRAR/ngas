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
# "@(#) $Id: ngasVerifyCloningTest.py,v 1.2 2008/08/19 20:37:46 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  27/01/2005  Created
#

"""
Test Suite for the ngasVerifyCloning Tool.
"""

import os, sys
from   ngams import *
import ngamsDiskInfo
import ngasUtils, ngasUtilsLib
from   ngamsTestLib import *
from   ngasUtilsTestLib import *


# Constants.
SRC_DISK_ID = "tmp-ngamsTest-NGAS:8001-FitsStorage1-Main-1"


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
                            pars=[["disk_id", SRC_DISK_ID]])


def _invokeVerCloning(srcDiskId,
                      trgHost,
                      ngasContactPort,
                      autoClone = 0,
                      recvNotifEmail = 1,
                      cfgFile = BASE_CFG_1,
                      accessCode = "X190ZXN0X18="):
    """
    Execute the ngasVerifyCloning tool as a shell command.

    srcDiskId:         Disk which was cloned (string).

    trgHost:           Name of the target host as registered in the NGAS DB
                       (string).

    ngasContactPort:   Port number of host to contact (integer).

    autoClone:         Run the tool with the -autoClone parameter
                       (integer/0|1).

    recvNotifEmail:    Receive email notification (integer/0|1).

    cfgFile:           Configuration file used for the test (string).

    accessCode:        Access code to use the NGAS Utilities (string).

    Returns:           Info written on stdout and notification email (tuple).
    """
    if (recvNotifEmail): flushEmailQueue()
    prepNgasResFile(cfgFile, ngasPort=ngasContactPort)

    os.environ["_NGAS_VERIFY_CLONING_TARGET_HOST_"] = trgHost
    cmd = "/opsw/packages/bin/ngasVerifyCloning -diskId %s" % srcDiskId
    if (autoClone): cmd += " -autoClone"
    info(1,"Command to invoke ngasVerifyCloning: %s" % cmd)
    stat, out = commands.getstatusoutput(cmd)
    out = out.replace(getHostName(), "localhost")
    if (recvNotifEmail):
        notifEmail = getEmailMsg()
        notifEmail = notifEmail.replace(getHostName(), "localhost")
    else:
        notifEmail = None
    return (stat, out, notifEmail)


def _hideSrcDisks():
    """
    Used to suppress data dir.

    Returns:  Void.
    """
    prepNgasResFile(BASE_CFG_1)
    server, db, user, password = ngasUtilsLib.getDbPars()
    dbConObj = ngamsDb.ngamsDb(server, db, user, password, 0)
    pathPat = "/tmp/ngamsTest/NGAS:8001/FitsStorage1-%s/saf/%s"

    # Make data dir disappear, mark disk as unmounted in the DB.
    for name in ("Main-1", "Rep-2"):
        dirOrg = pathPat % (name, "2001-05-08")
        dirTmp = pathPat % (name, ".2001-05-08")
        diskInfo = dirOrg + "/../../NgasDiskInfo"
        diskId = ngamsStatus.ngamsStatus().load(diskInfo, 1).\
                 getDiskStatusList()[0].getDiskId()
        ngamsDiskInfo.\
                        ngamsDiskInfo().read(dbConObj, diskId).\
                        setHostId("dummy-node").write(dbConObj)
        mvFile(dirOrg, dirTmp)


def _restoreSrcDisks():
    """
    Used to restore data dir.

    Returns:  Void.
    """
    prepNgasResFile(BASE_CFG_1)
    server, db, user, password = ngasUtilsLib.getDbPars()
    dbConObj = ngamsDb.ngamsDb(server, db, user, password, 0)
    pathPat = "/tmp/ngamsTest/NGAS:8001/FitsStorage1-%s/saf/%s"

    # Make data dir disappear, mark disk as unmounted in the DB.
    for name in ("Main-1", "Rep-2"):
        dirOrg = pathPat % (name, "2001-05-08")
        dirTmp = pathPat % (name, ".2001-05-08")
        diskInfo = dirTmp + "/../../NgasDiskInfo"
        diskId = ngamsStatus.ngamsStatus().load(diskInfo, 1).\
                 getDiskStatusList()[0].getDiskId()
        hostId = "%s:8001" % getHostName()
        ngamsDiskInfo.\
                        ngamsDiskInfo().read(dbConObj, diskId).\
                        setHostId(hostId).write(dbConObj)
        mvFile(dirTmp, dirOrg)


def _verifyEmailAndStdout(testSuiteObj,
                          testName,
                          verifyEmail = 1,
                          autoClone = 0,
                          cfgFile = BASE_CFG_1,
                          srcDiskId = SRC_DISK_ID):
    """
    Invoke ngasVerifyCloning and check that the results are as expected.

    testSuiteObj:   Test suite object (ngamsTestSuite)

    testName:       Name of the test case (string).

    verifyEmail:    Verify also the contents of the email notification mail
                    (integer/0|1).

    autoClone:      Run the tool with the -autoClone parameter
                    (integer/0|1).

    cfgFile:        Configuration file used for the test (string).

    Returns:        Void.
    """
    stat, out, notifEmail = _invokeVerCloning(srcDiskId, "%s:8000" %\
                                              getHostName(), 8000, autoClone,
                                              verifyEmail, cfgFile)
    refStatFile = "ref/ngasVerifyCloningTest_%s_1" % testName
    if (verifyEmail):
        tmpStatFile = saveInFile(None, notifEmail)
        testSuiteObj.checkFilesEq(refStatFile, tmpStatFile,
                                  "Incorrect/missing " +\
                                  "notification email msg")
    refStatFile = "ref/ngasVerifyCloningTest_%s_2" % testName
    tmpStatFile = saveInFile(None, out)
    testSuiteObj.checkFilesEq(refStatFile, tmpStatFile,
                              "Incorrect/missing info on stdout")


class ngasVerifyCloningTest(ngasUtilsTestSuite):
    """
    Synopsis:
    Test Suite for the ngasVerifyCloning Tool.

    Description:
    Exercise the ngasVerifyCloning Tool and check the proper functioning
    under normal and abnormal conditions.

    Missing Test Cases:
    ...
    """

    def test_NormalExec_1(self):
        """
        Synopsis:
        Normal operation, no missing files.
        
        Description:
        Test the nominal (ideal) case, whereby a disk has been successfully
        cloned and all files are found on the target system and in the DB
        for disks in the target system.

        Expected Result:
        The verification should not find any problems, and should indicate this
        on stdout and in the form of an Email Notification Email.

        Test Steps:
        - Start simluated cluster.
        - Archive set of 10 files onto node 1.
        - Clone target disk hosting the archived files onto node 2.
        - Make the files archived into the node 1 unavailable (to ensure
          that the right ones are checked).
        - Launch ngasVerifyCloning.
        - Check that the output on stdout indicates that no discrepancies
          were found.
        - Check that the report send via Email Notification does not report
          any problems.

        Remarks:
        ...
        """
        _prepTestEnv(self)
        _hideSrcDisks()
        _verifyEmailAndStdout(self, "test_NormalExec_1")
    

    def test_NormalExec_2(self):
        """
        Synopsis:
        Missing files on disk, re-run with Auto Clone, re-run to verify
        recovering.
        
        Description:
        The purpose of the test is to verify that the ngasVerifyCloning Tool
        detects, when files have not been properly cloned. In this case, all
        files are not found on the target system and can recover from this
        situation.

        Expected Result:
        When executing the tool it should detect that files are missing.
        
        When executing the tool again, with the Auto Clone parameter, the
        missing files should be cloned.

        Afterwards, when launching the tool again, no, problems should be
        reported.
        
        Test Steps:
        - Start simluated cluster with two nodes.
        - Archive a set of files onto node 1.
        - Clone the archived files to node 2.
        - Remove some of the cloned files from disk.
        - Make files on node 1 temporarily unavilable.
        - Check that the report written on stdout reports the errors as
          expected.
        - Check that the report sent out via email reports the errors expected.
        - Make the files on node 1 available.
        - Re-launch the tool with the Auto Clone option.
        - Verify that the problems encountered are recovered (stdout, email).
        - Make the files on node 1 unavailable.
        - Re-launch the tool without Auto Clone.
        - Check tha the reports (stdout/email) indicate that the problems
          have been recovered.

        Remarks:
        ...
        """
        _prepTestEnv(self)

        # Remove some of the cloned files and run the clone verification.
        diskId = "tmp-ngamsTest-NGAS:8000-FitsStorage1-Main-1"
        fileId = "TEST.2001-05-08T15:25:00.123"
        for fileVer in [1,3,7,8]:
            stat = sendPclCmd().sendCmdGen(getHostName(), 8000,
                                           NGAMS_DISCARD_CMD,
                                           pars=[["disk_id", diskId],
                                                 ["file_id", fileId],
                                                 ["file_version", fileVer],
                                                 ["execute", 1]])
            
        # Invoke ngasVerifyCloning(), check that missing files are detected.
        _hideSrcDisks()
        _verifyEmailAndStdout(self, "test_NormalExec_2_1")

        # Invoke ngasVerifyCloning() to have the missing files autocloned.
        # Make first the suppressed source files available.
        _restoreSrcDisks()
        _verifyEmailAndStdout(self, "test_NormalExec_2_2", autoClone=1)

        # Now, invoke ngasVerifyCloning() to check that everything is OK.
        # Suppress source files so that these are not taken into account.
        _hideSrcDisks()
        _verifyEmailAndStdout(self, "test_NormalExec_2_3")


    def test_AbnormalExec_1(self):
        """
        Synopsis:
        Test handling of abnormal execution.
        
        Description:
        The purpose of this test is to verify that the following cases are
        properly handled by the tool:

        - Missing ~/.ngas.
        - Problem connecting to DB.
        - Wrong access password given.

        Expected Result:
        In all three cases above, an appropriate error message should be
        generated on stdout.

        Test Steps:
        - Invoke tool. Ensure that there is no ~/.ngas resource file.
          - Check that message on stdout is as expected.
        - Change the ~/.ngas resource file such that the DB connection is
          invalid.
          - Check that message on stdout is as expected.
        - Change the ~/.ngas resource file such that the NGAS Utilities
          Access Password is invalid.
          - Check that message on stdout is as expected.

        Remarks:
        ...
        """
        # 1. No ~/.ngas file:
        rmFile(os.path.expanduser("~/.ngas"))
        cmd = "/opsw/packages/bin/ngasVerifyCloning -diskId DUMMY-DISK-ID"
        stat, out = commands.getstatusoutput(cmd)
        if (stat == 0): self.fail("Unexpected exit code: %d" % stat)
        refStatFile = "ref/ngasVerifyCloningTest_test_AbnormalExec_1_1"
        tmpStatFile = saveInFile(None, out)
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect error " +\
                          "message produced by ngasVerifyCloning")

        # 2. Invalid DB connection:
        tmpCfgFile = genTmpFilename("ngasVerifyCloningTest")
        cfg = ngamsConfig.ngamsConfig().load(BASE_CFG_1).\
              storeVal("NgamsCfg.Db[1].Server", "NOT-EXISTING").\
              save(tmpCfgFile, 0)
        stat, out, notifEmail = _invokeVerCloning(SRC_DISK_ID, "DUMMY-HOST",
                                                  8000, autoClone=0,
                                                  recvNotifEmail=0,
                                                  cfgFile=tmpCfgFile)
        splitStr = "ERROR occurred executing the Clone Verification Tool:"
        errMsg = out.split(splitStr)[-1].strip()
        refStatFile = "ref/ngasVerifyCloningTest_test_AbnormalExec_1_2"
        tmpStatFile = saveInFile(None, errMsg)
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect error " +\
                          "message produced by ngasVerifyCloning")

        # 3. Access Password invalid:
        stat, out, notifEmail = _invokeVerCloning("DUMMY-DISK-ID",
                                                  "TARGET-HOST", 11111,
                                                  recvNotifEmail=0,
                                                  accessCode="BLA-BLA-BLA")
        refStatFile = "ref/ngasVerifyCloningTest_test_AbnormalExec_1_3"
        tmpStatFile = saveInFile(None, out)
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect error " +\
                          "message produced by ngasVerifyCloning")

    
    def test_AbnormalExec_2(self):
        """
        Synopsis:
        Test handling of abnormal execution.

        Description:
        The purpose of this test is to verify that the following cases are
        properly handled by the tool:
        
        - Non-existing Disk ID given for verification.
        - Disk referred to by Disk ID inserted in same unit where the
          verification takes place.

        Expected Result:
        In all cases above, an appropriate error message should be
        generated on stdout.

        Test Steps:
        - Start clone verification, specifying a non-existing Disk ID.
          - Check that an error message generated by tool is correct.
        - Specify Disk ID of disk inserted in the node where the verification
          is taking place.
          - Check that an error message generated by tool is correct.

        Remarks:
        ...
        """
        # 1. Non-existing Disk ID:
        _verifyEmailAndStdout(self, "test_AbnormalExec_2_1",
                              verifyEmail=0, srcDiskId="NON-EXISTING")

        # 2. Source disk inserted in unit where verification is run.
        self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        for n in range(10): sendPclCmd(port=8888).archive(TEST_FILE_1)
        srcDisk = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        sendPclCmd().sendCmdGen(getHostName(), 8888, NGAMS_CLONE_CMD,
                                pars=[["disk_id", srcDisk]])
        _verifyEmailAndStdout(self, "test_AbnormalExec_2_2", verifyEmail=0,
                              srcDiskId=srcDisk)


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngasVerifyCloningTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
