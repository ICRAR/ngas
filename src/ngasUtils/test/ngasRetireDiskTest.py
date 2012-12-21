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
# "@(#) $Id: ngasRetireDiskTest.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  06/09/2005  Created
#

"""
This module contains the Test Suite for the NGAS Utilities Tool ngasRetireDisk.
"""

import os, sys
from   ngams import *
import ngamsDb, ngamsFileInfo, ngamsDiskInfo
import ngasUtils, ngasUtilsLib
from   ngamsTestLib import *
from   ngasUtilsTestLib import *


def _invokeRetireDisk(diskId,
                      execute,
                      force,
                      recvNotifEmail = 1,
                      cfgFile = BASE_CFG_1,
                      accessCode = "__test__"):
    """
    Execute the ngasRetireDisk tool as a shell command.

    diskId:            ID of disk to retire (string).   
    
    execute:           If set to 1 the -execute parameter is submitted to the
                       tool (integer/0|1).

    force:             If set to 1 the -force parameter is submitted to the
                       tool (integer/0|1).
    
    recvNotifEmail:    Receive email notification (integer/0|1).

    cfgFile:           Configuration file used for the test (string).

    accessCode:        Access code to use the NGAS Utilities (string).

    Returns:           Info written on stdout and notification email (tuple).
    """
    if (recvNotifEmail): flushEmailQueue()
    prepNgasResFile(cfgFile, ngasPort=8888)
    cmd = "/opsw/packages/bin/ngasRetireDisk -accessCode %s -diskid %s " %\
          (accessCode, diskId)
    if (execute): cmd += " -execute"
    if (force): cmd += " -force"
    stat, out = commands.getstatusoutput(cmd)
    out = out.replace(getHostName(), "localhost")
    out = supprVerbLogPreamb(out)
    if (recvNotifEmail):
        notifEmail = getEmailMsg()
        notifEmail = notifEmail.replace(getHostName(), "localhost")
    else:
        notifEmail = None
    return (stat, out, notifEmail)


def _verifyEmailAndStdout(testSuiteObj,
                          testName,
                          diskId,
                          execute,
                          force,
                          verifyEmail = 1,
                          cfgFile = BASE_CFG_1):
    """
    Invoke ngasDiscardFiles and check that the results are as expected.

    testSuiteObj:   Test suite object (ngamsTestSuite)

    testName:       Name of the test case (string).

    diskId:         ID of disk to retire (string).   

    execute:        If set to 1 the -execute parameter is submitted to the
                    tool (integer/0|1).

    force:          If set to 1 the -force parameter is submitted to the
                    tool (integer/0|1).
 
    verifyEmail:    Verify also the contents of the email notification mail
                    (integer/0|1).

    cfgFile:        Configuration file used for the test (string).

    Returns:        Void.
    """
    stat, out, notifEmail = _invokeRetireDisk(diskId, execute, force,
                                              verifyEmail, cfgFile)
    refStatFile = "ref/ngasRetireDisk_%s_1" % testName
    tmpStatFile = saveInFile(None, out)
    testSuiteObj.checkFilesEq(refStatFile, tmpStatFile,
                              "Incorrect/missing info on stdout")
    if (verifyEmail):
        refStatFile = "ref/ngasRetireDisk_%s_2" % testName
        tmpStatFile = saveInFile(None, notifEmail)
        testSuiteObj.checkFilesEq(refStatFile, tmpStatFile,
                                  "Incorrect/missing " +\
                                  "notification email msg")


def _checkDbInfo(testSuiteObj,
                 testName,
                 dbConObj,
                 diskId):
    """
    Dump the information in ngas_disks, ngas_files and ngas_files_retired in
    connection with the disk and compare the results to reference dumps.

    testSuiteObj:   Test suite object (ngamsTestSuite)

    testName:       Name of the test case (string).

    dbConObj:       DB connection object (ngamsDb).

    diskId:         ID of disk to retire (string).

    Returns:        Void.
    """
    # ngas_disks:
    tmpDiskInfo = ngamsDiskInfo.ngamsDiskInfo().read(dbConObj, diskId)
    diskInfoBuf = filterOutLines(tmpDiskInfo.dumpBuf(),
                                 ["InstallationDate:", "AvailableMb:",
                                  "TotalDiskWriteTime:"])
    refStatFile = "ref/ngasRetireDisk_%s_1" % testName
    tmpStatFile = saveInFile(None, diskInfoBuf)
    testSuiteObj.checkFilesEq(refStatFile, tmpStatFile,
                              "Incorrect/missing info in ngas_disks")
    
    # ngas_files:
    query = "SELECT %s FROM ngas_files nf WHERE disk_id='%s'" %\
            (ngamsDb._ngasFilesCols, diskId)
    curObj = dbConObj.dbCursor(query)
    fileInfoBuf = ""
    while (1):
        res = curObj.fetch(1)
        if (res == []): break
        tmpFileInfo = ngamsFileInfo.ngamsFileInfo().unpackSqlResult(res[0])
        fileInfoBuf += filterOutLines(tmpFileInfo.dumpBuf(),
                                      ["IngestionDate:", "CreationDate:"])+"\n"
    refStatFile = "ref/ngasRetireDisk_%s_2" % testName
    tmpStatFile = saveInFile(None, fileInfoBuf)
    testSuiteObj.checkFilesEq(refStatFile, tmpStatFile,
                              "Incorrect/missing info in ngas_files")

    # ngas_files_retired:
    query = "SELECT %s FROM ngas_files_retired nf WHERE disk_id='%s'" %\
            (ngamsDb._ngasFilesCols, diskId)
    curObj = dbConObj.dbCursor(query)
    fileInfoBuf = ""
    while (1):
        res = curObj.fetch(1)
        if (res == []): break
        tmpFileInfo = ngamsFileInfo.ngamsFileInfo().unpackSqlResult(res[0])
        fileInfoBuf += filterOutLines(tmpFileInfo.dumpBuf(),
                                      ["IngestionDate:", "CreationDate:"])+"\n"
    refStatFile = "ref/ngasRetireDisk_%s_3" % testName
    tmpStatFile = saveInFile(None, fileInfoBuf)
    testSuiteObj.checkFilesEq(refStatFile, tmpStatFile,
                              "Incorrect/missing info in ngas_files")

 
class ngasRetireDiskTest(ngasUtilsTestSuite):
    """
    Synopsis:
    Test Suite for the ngasRetireDisk Tool.

    Description:
    The Test Suite exercises the ngasRetireDisk Tool and checks the
    behavior under normal and abnormal conditions.

    Missing Test Cases:
    ...
    """

    def test_NormalExec_1(self):
        """
        Synopsis:
        Normal execution of the tool, a disk is retired.
        
        Description:
        The purpose of the test is to verify that the tool behaves as
        expected for a 'standard' disk retirement.

        Expected Result:
        The tool is first invoked without -execute, and should indicate
        that the retirement is possible. Subsequently it is invoked with
        -execute. An entry should be logged into the ngas_disk_hist, the entry
        from ngas_disks for the disk should be removed, the DB rows for the
        files registered for that disk should be moved to ngas_files_retired.

        Test Steps:
        - Start server.
        - Archive files onto the system.
        - Clone the Main Disk.
        - Bring the server Offline.
        - Invoke ngasRetireDisk to retire the cloned disk (execute=0).
        - Check outputs + that DB info intact.
        - Invoke ngasRetireDisk to retire the cloned disk (execute=1).
        - Check outputs + that DB info has been updated.

        Remarks:
        ...
        """
        diskId = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        for n in range(10): sendPclCmd(port=8888).archive(TEST_FILE_1)
        sendPclCmd().sendCmdGen(getHostName(), 8888, NGAMS_CLONE_CMD,
                                pars=[["disk_id", diskId]])
        sendPclCmd().sendCmdGen(getHostName(), 8888, NGAMS_OFFLINE_CMD)
        execute = 0
        force = 0
        _verifyEmailAndStdout(self,"test_NormalExec_1_1",diskId,execute,force)
        _checkDbInfo(self, "test_NormalExec_1_1_1", dbObj, diskId)
        execute = 1
        _verifyEmailAndStdout(self,"test_NormalExec_1_2",diskId,execute,force)
        _checkDbInfo(self, "test_NormalExec_1_2_1", dbObj, diskId)


    def test_NormalExec_2(self):
        """
        Synopsis:
        Normal execution of the tool, a disk is retired. Some files available
        in less than three copies (-force needed).
        
        Description:
        The purpose of the test is to verify that if there are less than 3
        copies of files on a disk to be retired, the retirement can be enforced
        by specifying the -force parameter to the ngasRetireDisk tool.

        Expected Result:
        First the outputs of the tool should indicate that there are files
        with less than 3 copies. The invoking the tool with -force, the disk
        retirement should be executed anyway and the outputs reflect this.
        Also the DB info (entries in ngas_files -> ngas_files_retired +
        entry in ngas_disks marked as 'RETIRED').

        Test Steps:
        - Start server.
        - Archive files onto the system.
        - Bring the server Offline.
        - Invoke ngasRetireDisk to retire the Main Disk (execute=0).
        - Check outputs (rejection) + that DB info intact.
        - Invoke ngasRetireDisk to retire the cloned disk (execute=1).
        - Check outputs (rejection) + that DB info intact.
        - Invoke ngasRetireDisk to retire the cloned disk (execute=1, force).
        - Check outputs + that DB info has been updated.

        Remarks:
        ...
        """
        diskId = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        for n in range(10): sendPclCmd(port=8888).archive(TEST_FILE_1)
        sendPclCmd().sendCmdGen(getHostName(), 8888, NGAMS_OFFLINE_CMD)
        execute = 0
        force = 0
        _verifyEmailAndStdout(self,"test_NormalExec_2_1",diskId,execute,force)
        _checkDbInfo(self, "test_NormalExec_2_1_1", dbObj, diskId)
        execute = 1
        _verifyEmailAndStdout(self,"test_NormalExec_2_2",diskId,execute,force)
        _checkDbInfo(self, "test_NormalExec_2_2_1", dbObj, diskId)
        force = 1
        _verifyEmailAndStdout(self,"test_NormalExec_2_3",diskId,execute,force)
        _checkDbInfo(self, "test_NormalExec_2_3_1", dbObj, diskId)


    def test_NormalExec_3(self):
        """
        Synopsis:
        Disk to be retired is Online.
        
        Description:
        The purpose of the test is to verify that the ngasRetireDisk Tool
        rejects a request to retire a disk when the disk is online.

        Expected Result:
        When invoking the tool (+/- execute, +/- force) it should always lead
        to rejection.

        Test Steps:
        - Start server.
        - Archive files onto the system.
        - Clone the Main Disk.
        - Invoke ngasRetireDisk to retire the cloned disk (execute=0).
        - Check outputs (rejection).
        - Invoke ngasRetireDisk to retire the cloned disk (execute=1).
        - Check outputs (rejection).
        - Invoke ngasRetireDisk to retire the cloned disk (execute=1, force).
        - Check outputs (rejection).

        Remarks:
        ...
        """
        diskId = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        for n in range(10): sendPclCmd(port=8888).archive(TEST_FILE_1)
        execute = 0
        force = 0
        _verifyEmailAndStdout(self,"test_NormalExec_3_1",diskId,execute,force)
        _checkDbInfo(self, "test_NormalExec_3_1_1", dbObj, diskId)
        execute = 1
        _verifyEmailAndStdout(self,"test_NormalExec_3_2",diskId,execute,force)
        _checkDbInfo(self, "test_NormalExec_3_2_1", dbObj, diskId)
        force = 1
        _verifyEmailAndStdout(self,"test_NormalExec_3_3",diskId,execute,force)
        _checkDbInfo(self, "test_NormalExec_3_3_1", dbObj, diskId)


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngasRetireDiskTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
