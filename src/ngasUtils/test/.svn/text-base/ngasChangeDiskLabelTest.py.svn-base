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
# "@(#) $Id: ngasChangeDiskLabelTest.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  08/08/2005  Created
#

"""
This module contains the Test Suite for the NGAS Utilities Tool
ngasChangeDiskLabel.
"""

import os, sys
from   ngams import *
import ngasUtils, ngasUtilsLib
from   ngamsTestLib import *
from   ngasUtilsTestLib import *


# IMPL: It seems that Logical Names may be allocated slightly differently when
#       the server goes Online. Probably the tests should be made immune to
#       this. I.e., from the Disk ID the actual label should be resolved and
#       the reference status files updated with the actual label.


def _invokeChangeDiskLabel(diskId,
                           newLabel,
                           recvNotifEmail = 1,
                           cfgFile = BASE_CFG_1,
                           accessCode = "X190ZXN0X18=",
                           execute = 1):
    """
    Execute the ngasChangeDiskLabel tool as a shell command.

    diskId:            ID of disk concerned (string).

    newLabel:          New label (string).

    recvNotifEmail:    Receive email notification (integer/0|1).

    cfgFile:           Configuration file used for the test (string).

    accessCode:        Access code to use the NGAS Utilities (string).

    Returns:           Info written on stdout and notification email (tuple).
    """
    if (recvNotifEmail): flushEmailQueue()
    prepNgasResFile(cfgFile)
    cmd = "/opsw/packages/bin/ngasChangeDiskLabel -accessCode %s -diskId %s "+\
          "-newLabel %s" 
    if (execute): cmd += " -execute"
    cmd = cmd % (accessCode, diskId, newLabel)
    stat, out = commands.getstatusoutput(cmd)
    if (recvNotifEmail):
        notifEmail = getEmailMsg()
        notifEmail = notifEmail.replace(getHostName(), "localhost")
    else:
        notifEmail = None
    return (stat, out, notifEmail)


def _verifyEmailAndStdout(testSuiteObj,
                          testName,
                          diskId,
                          newLabel,
                          verifyEmail = 1,
                          cfgFile = BASE_CFG_1):
    """
    Invoke ngasChangeDiskLabel and check that the results are as expected.

    testSuiteObj:   Test suite object (ngamsTestSuite)

    testName:       Name of the test case (string).

    diskId:         Disk ID of disk concerned (string).

    newLabel:       New label (string).

    verifyEmail:    Verify also the contents of the email notification mail
                    (integer/0|1).

    cfgFile:        Configuration file used for the test (string).

    Returns:        Void.
    """
    stat, out, notifEmail = _invokeChangeDiskLabel(diskId, newLabel,
                                                   verifyEmail, cfgFile,
                                                   accessCode="__test__")
    refStatFile = "ref/ngasChangeDiskLabel_%s_1" % testName
    out = filterOutLines(out, discardTags=["InstallationDate:",
                                           "AvailableMb:"])
    out = out.replace(getHostName(), "localhost")
    tmpStatFile = saveInFile(None, out)
    testSuiteObj.checkFilesEq(refStatFile, tmpStatFile,
                              "Incorrect/missing info on stdout")
    if (verifyEmail):
        refStatFile = "ref/ngasChangeDiskLabel_%s_2" % testName
        notifEmail = filterOutLines(notifEmail,
                                    discardTags=["InstallationDate:",
                                                 "AvailableMb:"])
        tmpStatFile = saveInFile(None, notifEmail)
        testSuiteObj.checkFilesEq(refStatFile, tmpStatFile,
                                  "Incorrect/missing " +\
                                  "notification email msg")
            

class ngasChangeDiskLabelTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite for the ngasChangeDiskLabel Tool.

    Description:
    The Test Suite excersizes the ngasChangeDiskLabel Tool and checks the
    behavior under normal and abnormal conditions.

    Missing Test Cases:
    ...
    """

    def test_NormalExec_1(self):
        """
        Synopsis:
        Normal execution of the tool, change a Disk Label.
        
        Description:
        The purpose of the test is to verify the standard behavior of the
        tool usage.

        Expected Result:
        The tool should be invoked to change a label.

        Test Steps:
        - Start server.
        - Invoke the tool to change label of one of the disks.
        - Check that a email notification is sent out, indicating the change.
        - Check that the new label is OK.
        - Bring server offline/online.
        - Check that the new label is OK.

        Remarks:
        ...
        """
        diskId = "tmp-ngamsTest-NGAS-FitsStorage2-Main-3"
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        _verifyEmailAndStdout(self, "test_NormalExec_1", diskId,
                              "test_NormalExec_1-M-000002")
        stat = sendPclCmd().sendCmdGen(getHostName(), 8888,
                                       NGAMS_OFFLINE_CMD)
        stat = sendPclCmd().sendCmdGen(getHostName(), 8888,
                                       NGAMS_ONLINE_CMD)
        labelName = str(dbObj.getLogicalNameFromDiskId(diskId))
        refVal = "test_NormalExec_1-M-000002"
        self.checkEqual(refVal, labelName,
                        "Incorrect new Disk Label: %s, expected: %s" %\
                        (labelName, refVal))
                    
    
    def test_AbnormalExec_1(self):
        """
        Synopsis:
        Change label to existing label.
        
        Description:
        The purpose of the test is to verify that the tool detects if it is
        attempted to change the label of a disk ti a name already existing.

        Expected Result:
        The tool should detect that an already existing name is chosen and
        should reject the request. Error messages should be generated
        accordingly.

        Test Steps:
        - Start server.
        - Try to change a label to an already existing label.
        - Check that email notification message indicates the problem.
        - Check that the output on stdout indicates the problem.
        - Check that the referenced Disk Label is unchanged.

        Remarks:
        ...
        """
        self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        _verifyEmailAndStdout(self, "test_AbnormalExec_1",
                              "tmp-ngamsTest-NGAS-FitsStorage2-Main-3",
                              "M-000005")


    def test_AbnormalExec_2(self):
        """
        Synopsis:
        Change label to an invalid label.
        
        Description:
        The purpose of the test is to verify that the tool detects if it is
        attempted to change the label of a disk to an invalid label.

        Expected Result:
        The tool should detect that the requested label is invalid and
        should reject the request. Error messages should be generated
        accordingly.

        Test Steps:
        - Start server.
        - Try to change a label to an invalid label.
        - Check that email notification message indicates the problem.
        - Check that the output on stdout indicates the problem.
        - Check that the referenced Disk Label is unchanged.

        Remarks:
        ...
        """
        # IMPL: To be implemented when the relabelling is done via the
        #       NG/AMS Server. The check should take place in the server and
        #       not in the tool.
        pass


    def test_AbnormalExec_3(self):
        """
        Synopsis:
        Refer to non-existing disk.
        
        Description:
        The purpose of the test is to verify that the tool detects if it is
        attempted to change the label of a non-existing disk.

        Expected Result:
        The tool should detect that the disk does not exist and should reject
        the request. Error messages should be generated accordingly.

        Test Steps:
        - Start server.
        - Try to change a label of a non-existing disk.
        - Check that email notification message indicates the problem.
        - Check that the output on stdout indicates the problem.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=BASE_CFG_1)
        _verifyEmailAndStdout(self, "test_AbnormalExec_3", "NON-EXISTING-DISK",
                              "test_AbnormalExec_3-M-000002")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngasChangeDiskLabel"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
