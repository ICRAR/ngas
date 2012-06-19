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
# "@(#) $Id: ngamsOnlineCmdTest.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  20/11/2003  Created
#

"""
This module contains the Test Suite for the ONLINE Command.
"""

import os, sys
from   ngams import *
from   ngamsTestLib import *



def _checkDiskInfo(testCaseObj,
                   srvObj,
                   testMethod,
                   mtPts = []):
    """
    Check info for disks in DB and in the NgasDiskInfo files.

    testCaseObj:   Instance of NG/AMS Test Suite object (child of
                   ngamsTestLib.ngamsTestSuite).

    srvObj:        Server object (ngamsServer).

    testMethod:    Test method name (string).

    mtPts:         List with mount points (sorted) (list).

    Returns:       List with mount points (sorted) (list).
    """
    # Check that disks have been properly inserted in DB.
    diskIds = srvObj.getDb().getDiskIds()
    diskIds.sort()
    diskInfoBuf = ""
    if (mtPts == []):
        getMtPts = 1
    else:
        getMtPts = 0
    for diskId in diskIds:
        diskInfo = ngamsDiskInfo.ngamsDiskInfo()
        diskInfo.unpackSqlResult(srvObj.getDb().getDiskInfoFromDiskId(diskId))
        if (getMtPts): mtPts.append(diskInfo.getMountPoint())
        diskInfoBuf += filterDbStatus1(diskInfo.dumpBuf()) + "\n"
    tmpStatFile = "tmp/ngamsCmdHandlingTest_" + testMethod + "_1_tmp"
    refStatFile = "ref/ngamsCmdHandlingTest_" + testMethod + "_1_ref"
    saveInFile(tmpStatFile, diskInfoBuf)
    testCaseObj.checkEqual("", cmpFiles(refStatFile, tmpStatFile), 
                           genErrMsg("Disks incorrectly registered (Test: " +\
                           testMethod + ")", refStatFile, tmpStatFile))

    # Check that NgasDiskInfo files have been properly created on the disks.
    mtPts.sort()
    diskInfoBuf = ""
    for mtPt in mtPts:
        statBuf = loadFile(os.path.normpath(mtPt + "/" + "NgasDiskInfo"))
        tmpStat = ngamsStatus.ngamsStatus().unpackXmlDoc(statBuf, 0, 1)
        diskInfoBuf += filterDbStatus1(tmpStat.dumpBuf(0, 0)) + "\n"
    tmpStatFile = "tmp/ngamsCmdHandlingTest_" + testMethod + "_2_tmp"
    refStatFile = "ref/ngamsCmdHandlingTest_" + testMethod + "_2_ref"
    saveInFile(tmpStatFile, diskInfoBuf)
    testCaseObj.checkEqual("", cmpFiles(refStatFile, tmpStatFile), 
                           genErrMsg("Invalid NgasDiskInfo files found " +\
                                     "(Test: " + testMethod + ")",
                                     refStatFile, tmpStatFile))
    return mtPts


class ngamsOnlineCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite for the ONLINE Command.

    Description:
    ...

    Missing Test Cases:
    - This whole Test Suite should be reviewed and missing Test Cases added.
    - Re-registration DB -> NgasDiskInfo (no NgasDiskInfo).
    - Re-registration DB -> NgasDiskInfo (DB info newer).
    - Re-registration NgasDiskInfo -> DB (no DB info).
    - Re-registration NgasDiskInfo -> DB (NgasDiskInfo info newer).
 
    - Make dummy Online Plug-In, which returns no physical disks, and check
      the SW behaves as expected (goes online, reports that there are no disks,
      ...).

    - Check that if there are no Disk Sets available and Archiving is disabled,
      no problems are reported.

    - Following Test Cases +/- completed disks:
      - M Slot: M Disk + R Slot: M Disk
      - M Slot: R Disk + R Slot: R Disk
      - R Slot: M Disk + R Slot: M Disk
      - more combinations illegal/legal ones ...
    """

    def test_OnlineCmd_1(self):
        """
        Synopsis:
        Test basic handling of Online Command.
        
        Description:
        The purpose of the test is to verify that the server goes Online
        initializing with the specified cfg. file when the ONLINE Command is
        issued.

        Expected Result:
        After being started up and in Offline/Idle State and receiving the
        ONLINE Command, the server should re-load the cfg. and should bring
        the system to Online State according to the cfg. file.

        Test Steps:
        - Start server (Auto Online=0).
        - Send ONLINE Command.
        - Check that the response from the server is as expected.

        Remarks:
        TODO: Check that the server is Online (DB + STATUS Command).
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 0)
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_ONLINE_CMD,
                                 genStatFile = 1)
        refStatFile = "ref/ngamsOnlineCmdTest_test_OnlineCmd_1_1_ref"
        self.checkFilesEq(refStatFile, tmpStatFile,
                          "Incorrect status returned for ONLINE command")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsOnlineCmdTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
