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
# "@(#) $Id: ngamsRegisterCmdTest.py,v 1.6 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/11/2003  Created
#

"""
This module contains the Test Suite for the REGISTER Command.
"""

import os, sys, time
from   ngams import *
import ngamsFileInfo
from   ngamsTestLib import *


class ngamsRegisterCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite for  REGISTER Command.

    Description:
    Test Suite that exercises the REGISTER Command in various usages.

    Missing Test Cases:
    - wait=0/1
    - Register file already registered.
    - Register illegal file.
    - Non-existing file/path.
    - File/path not on NGAS Disk.
    - Unknown mime-type.
    """

    def test_RegisterCmd_1(self):
        """
        Synopsis:
        REGISTER Command/register single file compl. path.
        
        Description:
        Test handling of the REGISTER Command under normal circumstances.
        
        Expected Result:
        The REGISTER Command should be accepted by the server and should
        be executed successfully.

        Test Steps:
        - Start server.
        - Copy file onto NGAS Disk.
        - Submit REGISTER Command requesting to register the file copied over
          (wait=1).
        - Check response from the server that the request was successfully
          executed.
        - Check the DB info for the registered file.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        srcFile = "src/SmallFile.fits"
        tmpSrcFile = "/tmp/ngamsTest/NGAS/" +\
                     "FitsStorage2-Main-3/saf/test/SmallFile.fits"
        checkCreatePath(os.path.dirname(tmpSrcFile))
        os.system("cp " + srcFile + " " + tmpSrcFile)
        tmpStatFile = sendExtCmd(getHostName(), 8888, NGAMS_REGISTER_CMD,
                                 [["path", tmpSrcFile]])
        refStatFile = "ref/ngamsRegisterCmdTest_test_RegisterCmd_1_ref"
        self.checkFilesEq(refStatFile, tmpStatFile,
                          "Incorrect status returned for REGISTER command")
        diskId = "tmp-ngamsTest-NGAS-FitsStorage2-Main-3"
        filePrefix = "ngamsRegisterCmdTest_test_RegisterCmd_1"
        fileInfoRef = "ref/" + filePrefix + "_FileInfo_ref"
        fileInfoTmp = "tmp/" + filePrefix + "_FileInfo_tmp"
        fileId = "TEST.2001-05-08T15:25:00.123"
        startTime = time.time()
        while ((time.time() - startTime) < 10):
            tmpFileRes = dbObj.getFileInfoFromFileIdHostId(getHostName(),
                                                           fileId, 1, diskId)
            if (tmpFileRes): break
        tmpFileObj = ngamsFileInfo.ngamsFileInfo().unpackSqlResult(tmpFileRes)
        saveInFile(fileInfoTmp, filterDbStatus1(tmpFileObj.dumpBuf()))
        self.checkFilesEq(fileInfoRef, fileInfoTmp,
                          "Incorrect info in DB for registered file")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsRegisterCmdTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
