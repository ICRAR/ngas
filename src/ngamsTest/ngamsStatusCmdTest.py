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
# "@(#) $Id: ngamsStatusCmdTest.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  23/04/2002  Created
#

"""
This module contains the Test Suite for the STATUS Command.
"""

import sys, unittest, socket, time, shutil
from   ngams import *
from   ngamsTestLib import *
import ngamsLib, ngamsConfig, ngamsStatus, ngamsReqProps
import ngamsDiskInfo, ngamsFileInfo
import ngamsHighLevelLib, ngamsCmdHandling


class ngamsStatusCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Test execution of the STATUS Command.

    Description:
    The purpose of this Test Suite is to exercise the STATUS Command
    under normal and abnormal condition.

    Both the simple execution on the contacted node, and the Proxy Mode
    are tested.

    Missing Test Cases:
    - Review this Test Suite and add missing, important Test Cases.
    - Test STATUS?request_id!!!!
    - Test STATUS?disk_id
    - Test all cases for usage of STATUS with proxy host.
    - STATUS?file_access: File not found on disk.
    """

    def test_StatusCmd_1(self):
        """
        Synopsis:
        Test normal execution of STATUS Command/local.
        
        Description:
        Test the execution of the STATUS Command when no parameters are
        specified. This just makes the server return the basic status about
        the operational environment.

        Expected Result:
        The server should return a reponse to the STATUS Command, indicating
        the status of the system.

        Test Steps:
        - Start server.
        - Submit STATUS Command.
        - Check that the response to the STATUS Command contains the info
          as expected.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(8888, 1, 1, 1)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)
        status = client.status()
        if (status.getMessage().\
            find("Successfully handled command STATUS") == -1):
            self.fail("Illegal status returned for STATUS Command")


    def test_StatusCmd_2(self):
        """
        Synopsis:
        Test normal execution of STATUS Command/Proxy.
        
        Description:
        If another node is specified than the contacted node, when submitting
        the STATUS Command, the contacted node should act as proxy and should
        forward the request to the specified sub-node.

        Expected Result:
        The response from the specified sub-node should be returned to the
        client.

        Test Steps:
        - Start simulated cluster.
        - Submit STATUS Command referring to sub-node in cluster.
        - Check that the response returned indicates that the request was
          forwarded to the sub-node.

        Remarks:
        ...
        """
        self.prepCluster("src/ngamsCfg.xml",
                         [[8000, None, None, getClusterName()],
                          [8011, None, None, getClusterName()]])
        statObj = ngamsPClient.ngamsPClient().\
                  sendCmdGen(getHostName(), 8000, "STATUS",
                             1, "", [["host_id", getNcu11()]])        
        refMsg = "Successfully handled command STATUS"
        if ((statObj.getMessage().find(refMsg) == -1) or
            (statObj.getHostId() != getNcu11())):
            self.fail("Illegal status returned for STATUS Command")


    def test_StatusCmd_3(self):
        """
        Synopsis:
        Test normal execution of STATUS Command/File Access/Proxy.
        
        Description:
        It is possible to query the information about the accessibility of
        a file given by its File ID and/or corresponding Disk ID and/or
        File Version. Also Proxy Mode is supported for this feature. This is
        exercised in this Test Suite.

        Expected Result:
        The information about the file, located on a sub-node should be
        retrieved by the contacted node acting as proxy.

        Test Steps:
        - Start simluated cluster.
        - Archive file onto sub-node.
        - Submit STATUS?file_access&file_version Command specifying file,
          previously archived.
        - Check that the contacted node located the file and forwarded the
          request to the sub-node and that the appropriate reponse was
          returned.

        Remarks:
        ...
        """
        self.prepCluster("src/ngamsCfg.xml",
                         [[8000, None, None, getClusterName()],
                          [8011, None, None, getClusterName()]])
        srcFile = "src/TinyTestFile.fits"
        ngamsPClient.ngamsPClient(getHostName(),8011).archive(srcFile,"", 1, 0)
        statObj = ngamsPClient.ngamsPClient().\
                  sendCmdGen(getHostName(), 8011, "STATUS", 1, "",
                             [["file_access", "NCU.2003-11-11T11:11:11.111"],
                              ["file_version", "1"]])
        refMsg = "NGAMS_INFO_FILE_AVAIL:4029:INFO: File with File ID: " +\
                 "NCU.2003-11-11T11:11:11.111/Version: 1, is available on " +\
                 "NGAS Host with Host ID: %s." % getNcu11()
        if (statObj.getMessage().find(refMsg) == -1):
            self.checkEqual(refMsg, statObj.getMessage(), "Illegal status " +\
                            "returned for STATUS/File Access Command")

def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsStatusCmdTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
