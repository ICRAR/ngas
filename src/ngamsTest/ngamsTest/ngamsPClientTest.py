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
# "@(#) $Id: ngamsPClientTest.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  23/04/2002  Created
#
"""
This module contains the Test Suite for the Python Client.
"""

import commands
import shutil
import sys

from ngamsLib.ngamsCore import getHostName, ngamsGetSrcDir
from ngamsPClient import ngamsPClient
from ngamsTestLib import ngamsTestSuite, saveInFile, filterOutLines, waitReqCompl, runTest


class ngamsPClientTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite for NG/AMS Python Client.

    Description:
    The purpose of the Test Suite is to exercise the various features provided
    by the NG/AMS Python Client.

    Missing Test Cases:
    - This whole Test Suite should be reviewed and the missing Test Cases
      added/unnecessary Test Cases removed.
    """

    def test_Archive_1(self):
        """
        Synopsis:
        Send Archive Push Request via P-Client.
        
        Description:
        Check the handling of Archive Push Requests through the NG/AMS
        P-Client. No mime-type is submitted with the request.

        Expected Result:
        The ngamsPClient should accept the command and submit the Archive
        Push Request to the remote NG/AMS Server.

        Test Steps:
        - Start server.
        - Issue Archive Push Request (no mime-type).
        - Wait for handling to finish.
        - Check reply from server.
       
        Remarks:
        It is not checked if the file has been properly cloned. The ARCHIVE
        Command is tested in the Test Suite ngamsArchiveCmdTest.py.
        """
        self.prepExtSrv(8888, 1, 1, 1)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)
        status = client.archive("src/SmallFile.fits")
        refMsg = "Successfully handled Archive Push Request for data " +\
                 "file with URI: SmallFile.fits"
        self.checkEqual(refMsg, status.getMessage(), "Problem executing " +\
                        "Archive Push Request")


    def test_Archive_2(self):
        """
        Synopsis:
        Send Archive Pull Request via P-Client
        
        Description:
        The purpose of the test is to test the handling of Archive Push
        Requests via the NG/AMS P-Client.

        Expected Result:
        The P-Client should submit the Archive Pull Request to the
        server, which should archive the file successfully.

        Test Steps:
        - Start server.
        - Launch Archive Pull Request via the NG/AMS P-Client.
        - Check the response from the server.

        Remarks:
        It is not checked if the file has been properly cloned. The ARCHIVE
        Command is tested in the Test Suite ngamsArchiveCmdTest.py.
        """
        self.prepExtSrv(8888, 1, 1, 1)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)
        srcFileUri = "file:" + ngamsGetSrcDir() +\
                     "/ngamsTest/src/SmallFile.fits"
        status = client.archive(srcFileUri)
        refMsg = "Successfully handled Archive Pull Request for data file " +\
                 "with URI: file:" + ngamsGetSrcDir() + "/ngamsTest/src/" +\
                 "SmallFile.fits"
        self.checkEqual(refMsg, status.getMessage(), "Problem executing " +\
                        "Archive Pull Request")


    def test_Clone_1(self):
        """
        Synopsis:
        Send CLONE Command via P-Client.
        
        Description:
        Check the handling of CLONE Commands via the P-Client.

        Expected Result:
        The CLONE Command should be send to the server and should be
        executed there as expected. Wait=1.

        Test Steps:
        - Start server.
        - Archive a file.
        - Submit a CLONE Command to clone the disk hosting the archived file
          (wait=1).
        - Check the response from the server.

        Remarks:
        It is not checked if the files have been cloned as such. The CLONE
        Command is tested in the Test Suite ngamsCloneCmdTest.py.
        """
	print "Executing test_Clone_1"
        self.prepExtSrv(8888, 1, 1, 1)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)
        client.archive("src/SmallFile.fits")
        statObj = client.clone("TEST.2001-05-08T15:25:00.123",
                               "tmp-ngamsTest-NGAS-FitsStorage1-Main-1", -1)
        refMsg = "Successfully handled command CLONE"
        self.checkEqual(refMsg, statObj.getMessage(), "Problem executing " +\
                        "Archive Pull Request")

        
    def test_Clone_2(self):
        """
        Synopsis:
        Send CLONE Command via P-Client.
        
        Description:
        Submit a CLONE Command via the P-Client. Disk ID, File ID and File
        Version given. Do not wait for termination (wait=0).

        Expected Result:
        The CLONE Command should be send to the server and accepted.

        Test Steps:
        - Start server.
        - Archive a FITS file.
        - Submit a CLONE Command to clone the disk hosting the archived file.
        - Check the reply from the server indicates that the Clone Request
          was accepted for execution.

        Remarks:
        It is not checked if the files have been cloned as such. The CLONE
        Command is tested in the Test Suite ngamsCloneCmdTest.py.
        """
        self.prepExtSrv(8888, 1, 1, 1)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)
        client.archive("src/SmallFile.fits")
        statObj = client.clone("TEST.2001-05-08T15:25:00.123",
                               "tmp-ngamsTest-NGAS-FitsStorage1-Main-1", -1,
                               wait = 0)
        refMsg = "Accepted CLONE command for execution"
        self.checkEqual(refMsg, statObj.getMessage(), "Problem executing " +\
                        "Archive Pull Request")


    def test_Clone_3(self):
        """
        Synopsis:
        Send CLONE Command via P-Client.
        
        Description:
        Test the CLONE Command. Disk ID, File ID and File Version given.
        Wait for termination.

        Expected Result:
        The CLONE Command should be send to the server and accepted there.
        Disk ID, File ID and File Version given. Wait for termination (wait=1).

        Test Steps:
        - Start server.
        - Achive FITS file.
        - Clone file specifying Disk ID, File ID and File Version + wait=1.
        - Check response from server that the command was executed.

        Remarks:
        It is not checked if the files have been cloned as such. The CLONE
        Command is tested in the Test Suite ngamsCloneCmdTest.py.
        """
        self.prepExtSrv(8888, 1, 1, 1)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)
        client.archive("src/SmallFile.fits")
        statObj = client.clone("TEST.2001-05-08T15:25:00.123",
                               "tmp-ngamsTest-NGAS-FitsStorage1-Main-1", -1,
                               wait = 1)
        refMsg = "Successfully handled command CLONE"
        self.checkEqual(refMsg, statObj.getMessage(), "Problem executing " +\
                        "Archive Pull Request")
        

    def test_CorrectUsageBuf_1(self):
        """
        Synopsis:
        Test Online help feature of NG/AMS P-Client.
        
        Description:
        Check that the man-page of the P-Client is displayed on stdout when
        the tool is invoked without command line parameters.

        Expected Result:
        When the tool it invoked on the shell without parameters it should
        print out the online help opn stdout.

        Test Steps:
        - Invoke P-Client on the shell + capture output from stdout.
        - Filter output + check that output is as expected.

        Remarks:
        ...
        """
        stat, out = commands.\
                    getstatusoutput("python ../ngamsPClient/ngamsPClient.py")
        refStatFile = "ref/ngamsPClientTest_test_CorrectUsageBuf_1_1_ref"
        tmpStatFile = "tmp/ngamsPClientTest_test_CorrectUsageBuf_1_1_tmp"
        saveInFile(tmpStatFile, filterOutLines(out, ["(c) ALMA"]))
        self.checkFilesEq(refStatFile, tmpStatFile,
                          "Incorrect man-page generated")


    def test_Init_1(self):
        """
        Synopsis:
        Test that the INIT command is correctly handled via the NG/AMS
        Python API.
        
        Description:
        Test that the INIT Command can be submitted via the P-Client.

        Expected Result:
        The INTI Command should be send to the server, which should accept
        it and re-initialize.

        Test Steps:
        - Start server.
        - Submit INIT Command.
        - Check output from server that the command was successfully executed.

        Remarks:
        ...
       
        """
        self.prepExtSrv(8888, 1, 1, 1)
        status = ngamsPClient.ngamsPClient(getHostName(), 8888).init()
        refMsg = "Successfully handled command INIT"
        self.checkEqual(refMsg, status.getMessage(), "Problem executing " +\
                        "INIT Command")


    def test_Label_1(self):
        """
        Synopsis:
        Send LABEL Command via P-Client.

        Description:
        Test that it is possible to send LABEL Command via the P-Client.

        Expected Result:
        The LABEL Command should be send to the server and a label produced.
        
        Test Steps:
        - Start server.
        - Submit LABEL Command specifying Slot ID/Host ID.
        - Check response from the server.

        Remarks:
        ...
        """
        self.prepExtSrv(8888, 1, 1, 1)
        status = ngamsPClient.ngamsPClient(getHostName(), 8888).\
                 label("1", getHostName())
        refMsg = "Successfully handled command LABEL"
        self.checkEqual(refMsg, status.getMessage(), "Problem executing " +\
                        "LABEL Command")


    def test_Online_1(self):
        """
        Synopsis:
        Handling of ONLINE Command via P-Client.
        
        Description:
        The purpose of the test is to test the handling of ONLINE Commands
        via the P-Client.

        Expected Result:
        The ONLINE Command should be send to the server by the P-Client.
        The server should fo Online.

        Test Steps:
        - Start server (Auto Online=0).
        - Check on response from server that command was successfully handled.

        Remarks:
        ...
        """
        self.prepExtSrv(8888, 1, 1, 0)
        status = ngamsPClient.ngamsPClient(getHostName(), 8888).online()
        refMsg = "Successfully handled command ONLINE"
        self.checkEqual(refMsg, status.getMessage(), "Problem executing " +\
                        "ONLINE Command")

    
    def test_Register_1(self):
        """
        Synopsis:
        Send REGISTER Command via P-Client (wait=0).

        Description:
        Test that the REGISTER command is correctly handled via the NG/AMS
        Python API.

        Parameter wait specified = 0 (= there is not waited till command
        execution has finished).

        Expected Result:
        The REGISTER Command should be send to the server, which should
        start registering the files found under the given path. A response
        should be returned before the actual execution of the REGISTER
        Command is initiated.

        Test Steps:
        - Start server.
        - Copy file onto one the NGAS Disks.
        - Send REGISTER Command via the P-Client to initiate the REGISTER
          Command (wait=0).
        - Check response from  the server.

        Remarks:
        ...
        """
        self.prepExtSrv(8888, 1, 1, 1)
        trgFile = "/tmp/ngamsTest/NGAS/FitsStorage1-Main-1/SmallFile.fits"
        shutil.copy("src/SmallFile.fits", trgFile)
        status = ngamsPClient.ngamsPClient(getHostName(), 8888).\
                 register(trgFile)
        refMsg = "Successfully handled command REGISTER"
        self.checkEqual(refMsg, status.getMessage(), "Problem executing " +\
                        "REGISTER Command")

        
    def test_Register_2(self):
        """
        Synopsis:
        Send REGISTER Command via P-Client (wait=1).

        Description:
        Test that the REGISTER command is correctly handled via the NG/AMS
        Python API.

        Parameter wait specified = 1 (= there waited till command execution
        has finished).

        Expected Result:
        The REGISTER Command should be send to the server, which should
        start registering the files found under the given path. A response
        should be returned when the execution finishes.

        Test Steps:
        - Start server.
        - Copy file onto one the NGAS Disks.
        - Send REGISTER Command via the P-Client to initiate the REGISTER
          Command (wait=1).
        - Check response from  the server.

        Remarks:
        ...
        """
        self.prepExtSrv(8888, 1, 1, 1)
        trgFile = "/tmp/ngamsTest/NGAS/FitsStorage1-Main-1/SmallFile.fits"
        shutil.copy("src/SmallFile.fits", trgFile)
        status = ngamsPClient.ngamsPClient(getHostName(), 8888).\
                 register(trgFile, wait = 1)
        refMsg = "Successfully handled command REGISTER"
        self.checkEqual(refMsg, status.getMessage(), "Problem executing " +\
                        "REGISTER Command")


    def test_RemDisk_1(self):
        """
        Synopsis:
        Handling of REMDISK Command via P-Client (execute=1).
        
        Description:
        Test that the REMDISK command is correctly handled via the NG/AMS
        Python API.

        Expected Result:
        The REMDISK Command should be send to the server and executed
        accordingly.

        Test Steps:
        - Start server.
        - Submit REMDISK Command to remove one of the registered disks
          (execute=1).
        - Check response from the server.

        Remarks:
        ...
        """
        self.prepExtSrv(8888, 1, 1, 1)
        status = ngamsPClient.ngamsPClient(getHostName(), 8888).\
                 remDisk("tmp-ngamsTest-NGAS-FitsStorage1-Main-1", 1)
        refMsg = "NGAMS_INFO_DEL_DISK:4043:INFO: Successfully deleted " +\
                 "info for disk. Disk ID: " +\
                 "tmp-ngamsTest-NGAS-FitsStorage1-Main-1."
        self.checkEqual(refMsg, status.getMessage(), "Problem executing " +\
                        "REMDISK Command")


    def test_RemFile_1(self):
        """
        Synopsis:
        Send REMFILE Command via P-Client.
        
        Description:
        Test correct handling of the REMFILE command via the P-Client.

        Expected Result:
        The REMFILE Command should be send to the server, which should
        execute it accordingly.

        Test Steps:
        - Start server.
        - Archive a FITS file.
        - Clone disk hosting the file.
        - Issue REMFILE Command via the P-Client specifying to remove the
          first copy of the file.
        - Check the response from the server.

        Remarks:
        ...
        """
	print "Executing test_RemFile_1"
        self.prepExtSrv(8888, 1, 1, 1)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)
        client.archive("src/SmallFile.fits")
        status = client.clone("", "tmp-ngamsTest-NGAS-FitsStorage1-Main-1", -1)
        waitReqCompl(client, status.getRequestId())
        status = client.remFile("tmp-ngamsTest-NGAS-FitsStorage1-Main-1",
                                "TEST.2001-05-08T15:25:00.123", -1)
        refMsg = "NGAMS_INFO_FILE_DEL_STAT:4040:INFO: File deletion status. "+\
                 "Files Selected: 1, Files Deleted: 0, " +\
                 "Failed File Deletions: 0."
        self.checkEqual(refMsg, status.getMessage(), "Problem executing " +\
                        "REMFILE Command")


    def test_Retrieve2File_1(self):
        """
        Synopsis:
        Handling of RETRIEVE Command via P-Client.
        
        Description:
        Test correct handling of the RETRIEVE command from the NG/AMS
        Python API (no processing).

        Expected Result:
        The RETRIEVE Command should be send to the server requesting for
        an archive file, which subsequently should be send back to the client
        and stored on the local disk.

        Test Steps:
        - Start server.
        - Archive a file.
        - Retrieve the archived file into a local file.
        - Check the response from the server.
        - Check that the requested file has been successfully stored on the
          local disk.

        Remarks:
        ...
        """
	print "Executing test_Retrieve2File_1"
        self.prepExtSrv(8888, 1, 1, 1)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)
        client.archive("src/SmallFile.fits")
        trgDir = "tmp"
        status = client.retrieve2File("TEST.2001-05-08T15:25:00.123",1,trgDir)
        refMsg = "Successfully handled request"
        self.checkEqual(refMsg, status.getMessage(), "Problem executing " +\
                        "RETRIEVE Command")
        tmpFile = "tmp/TEST.2001-05-08T15:25:00.123.fits.gz"
        refFile = "src/SmallFile.fits.gz"
        self.checkFilesEq(refFile, tmpFile, "Retrieved file incorrect")
        

    def test_ServerMultiplexing_01(self):
        """
        Synopsis:
        Test server multiplexing feature of the Python-API/Client.
        
        Description:
        The purpose of the test is to verify the proper functioning of the
        server context switching (multiplexing) provided by the Python-
        API/Client. With this feature a list of servers + ports is given, and
        the C-API switches between the specified servers.

        A STATUS Command will be sent out to test the server multiplexing.

        Expected Result:
        After a limited number of attempts, all servers available, should
        have been contacted by the Python-Client.

        Test Steps:
        - Start simulated cluster with 5 units.
        - Submit STATUS Command invoking the Python-Client with the list of
          servers.
        - Verify that within 100 attempts, all servers have been contacted.

        Remarks:
        ...

        Test Data:
        ...
        """
        nodeDic = {}
        for n in range(5): nodeDic["%s:%d" % (getHostName(), (8000 + n))] = 0
        nodeList = []
        for node in nodeDic.keys():
            nodeList.append([8000 + len(nodeList), None, None, getHostName()])
        self.prepCluster("src/ngamsCfg.xml", nodeList)
        noOfNodes = len(nodeDic.keys())
        nodeCount = 0
        srvList = ""
        for node in nodeDic.keys(): srvList += "%s," % node
        client = ngamsPClient.ngamsPClient().parseSrvList(srvList[:-1])
        for n in range(100):
            status = client.status()
            if (nodeDic[status.getHostId()] == 0):
                nodeDic[status.getHostId()] = 1
                nodeCount += 1
                if (nodeCount == noOfNodes): break
        if (nodeCount != noOfNodes):
            self.fail("Not all specified NGAS Nodes were contacted " +\
                      "within 100 attempts")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsPClientTest.test_Archive_1"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
