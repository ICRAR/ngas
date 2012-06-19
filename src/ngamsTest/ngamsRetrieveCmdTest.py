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
# "@(#) $Id: ngamsRetrieveCmdTest.py,v 1.7 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  23/04/2002  Created
#

"""
This module contains the Test Suite for the RETRIEVE Command.
"""

import os, sys
from   ngams import *
from   ngamsTestLib import *
import ngamsRetrieveCmd


class ngamsRetrieveCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Exercise the RETRIEVE Command.

    Description:
    The purpose of the Test Suite is to exercise the RETRIEVE Command under
    normal and abnormal conditions.

    The nominal case, where a file is retrieved from a node where it is
    residing is tested, but also the proxy mode where a node is acting as
    proxy and retrieving the file from another node is tested. Also the
    HTTP re-direction mode for retrieving is tested.

    Missing Test Cases:
    - Review this entire Test Suite and add important, missing Test Cases.
    - TEST RETRIEVAL OF T-FITS FILES VIA A PROXY!
    - Test RETRIEVE + processing via DPPI.
    - Test RETRIEVE + processing via DPPI, DPPI not available.
    - Test RETRIEVE + processing via DPPI, DPPI not available/Proxy Mode.
    - Test that the scheme for determining the best suitable file is working
      (create dummy entries in ngas_files for files on other domains etc.).
    """

    def test_RetrieveCmd_1(self):
        """
        Synopsis:
        Retrieve file from server hosting the file.
        
        Description:
        The purpose of the test is to test the case where it is attempted
        to retrieve an archived file directly from the unit hosting the file.

        Expected Result:
        The server should locate the file and send it back to the requestor.

        Test Steps:
        - Start server.
        - Archive FITS file.
        - Retrieve the file and store it in a local file.
        - Check reply from the server indiates that the request was handled
          successfully.
        - Check that the file on disk has been successfully retrieved.

        Remarks:
        ...
        """
        self.prepExtSrv(8888, 1, 1, 1)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)
        client.archive("src/SmallFile.fits")

        # Retrieve the file.
        trgFile = "tmp/test_RetrieveCmd_1_1_tmp"
        status = client.retrieve2File("TEST.2001-05-08T15:25:00.123",1,trgFile)

        # Check reply.
        refStatFile = "ref/ngamsRetrieveCmdTest_test_RetrieveCmd_1_1_ref"
        tmpStatFile = "tmp/ngamsRetrieveCmdTest_test_RetrieveCmd_1_1_tmp"
        saveInFile(tmpStatFile, filterDbStatus1(status.dumpBuf(0, 1, 1)))
        self.checkFilesEq(refStatFile, tmpStatFile, 
                          "Incorrect status for RETRIEVE Command/Normal " +\
                          "Execution")

        # Check file retrieved.
        refFile = "src/SmallFile.fits.Z"
        self.checkFilesEq(refFile, trgFile, "Retrieved file incorrect")


    def test_RetrieveCmd_2(self):
        """
        Synopsis:
        Attempt to retrieve non-existing file (wrong version).
        
        Description:
        Test the case where it is attempted to retrieve a non-exiting file.
        An appropriate error response should be generated.

        Expected Result:
        The server should identify that the file is not found in the NGAS
        Archive and should return a response indicating this.

        Test Steps:
        - Start server.
        - Archive FITS file.
        - Retrieve file with same File ID as archived file but with another
          version than the one allocated to the file.
        - Check the error response from the server.

        Remarks:
        ...
        """
        self.prepExtSrv(8888, 1, 1, 1)
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)
        client.archive("src/SmallFile.fits")

        # Retrieve the file.
        trgFile = "tmp/test_RetrieveCmd_1_1_tmp"
        status = client.retrieve2File("TEST.2001-05-08T15:25:00.123",2,trgFile)

        # Check reply.
        refStatFile = "ref/ngamsRetrieveCmdTest_test_RetrieveCmd_2_1_ref"
        tmpStatFile = "tmp/ngamsRetrieveCmdTest_test_RetrieveCmd_2_1_tmp"
        saveInFile(tmpStatFile, filterDbStatus1(status.dumpBuf(0, 1, 1)))
        self.checkFilesEq(refStatFile, tmpStatFile, 
                          "Incorrect status for RETRIEVE Command/Normal " +\
                          "Execution")


    def test_RetrieveCmd_3(self):
        """
        Synopsis:
        Retrieve file from NGAS Cluster sub-node.
        
        Description:
        Test that a file stored on a sub-node is located and returned to the
        requestor by the contacted server acting as proxy is Proxy Mode is
        eneabled.
        
        Expected Result:
        The contacted server should locate the file, forward the request to
        the node hosting the file, and should send back the file to the
        requestor.

        Test Steps:
        - Start simulated cluster with master and sub-node.
        - Archive file onto sub-node.
        - Submit Retrieve Request to Master Node to retrieved the archived
          file.
        - Check the response from the Master Node.
        - Check that the file has been retrieved as expected.

        Remarks:
        ...
        """
        self.prepCluster("src/ngamsCfg.xml",
                         [[8000, None, None, getClusterName()],
                          [8011, None, None, getClusterName()]])
        # Archive file into sub-node (port=8011).
        ngamsPClient.ngamsPClient(getHostName(),
                                  8011).pushFile("src/TinyTestFile.fits")
        
        # Retrieve a file.
        trgFile = "tmp/test_RetrieveCmd_3_1_tmp"
        client = ngamsPClient.ngamsPClient(getHostName(), 8000)
        status = client.retrieve2File("NCU.2003-11-11T11:11:11.111",1,trgFile)

        # Check reply.
        refStatFile = "ref/ngamsRetrieveCmdTest_test_RetrieveCmd_3_1_ref"
        tmpStatFile = "tmp/ngamsRetrieveCmdTest_test_RetrieveCmd_3_1_tmp"
        saveInFile(tmpStatFile, filterDbStatus1(status.dumpBuf(0, 1, 1)))
        self.checkFilesEq(refStatFile, tmpStatFile, 
                          "Incorrect status for RETRIEVE Command/Cluster " +\
                          "Retrieval")

        # Check the retrieved file (checksum).
        refFile = "src/TinyTestFile.fits.Z"
        self.checkFilesEq(refFile, trgFile, "Retrieved file incorrect")


    def test_RetrieveCmd_4(self):
        """
        Synopsis:
        Retrieve Log File from NMU/Sub-Node.

        Description:
        Retrieve the log file from the master node. Retrieve the log file via
        a master node acting as proxy for a sub-node.

        Expected Result:
        The log file from the Master Node should be returned when no host_id
        is given. When specifying a host_id, the contacted server should
        identify that the requested log file is located on another node and
        should forward the request to the node in question. The contacted node
        should send back the file to the requestor.

        Test Steps:
        - Prepare simulated cluster.
        - Retrieve NG/AMS Log File from master node.
        - Check that the correct log file has been returned.
        - Retrieve NG/AMS Log File from sub-node via master node.
        - Check that the correct log file has been returned.

        Remarks:
        ...
        """
        self.prepCluster("src/ngamsCfg.xml",
                         [[8000, None, None, getClusterName()],
                          [8011, None, None, getClusterName()]])
        
        # Retrieve Log File from the Main-Node.
        trgFile = "tmp/test_RetrieveCmd_4_1_tmp"
        client = ngamsPClient.ngamsPClient()
        client.sendCmdGen(getHostName(), 8000, "RETRIEVE",
                          1, trgFile, [["ng_log", ""]])
        fo = open(trgFile)
        logBuf = fo.read()
        fo.close()
        refStr = "NG/AMS HTTP Server ready (Host: %s - Port: 8000)" %\
                 getHostName()
        if (logBuf.find(refStr) == -1):
            self.fail("Illegal Log File retrieved from " + getHostName())

        # Retrieve Log File from a Sub-Node via the Main Node.
        trgFile = "tmp/test_RetrieveCmd_4_2_tmp"
        statObj = client.sendCmdGen(getHostName(), 8000, "RETRIEVE",
                                    1, trgFile, [["ng_log", ""],
                                                 ["host_id", getNcu11()]])
        fo = open(trgFile)
        logBuf = fo.read()
        fo.close()
        refStr = "NG/AMS HTTP Server ready (Host: %s - Port: 8011" %\
                 getHostName()
        if (logBuf.find(refStr) == -1):
            self.fail("Illegal Log File retrieved from %s/%s" %\
                      (getNcu11(), getNmu()))


    def test_RetrieveCmd_5(self):
        """
        Synopsis:
        Retrieve Cfg. File from NMU/Sub-Node.
        
        Description:
        Test that the NG/ASM Configuration can be returned from a master
        node and a sub-node via a master acting as proxy.

        Expected Result:
        When no host_id is specified, the cfg. from the contacted node should
        be returned. When a host_id is specified, the cfg. from the specified
        node should be returned via the contacted node acting as master.

        Test Steps:
        - Prepare simulated cluster.
        - Request cfg. from master.
        - Check that correct cfg. has been returned.
        - Request cfg. from sub-node via the master.
        - Check that correct cfg. has been returned.

        Remarks:
        ...
       
        """
        self.prepCluster("src/ngamsCfg.xml",
                         [[8000, None, None, getClusterName()],
                          [8011, None, None, getClusterName()]])
        
        # Retrieve Log File from the Main-Node.
        trgFile = "tmp/test_RetrieveCmd_5_1_tmp"
        client = ngamsPClient.ngamsPClient()
        client.sendCmdGen(getHostName(), 8000, "RETRIEVE",
                          1, trgFile, [["cfg", ""]])
        fo = open(trgFile)
        logBuf = fo.read()
        fo.close()
        refStr = "TEST CONFIG: %s:8000" % getHostName()
        if (logBuf.find(refStr) == -1):
            self.fail("Illegal Cfg. File retrieved from " + getHostName())

        # Retrieve Log File from a Sub-Node via the Main Node.
        trgFile = "tmp/test_RetrieveCmd_5_2_tmp"
        statObj = client.sendCmdGen(getHostName(), 8000, "RETRIEVE",
                                    1, trgFile, [["cfg", ""],
                                                 ["host_id", getNcu11()]])
        fo = open(trgFile)
        logBuf = fo.read()
        fo.close()
        refStr = "TEST CONFIG: %s:8011" % getHostName()
        if (logBuf.find(refStr) == -1):
            self.fail("Illegal Cfg. File retrieved from %s via %s" %\
                      (getNcu11(), getNmu()))


    def test_RetrieveCmd_6(self):
        """
        Synopsis:
        Retrieve Internal File from NMU/Sub-Node.
        
        Description:
        With the RETRIEVE?internal Retrieve Request, it is possible to retrieve
        files not being archived files and readable for the user running NGAS.
        This test exercises this feature.

        Expected Result:
        The specified internal file should be located by the server and send
        back to the requestor. The internal file is first retrieved from the
        master node, then from a sub-node via the master acting as proxy.

        Test Steps:
        - Prepare simulated cluster with master and sub-node.
        - Send RETRIEVE Command to retrieve /etc/hosts from the master.
        - Check that the proper file has been retrieved.
        - Send RETRIEVE Command to retrieve /etc/hosts from the sub-node
          via the master, acting as proxy.
        - Check that the proper file has been retrieved.

        Remarks:
        ...
        """
        self.prepCluster("src/ngamsCfg.xml",
                         [[8000, None, None, getClusterName()],
                          [8011, None, None, getClusterName()]])
        
        # Retrieve Log File from the Main-Node.
        trgFile = "tmp/test_RetrieveCmd_5_1_tmp"
        client = ngamsPClient.ngamsPClient()
        client.sendCmdGen(getHostName(), 8000, "RETRIEVE",
                          1, trgFile, [["internal", "/etc/hosts"]])
        fo = open(trgFile)
        logBuf = fo.read()
        fo.close()
        if (logBuf.find("localhost") == -1):
            self.fail("Illegal internal file retrieved from %s: %s" %\
                      (getHostName(), "/etc/hosts"))

        # Retrieve Log File from a Sub-Node via the Main Node.
        trgFile = "tmp/test_RetrieveCmd_5_2_tmp"
        statObj = client.sendCmdGen(getHostName(), 8000, "RETRIEVE",
                                    1, trgFile, [["internal", "/etc/passwd"],
                                                 ["host_id", getNcu11()]])
        fo = open(trgFile)
        logBuf = fo.read()
        fo.close()
        if (logBuf.find("root:x:0:0:") == -1):
            self.fail("Illegal internal file retrieved from %s via %s: %s" %\
                      (getNcu11(), getNmu(), "/etc/passwd"))


    def test_RetrieveCmd_7(self):
        """
        Synopsis:
        Rerieve file from cluster, Online/Suspended and Offline files
        contained in archive.
        
        Description:
        The purpose of the test is to verify that the problem that there are
        Online and Offline files in the archive and thus the cross-checking
        of the number of files for which info was queried from the NGAS DB (in
        ngamsDb.dumpFileInfo()), detected erroneously a discrepancy.

        Expected Result:
        The RETRIEVE Command should execute successfully. The check of the
        possible files matching the query should not produce and error.

        Test Steps:
        - Prepare simulated cluster with three nodes. One node should
          suspend itself after few seconds.
        - Archive the same file onto the three nodes 3 times.
        - Make one node exit.
        - Wait till the node suspending itself has suspended itself.
        - Send RETRIEVE Command to retrieve a file given by its File ID.
        - Check that response from the server is OK.
        - Send RETRIEVE Command to retrieve a file given by its File ID
          and its File Version.
        - Check that the response from the server is OK.
        - Send RETRIEVE Command to retrieve a file given by the Disk ID,
          the File ID and the File Version.
        - Check that the response from the server is OK.

        Remarks:
        ...
        """
        nodes = [getHostName() + ":8000", getHostName() + ":8001",
                 getHostName() + ":8002"]
        suspPars = [["NgamsCfg.HostSuspension[1].IdleSuspension", "1"],
                    ["NgamsCfg.JanitorThread[1].SuspensionTime", "0T00:00:05"]]
        envDic = self.prepCluster("src/ngamsCfg.xml",
                                  [[8000, None, None, getClusterName()],
                                   [8001, None, None, getClusterName()],
                                   [8002, None, None, getClusterName(),
                                    suspPars]])
        for portNo in [8000, 8001, 8002]:
            for n in range(3):
                sendPclCmd(port=portNo).archive("src/SmallFile.fits")
        sendPclCmd(port=8001).offline()
        sendPclCmd(port=8001).exit()
        subNode2 = nodes[2]
        waitTillSuspended(self, envDic[subNode2][1], subNode2, 45, nodes)
        client = ngamsPClient.ngamsPClient(getHostName(), 8000)

        # Retrieve file (File ID).
        fileId = "TEST.2001-05-08T15:25:00.123"
        statObj = client.retrieve2File(fileId)
        refStatFile = "ref/ngamsRetrieveCmdTest_test_RetrieveCmd_7_1_ref"
        tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile,
                          "Unexpected response returned to RETRIEVE Command")

        # Retrieve file (File ID + File Version).
        statObj = client.retrieve2File(fileId, fileVersion=2)
        tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile,
                          "Unexpected response returned to RETRIEVE Command")

        # Retrieve file (Disk ID + File ID + File Version).
        info(1,"TODO: Implement this case.")


    def test_IntFolder_01(self):
        """
        Synopsis:
        Retrieve TOC an internal folder from contacted node.
        
        Description:
        The purpose of this test is to verify the proper functioning of
        retrieving folder status XML document using the
        RETRIEVE?internal=<Folder>

        Expected Result:
        The contacted server should detect that the folder in question is a
        local folder. It should detect that an XML list of the contents of the
        folder is requested and it should return this.

        Test Steps:
        - Start server.
        - Submit RETRIEVE?internal=/tmp/ngamsTest/NGAS/FitsStorage1-Main-1/
        - Check that the contents of the return XML document is as expected.

        Remarks:
        ...

        Test Data:
        ...
        """
        self.prepExtSrv(8888, 1, 1, 1)
        sendPclCmd().archive("src/SmallFile.fits")
        dirName = "/tmp/ngamsTest/NGAS/FitsStorage1-Main-1"
        stat = sendPclCmd().sendCmd(NGAMS_RETRIEVE_CMD,
                                  pars=[["internal", dirName]])
        refStatFile = "ref/ngamsRemFileCmdTest_test_IntFolder_01_01_ref"
        filtStat = filterDbStatus1(stat.dumpBuf(), filterTags=["FileSize:",
                                                               "Owner:",
                                                               "Group:"])
        tmpStatFile = saveInFile(None, filtStat)
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status for " +\
                          "RETRIEVE Command/internal folder, local", sort=1)


    def test_IntFolder_02(self):
        """
        Synopsis:
        Retrieve an internal folder from contacted node/Proxy Mode.
        
        Description:
        The purpose of this test is to verify the proper functioning of
        retrieving folder status XML document using the
        RETRIEVE?internal=<Folder>&host_id=<Host>

        Expected Result:
        The contacted server should detect that the folder in question is a
        on another node. It should forward the request to this node and return
        the response from this node.

        Test Steps:
        - Start simulated cluster.
        - Submit RETRIEVE?internal=/tmp/ngamsTest/NGAS/FitsStorage1-Main-1/&\
                          host_id=<Host>
        - Check that the contents of the return XML document is as expected.

        Remarks:
        ...

        Test Data:
        ...
        """
        self.prepCluster("src/ngamsCfg.xml",
                         [[8000, None, None, getClusterName()],
                          [8011, None, None, getClusterName()]])
        sendPclCmd(port=8011).archive("src/SmallFile.fits")
        dirName = "/tmp/ngamsTest/NGAS:8011/FitsStorage1-Main-1"
        stat = sendPclCmd(port=8000).sendCmd(NGAMS_RETRIEVE_CMD,
                                             pars=[["internal", dirName],
                                                   ["host_id", "%s:8011" %\
                                                    getHostName()]])
        refStatFile = "ref/ngamsRemFileCmdTest_test_IntFolder_02_01_ref"
        filtStat = filterDbStatus1(stat.dumpBuf(), filterTags=["FileSize:",
                                                               "Owner:",
                                                               "Group:"])
        tmpStatFile = saveInFile(None, filtStat)
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status for " +\
                          "RETRIEVE Command/internal folder, proxy", sort=1)


    def test_HttpRedirection_01(self):
        """
        Synopsis:
        Test the HTTP redirection, contacted node redirects to sub-node.
        
        Description:
        The purpose of this test case is to verify that the HTTP redirection
        works in connection with the RETRIEVE Command.

        Expected Result:
        The contacted node should detect that the file requested is stored
        on another node (sub-node). It should resolve the address and return
        an HTTP re-direction response to the client Python-API, which
        internally will pick up the file.

        Test Steps:
        - Start simulated cluster (Proxy Mode: Off).
        - Archive file onto MNU, archive file onto NCU.
        - Submit RETRIEVE to retrieve file on NCU.
        - Check that an HTTP redirection response was returned such
          that the file was retrieved directly from the sub-node.

        Remarks:
        ...

        Test Data:
        ...
        """
        nmuCfgPars = [["NgamsCfg.Server[1].ProxyMode", "0"]]
        self.prepCluster("src/ngamsCfg.xml",
                         [[8000, None, None, getClusterName(), nmuCfgPars],
                          [8011, None, None, getClusterName()]])
        sendPclCmd(port=8000).archive("src/SmallFile.fits")
        stat = sendPclCmd(port=8011).archive("src/SmallFile.fits")
        fileId = "TEST.2001-05-08T15:25:00.123"
        stat = sendPclCmd(port=8000).sendCmd(NGAMS_RETRIEVE_CMD,
                                             pars=[["file_id", fileId]])
        refStatFile = "ref/ngamsRemFileCmdTest_test_HttpRedirection_01_ref"
        tmpStatFile = saveInFile(None, filterDbStatus1(stat.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status for " +\
                          "RETRIEVE Command/HTTP redirection")
        # Check that a log entry in the log file of the NMU is found.
        grepCmd = "grep '\[INFO\] NGAMS_INFO_REDIRECT' " +\
                  "/tmp/ngamsTest/NGAS:8000/log/LogFile.nglog"
        stat, out = commands.getstatusoutput(grepCmd)
        if (out.find("Redirection URL:") == -1):
            self.fail("Unexpected/missing HTTP redirection log entry: %s"%out)


    def test_DppiProc_01(self):
        """
        Synopsis:
        Test the proper execution of DPPI processing/result in file. 
        
        Description:
        When requesting a file from NGAS, it is possible to specify to have
        the file processed by a DPPI. The result can either be stored in a
        file or contained in the buffer when handed over to the NG/AMS Server.

        Expected Result:
        The DPPI should be invoked the file to be retrieved and the result
        (stored by the DPPI in a file), properly sent back to the client.

        Test Steps:
        - Start a server configured to invoke a test DPPI (header extraction).
        - Archive a file.
        - Retrieve the file, specifying to apply the DPPI on it.
        - Check that the file has been processed as expected.

        Remarks:
        ...

        Test Data:
        ...
        """
        tmpCfgFile = genTmpFilename("ngamsRetrieveCmdTest")
        cfg = ngamsConfig.ngamsConfig().load("src/ngamsCfg.xml")
        cfg.storeVal("NgamsCfg.Processing[1].PlugIn[1].Name", "ngamsTestDppi1")
        cfg.storeVal("NgamsCfg.Processing[1].PlugIn[1].PlugInPars",
                     "TAG=test_DppiProc_01,TARGET=FILE")
        cfg.save(tmpCfgFile, 0)
        self.prepExtSrv(8888, 1, 1, 1, cfgFile=tmpCfgFile)
        sendPclCmd(port=8888).archive("src/SmallFile.fits")
        # Retrieve the file specifying to apply the DPPI.
        outFile = genTmpFilename("test_DppiProc_01")
        cmdPars = [["file_id", "TEST.2001-05-08T15:25:00.123"],
                   ["processing", "ngamsTestDppi1"],
                   ["test_suite", "ngamsRetrieveCmdTest"],
                   ["test_case", "test_DppiProc_01"]]                   
        stat = ngamsPClient.ngamsPClient().sendCmdGen(getHostName(), 8888,
                                                      NGAMS_RETRIEVE_CMD,
                                                      outputFile=outFile,
                                                      pars=cmdPars)
        refStatFile = "ref/ngamsRemFileCmdTest_test_DppiProc_01_01_ref"
        self.checkFilesEq(refStatFile, outFile, "Incorrect status for " +\
                          "RETRIEVE Command/DPPI Processing, result in file")
        

    def test_DppiProc_02(self):
        """
        Synopsis:
        Test the proper execution of DPPI processing/result in buffer. 
                
        Description:
        When requesting a file from NGAS, it is possible to specify to have
        the file processed by a DPPI. The result can either be stored in a
        file or contained in the buffer when handed over to the NG/AMS Server.

        Expected Result:
        The DPPI should be invoked the file to be retrieved and the result
        (stored by the DPPI in a buffer), properly sent back to the client.

        Test Steps:
        - Start a server configured to invoke a test DPPI (header extraction).
        - Archive a file.
        - Retrieve the file, specifying to apply the DPPI on it. Result will
          be stored in a buffer in memory.
        - Check that the file has been processed as expected.

        Remarks:
        ...

        Test Data:
        ...
        """
        tmpCfgFile = genTmpFilename("ngamsRetrieveCmdTest")
        cfg = ngamsConfig.ngamsConfig().load("src/ngamsCfg.xml")
        cfg.storeVal("NgamsCfg.Processing[1].PlugIn[1].Name", "ngamsTestDppi1")
        cfg.storeVal("NgamsCfg.Processing[1].PlugIn[1].PlugInPars",
                     "TAG=test_DppiProc_02,TARGET=BUFFER")
        cfg.save(tmpCfgFile, 0)
        self.prepExtSrv(8888, 1, 1, 1, cfgFile=tmpCfgFile)
        sendPclCmd(port=8888).archive("src/SmallFile.fits")
        # Retrieve the file specifying to apply the DPPI.
        outFile = genTmpFilename("test_DppiProc_02")
        cmdPars = [["file_id", "TEST.2001-05-08T15:25:00.123"],
                   ["processing", "ngamsTestDppi1"],
                   ["test_suite", "ngamsRetrieveCmdTest"],
                   ["test_case", "test_DppiProc_02"]]                   
        stat = ngamsPClient.ngamsPClient().sendCmdGen(getHostName(), 8888,
                                                      NGAMS_RETRIEVE_CMD,
                                                      outputFile=outFile,
                                                      pars=cmdPars)
        refStatFile = "ref/ngamsRemFileCmdTest_test_DppiProc_02_01_ref"
        self.checkFilesEq(refStatFile, outFile, "Incorrect status for " +\
                          "RETRIEVE Command/DPPI Processing, result in buffer")


    def test_DppiProc_03(self):
        """
        Synopsis:
        Test the proper execution of DPPI proc./result in file/Proxy Mode. 
        
        Description:
        When requesting a file from NGAS, it is possible to specify to have
        the file processed by a DPPI. The result can either be stored in a
        file or contained in the buffer when handed over to the NG/AMS Server.

        Expected Result:
        The DPPI should be invoked the file to be retrieved and the result
        (stored by the DPPI in a file), properly sent back to the client.
        This is tested while retrieving the DPPI processed via a server acting
        as proxy.

        Test Steps:
        - Start a simulated cluster.
        - Archive a file into sub-node.
        - Retrieve the file via the master, specifying to apply the DPPI on it.
        - Check that the file has been processed as expected.

        Remarks:
        ...

        Test Data:
        ...
        """
        ncuCfgPars = [["NgamsCfg.Processing[1].PlugIn[1].Name",
                       "ngamsTestDppi1"],
                      ["NgamsCfg.Processing[1].PlugIn[1].PlugInPars",
                       "TAG=test_DppiProc_02,TARGET=FILE"]]
        self.prepCluster("src/ngamsCfg.xml",
                         [[8000, None, None, getClusterName()],
                          [8011, None, None, getClusterName(), ncuCfgPars]])
        sendPclCmd(port=8011).archive("src/SmallFile.fits")
        # Retrieve the file specifying to apply the DPPI.
        outFile = genTmpFilename("test_DppiProc_03")
        cmdPars = [["file_id", "TEST.2001-05-08T15:25:00.123"],
                   ["processing", "ngamsTestDppi1"],
                   ["test_suite", "ngamsRetrieveCmdTest"],
                   ["test_case", "test_DppiProc_03"]]                   
        stat = ngamsPClient.ngamsPClient().sendCmdGen(getHostName(), 8000,
                                                      NGAMS_RETRIEVE_CMD,
                                                      outputFile=outFile,
                                                      pars=cmdPars)
        refStatFile = "ref/ngamsRemFileCmdTest_test_DppiProc_03_01_ref"
        self.checkFilesEq(refStatFile, outFile, "Incorrect status for " +\
                          "RETRIEVE Command/DPPI Processing, Proxy Mode, " +\
                          "result in buffer")


    def test_VolumeDir_01(self):
        """
        Synopsis:
        Grouping of data volumes under the Volume Dir in the NGAS Root Dir.
        
        Description:
        See ngamsArchiveCmdTest.test_VolumeDir_01().

        This tests verifies that the files archived into this structure can
        be received.

        Expected Result:
        When the server goes Online, it should accept the given directory
        structure and it should be possible to archive files into this
        structureand subsequently to receive them.

        Test Steps:
        - Create the volume dirs from an existing structure.
        - Start server with configuration specifying the Volumes Dir in which
          all volumes will be hosted.
        - Archive a FITS file.
        - Retrieve the files and check that they are OK.

        Remarks:
        ...
        """
        # Create basic structure.
        ngasRootDir = "/tmp/ngamsTest/NGAS/"
        checkCreatePath(ngasRootDir)
        tarCmd = "tar zxvf src/volumes_dir.tar.gz"
        stat, out = commands.getstatusoutput(tarCmd)
        rmFile(os.path.normpath("%s/volumes") % ngasRootDir)
        mvCmd = "mv volumes %s" % ngasRootDir
        stat, out = commands.getstatusoutput(mvCmd)
        
        # Create configuration, start server.
        cwd = os.getcwd()
        configFile = os.path.normpath(cwd+"/src/ngamsCfg_VolumeDirectory.xml")
        self.prepExtSrv(delDirs=0, cfgFile=configFile)

        # Archive a file.
        stat = sendPclCmd().archive("src/SmallFile.fits")
        tmpStatFile = saveInFile(None, filterDbStatus1(stat.dumpBuf()))
        refStatFile = "ref/ngamsRetrieveCmdTest_test_VolumeDir_01_01_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status " +\
                          "message from NG/AMS Server")
        
        # Check that the target files have been archived in their
        # appropriate locations.
        trgFile = "tmp/test_VolumeDir_01_tmp"
        refFile = "src/SmallFile.fits.Z"
        client = ngamsPClient.ngamsPClient(getHostName(), 8888)
        client.retrieve2File("TEST.2001-05-08T15:25:00.123", 1, trgFile)
        self.checkFilesEq(refFile, trgFile, "Retrieved file incorrect")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsRetrieveCmdTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
