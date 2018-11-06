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
# "@(#) $Id: ngamsCClientTest.py,v 1.8 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  10/11/2003  Created
#
"""
This module contains the Test Suite for the NG/AMS C-Client and C-API.
"""

import os
import subprocess
import time
import unittest

from ngamsLib import ngamsStatus, utils
from ngamsLib.ngamsCore import getHostName, execCmd
from ..ngamsTestLib import ngamsTestSuite, has_program, save_to_tmp, \
    genTmpFilename


STD_DISK_STAT_FILT = ["AccessDate", "AvailableMb", "CreationDate", "Date",
                      "HostId", "IngestionDate", "InstallationDate",
                      "ModificationDate", "TotalDiskWriteTime", "Version"]

def _execCClient(unpackXmlStat = 1,
                 pars = []):
    """
    Execute the NG/AMS C-Client on the shell.

    cmdLineParsDic:   Dictionary with command line parameters (dictionary).

    pars:             Extra parameters for invoking NGAMS C-Client (list).

    Returns:          List with status objects or stdout output from c-client:

                        [<stat obj>, ...]  or <stdout c-client>  (list|string).
    """
    cmd = ['ngamsCClient']
    for opt, val in pars:
        cmd.append(opt)
        cmd.append(str(val))

    if '-servers' not in (x for x, _ in pars):
        cmd.append('-host')
        cmd.append('127.0.0.1')
    cmd.append('-status')

    env = os.environ.copy()
    env['NGAMS_VERBOSE_LEVEL'] = '0'
    _, out, _ = execCmd(cmd, shell=False)
    out = utils.b2s(out)
    if (unpackXmlStat):
        statObjList = []
        xmlStatList = out.split("Command repetition counter:")
        for xmlStat in xmlStatList:
            # Clean up the output.
            xmlStat = xmlStat.strip()
            if (xmlStat == ""): continue
            idx = 0
            while (xmlStat[idx] != "<"): idx += 1
            xmlStat = xmlStat[idx:]
            # Unpack it.
            statObj = ngamsStatus.ngamsStatus().unpackXmlDoc(xmlStat)
            statObjList.append(statObj)
        out = statObjList
    return out


@unittest.skipUnless(has_program('ngamsCClient'), "C client unavailable")
class ngamsCClientTest(ngamsTestSuite):
    """
    Synopsis:
    Tests of NG/AMS CClient + NG/AMS C-API.

    Description:
    The purpose of this Test Suite is to exercise the NG/AMS C-Client
    and thereby also the NG/AMS C-API.

    The NG/AMS Archive Client is tested in a separate suite
    (ngamsArchiveClientTest).

    Missing Test Cases:
    Test Suite should be reviewed and missing Test Cases added. Many Test Cases
    exercising different combination of command line options for the
    NG/AMS C-Client are missing.
    """

    def assert_client_ref_file(self, ref_file, data, msg):
        self.assert_ref_file(ref_file, data, startswith_filters=['Host'], msg=msg)

    def test_StatusCmd_1(self):
        """
        Synopsis:
        Issue STATUS Command/basic (no parameters).

        Description:
        Issue a STATUS Command via the C-Client (on the shell).

        Expected Result:
        The STATUS Command should be accepted and executed by the C-Client
        and the server.

        Test Steps:
        - Start standard server.
        - Execute the ngamsCClient tool on the shell.
        - Capture the output from the ngamsCClient and compare this with the
          expected output.

        Remarks:
        ...

        """
        cfgObj, dbObj = self.prepExtSrv(port=8000)
        statObj = _execCClient(pars=[["-port", "8000"],
                                     ["-cmd", "STATUS"]])[0]
        refStatFile = "ref/ngamsCClientTest_test_StatusCmd_1_1_ref"
        msg = "Incorrect info in STATUS Command XML Status Document"
        self.assert_status_ref_file(refStatFile, statObj, msg=msg)


    def test_StatusCmd_2(self):
        """
        Synopsis:
        Issue STATUS Command/proxy mode (host_id).

        Description:
        The purpose of this Test Case is to check that the ngamsCClient
        accepts the -hostId command line parameter and transfers this
        properly to the server. This is used for the proxy mode.

        Expected Result:
        The STATUS Command should be issued to the server, which will act
        as proxy and forward the request to the specified target node.

        Test Steps:
        - Start two servers.
        - Issue STATUS Command to one of them specifying the other as target.
        - Check that the command is successfully handled on the given target
          node.

        Remarks:
        The check to see if the command is actually executed on the specified
        target node is not yet fully implemented.
        """
        self.prepCluster((8000, 8011))
        hostId = "%s:%d" % (getHostName(), 8011)
        statObj = _execCClient(pars=[["-port", "8000"],
                                     ["-cmd", "STATUS"],
                                     ["-hostId", hostId]])[0]
        self.checkTags(statObj.dumpBuf(),
                       [hostId, "Successfully handled command STATUS"])


    def test_StatusCmd_3(self):
        """
        Synopsis:
        Issue STATUS Command/file_id.

        Description:
        Test that the C-Client/API can handle STATUS Command with -fileId.

        Expected Result:
        The command should be properly sent to the server, which should
        query the info about the file and send this back where it is
        handled appropriately.

        Test Steps:
        - Start two servers.
        - Archive file into one server.
        - Submit STATUS + File ID to the other server.
        - Verify that the requested info is returned.

        Remarks:
        ...

        """
        self.prepCluster((8000, 8011))
        for n in range(2):
            self.archive(8011, "src/SmallFile.fits")
        fileId = "TEST.2001-05-08T15:25:00.123"
        statObj = _execCClient(pars=[["-port", "8000"],
                                     ["-cmd", "STATUS"],
                                     ["-fileId", fileId],
                                     ["-fileVersion", "2"]])[0]
        self.checkTags(statObj.dumpBuf(),
                       ["NumberOfFiles:                      2",
                        "saf/2001-05-08/2/TEST.2001-05-08T15:25:00.123.fits.gz",
                        "TEST.2001-05-08T15:25:00.123",
                        "application/x-gfits",
                        "UncompressedFileSize:               69120",
                        "Compression:                        gzip",
                        "Ignore:                             0"])


    def test_StatusCmd_4(self):
        """
        Synopsis:
        Issue STATUS Command/file_id,file_version.

        Description:
        Same as test_StatusCmd_3 but also File Version is specified.

        Expected Result:
        The request info for the specified file should be returned.

        Test Steps:
        See test_StatusCmd_3.

        Remarks:
        ...

        """
        self.prepCluster((8000, 8011))
        for n in range(2):
            self.archive(8011, "src/SmallFile.fits")
        fileId = "TEST.2001-05-08T15:25:00.123"
        statObj = _execCClient(pars=[["-port", "8000"],
                                     ["-cmd", "STATUS"],
                                     ["-fileId", fileId],
                                     ["-fileVersion", "2"]])[0]
        self.checkTags(statObj.dumpBuf(),
                       ["saf/2001-05-08/2/TEST.2001-05-08T15:25:00.123.fits.gz",
                        "TEST.2001-05-08T15:25:00.123"])


    def test_StatusCmd_5(self):
        """
        Synopsis:
        Issue STATUS Command/disk_id.

        Description:
        Test that the C-Client/API can handle STATUS Command with -diskId.

        Expected Result:
        The STATUS Command requesting for info about a specified disk,
        should be properly transferred via the C-Client/API and executed
        on the server.

        Test Steps:
        - Start server.
        - Issue STATUS Command requesting for info about a certain disk.
        - Compare the returned XML disk info with a reference file.

        Remarks:
        ...
        """
        cfgObj, dbObj = self.prepExtSrv(port=8111)
        diskId = self.ngas_disk_id("FitsStorage1/Main/1")
        statObj = _execCClient(pars=[["-cmd", "STATUS"],
                                     ["-diskId", diskId],
                                     ["-port", "8111"]])[0]
        refStatFile = "ref/ngamsCClientTest_test_StatusCmd_5_1_ref"
        msg = "Incorrect info in STATUS Command XML Status Document/disk_id"
        self.assert_status_ref_file(refStatFile, statObj, msg=msg,
                                          startswith_filters=STD_DISK_STAT_FILT)


    def test_ArchiveCmd_Err_1(self):
        """
        Synopsis:
        Issue ARCHIVE Command/request times out.

        Description:
        The purpose of the test is to check that a request that times out
        is handled properly by the C-Client/API and a proper error message
        is produced.

        Expected Result:
        After the given timeout, the C-Client/API should generate a timeout
        error message.

        Test Steps:
        - Start speciel instance of server where Archive Requests blocks.
        - Issue Archive Request (small file) specifying a timeout of 10s.
        - Capture the output from the ngamsCClient, filter this, and
          check that the proper error message has been generated.

        Remarks:
        ...
        """
        save_to_tmp("reqCallBack_BlockCmds1", fname="reqCallBack_tmp")
        self.prepExtSrv(srvModule="test.support.ngamsSrvTestDynReqCallBack")
        out = _execCClient(unpackXmlStat = 0,
                           pars = [["-port", "8888"],
                                   ["-cmd", "ARCHIVE"],
                                   ["-fileUri", "src/SmallFile.fits"],
                                   ["-timeOut", "5"]])
        refStatFile = "ref/ngamsCClientTest_test_ArchiveCmd_Err_1_ref"
        msg = "Incorrect handling of timeout of Archive Request in C-Client/API"
        self.assert_client_ref_file(refStatFile, out, msg=msg)



    def test_ArchiveCmd_Err_2(self):
        """
        Synopsis:
        Issue ARCHIVE Command/server crashes (broken socket connection).

        Description:
        The purpose of the test is to verify the correct handling/behavior
        of the C-Client/API in the case the socket connection to the server
        breaks.

        Expected Result:
        During the request handling the socket connection is provoked to break
        and the C-Client/API should detect this and shoudl produce the proper
        error message.

        Test Steps:
        - Start special instance of the server which makes itself crash when
          a request is received (to simulate a broken socket connection).
        - Submit an ARCHIVE Command via the C-Client/API (small file).
        - Check the result produced by the ngamsCClient on stdout that this
          is as expected.

        Remarks:
        ...

        """
        save_to_tmp("reqCallBack_SrvCrash1", fname="reqCallBack_tmp")
        self.prepExtSrv(srvModule="test.support.ngamsSrvTestDynReqCallBack")
        out = _execCClient(unpackXmlStat = 0,
                           pars = [["-port", "8888"],
                                   ["-cmd", "ARCHIVE"],
                                   ["-fileUri", "src/SmallFile.fits"]])
        refStatFile = "ref/ngamsCClientTest_test_ArchiveCmd_Err_2_ref"
        msg = "Incorrect handling of crash of server in C-Client/API"
        self.assert_client_ref_file(refStatFile, out, msg=msg)


    def test_ArchiveCmd_Err_3_1(self):
        """
        Synopsis:
        Handling of corrupted HTTP response.

        Description:
        The purpose of the test is test that a corrupted HTTP response is
        properly handled by the C-Client/API.

        The response is corrupted such that it only contains a '\015\012'
        ({Slash r}{Slash n}).

        Expected Result:
        The corrupt HTTP response cannot be unpacked/interpreted as an
        XML document. This should be detected by the C-API and a proper
        error message returned.

        Test Steps:
        - Start special instance of the server class, which produces
          an illegal response.
        - Issue an ARCHIVE Command via the ngamsCClient.
        - Check that the output produced on stdout refers to the appropriate
          error message.

        Remarks:
        ...
        """

        # TODO: From V4.0, this test case produces the error:
        #
        # Error Code:     -4
        # Message:        Problem communicating with server
        #
        # - and not:
        #
        # Error Code:     -103
        # Message:        Invalid reply from data server
        #
        # - as for previous version.
        #
        # This should be investigated and resolved.

        save_to_tmp("reqCallBack_IllegalResp", fname="reqCallBack_tmp")
        save_to_tmp("\015\012", fname="ngamsServerTestIllegalResp_tmp")
        self.prepExtSrv(srvModule="test.support.ngamsSrvTestDynReqCallBack")
        out = _execCClient(unpackXmlStat = 0,
                           pars = [["-port", "8888"],
                                   ["-cmd", "ARCHIVE"],
                                   ["-fileUri", "src/SmallFile.fits"]])
        refStatFile = "ref/ngamsCClientTest_test_ArchiveCmd_Err_3_1_ref"
        msg = "Incorrect handling of corrupt server HTTP response in C-Client/API"
        self.assert_client_ref_file(refStatFile, out, msg=msg)


    def test_ArchiveCmd_Err_3_2(self):
        """
        Synopsis:
        Issue ARCHIVE Command/server sends back an empty HTTP response (='').

        Description:
        The purpose of the test is test that a corrupted HTTP response is
        properly handled by the C-Client/API.

        The response is corrupted such that it does not have any contents.

        Expected Result:
        The C-API should detect the illegally formatted response and return
        the correct error code.

        Test Steps:
        - Start special instance of the server that generates an empty
          HTTP response.
        - Issue an Archive Request.
        - Check that the proper error message is produced by the ngamsCClient

        Remarks:
        ...
        """
        save_to_tmp("reqCallBack_IllegalResp", fname="reqCallBack_tmp")
        save_to_tmp("", fname="ngamsServerTestIllegalResp_tmp")
        self.prepExtSrv(srvModule="test.support.ngamsSrvTestDynReqCallBack")
        out = _execCClient(unpackXmlStat = 0,
                           pars = [["-port", "8888"],
                                   ["-cmd", "ARCHIVE"],
                                   ["-fileUri", "src/SmallFile.fits"]])
        refStatFile = "ref/ngamsCClientTest_test_ArchiveCmd_Err_3_2_ref"
        msg = "Incorrect handling of corrupt server HTTP response in C-Client/API"
        self.assert_client_ref_file(refStatFile, out, msg=msg)


    def test_ArchiveCmd_Err_3_3(self):
        """
        Synopsis:
        Issue ARCHIVE Command/server sends back a nonsense HTTP response.

        Description:
        The purpose of the test is test that a corrupted HTTP response is
        properly handled by the C-Client/API.

        The response is corrupted such that it contains 'noise'.

        Expected Result:
        The C-API should detect the illegally formatted response and return
        the correct error code.

        Test Steps:
        - Start special instance of the server that generates a non-sense
          HTTP response.
        - Issue an Archive Request via the ngamsCClient.
        - Check that the proper error message is produced by the ngamsCClient

        Remarks:
        ...
        """
        save_to_tmp("reqCallBack_IllegalResp", fname="reqCallBack_tmp")
        save_to_tmp("f-423hcqfe-0", fname="ngamsServerTestIllegalResp_tmp")
        self.prepExtSrv(srvModule="test.support.ngamsSrvTestDynReqCallBack")
        out = _execCClient(unpackXmlStat = 0,
                           pars = [["-port", "8888"],
                                   ["-cmd", "ARCHIVE"],
                                   ["-fileUri", "src/SmallFile.fits"]])
        refStatFile = "ref/ngamsCClientTest_test_ArchiveCmd_Err_3_3_ref"
        msg = "Incorrect handling of corrupt server HTTP response in C-Client/API"
        self.assert_client_ref_file(refStatFile, out, msg=msg)


    def test_ArchiveCmd_Err_4_1(self):
        """
        Synopsis:
        Correct HTTP response, but illegal NG/AMS XML status document.

        Description:
        The purpose of the test is to verify that the C-Client/API handles
        correctly the situation where an incorrectly formatted XML status
        response is contained in the response from the server and the
        proper error code generated by the C-API.

        Expected Result:
        The C-API detects the wrongly formatted NG/AMS XML status document,
        and produces the appropriate error code.

        Test Steps:
        - Start special instance of the server which generates an HTTP
          response with a corrupt XML NG/AMS status document.
        - Issue an Archive Request via the ngamsCClient.
        - Compare the output from ngamsCClient to check that the invalid
          response was correctly handled.

        Remarks:
        ...
        """

        # TODO: From V4.0, this test case produces the error:
        #
        #  Error Code:     -103
        #  Message:        Invalid reply from data server
        #
        # - and not:
        #
        # Error Code:     -4
        # Message:        Problem communicating with server
        # Status:         FAILURE
        #
        # - as for previous version.
        #
        # This should be investigated and resolved.

        save_to_tmp("reqCallBack_IllegalResp", fname="reqCallBack_tmp")
        self.prepExtSrv(srvModule="test.support.ngamsSrvTestDynReqCallBack")
        httpResp = "HTTP/1.0 200 OK\015\012" +\
                   "Server: NGAMS/v2.3/2004-07-12T11:39:39\015\012" +\
                   "Date: Thu, 7 Oct 2004 16:20:28 GMT\015\012" +\
                   "Expires: Thu, 7 Oct 2004 16:20:28 GMT\015\012" +\
                   "Content-Type: text/xml\015\012" +\
                   "Content-Length: 36\015\012" +\
                   "\015\012" +\
                   "COMPLETELY CORRUPT NG/AMS XML STATUS"
        save_to_tmp(httpResp, fname="ngamsServerTestIllegalResp_tmp")
        out = _execCClient(unpackXmlStat = 0,
                           pars = [["-port", "8888"],
                                   ["-cmd", "ARCHIVE"],
                                   ["-fileUri", "src/SmallFile.fits"]])
        refStatFile = "ref/ngamsCClientTest_test_ArchiveCmd_Err_4_1_ref"
        msg = "Incorrect handling of corrupt server HTTP response in C-Client/API"
        self.assert_client_ref_file(refStatFile, out, msg=msg)


    @unittest.skipUnless(has_program('uncompress'), 'external uncompress program unavailable')
    def test_ArchiveCmd_Err_5_1(self):
        """
        Synopsis:
        Issue ARCHIVE Command/socket breaks while writing data on it.

        Description:
        The purpose of the test is to verify that the C-API handles
        properly the situation where the socket connection breaks while
        data is being written on it (during an Archive Push Request).

        Expected Result:
        The C-API should detect that the write socket connection breaks, and
        should produce the appropriate error message.

        Test Steps:
        - Start special instance of server which terminates itself while
          the client writes the data to the server.
        - Issue Archive Push Request with a big file to the server.
        - Verify that the proper error response is produced by the C-API.

        Remarks:
        ...
        """
        save_to_tmp("reqCallBack_SrvCrash1", fname="reqCallBack_tmp")
        self.prepExtSrv(srvModule="test.support.ngamsSrvTestDynReqCallBack")
        tmp_fname = genTmpFilename(prefix='WFI-TEST', suffix='.fits.Z')
        self.cp("src/WFI-TEST.fits.Z", tmp_fname)
        subprocess.check_call(['uncompress', tmp_fname])
        tmp_fname = tmp_fname[:-2]
        out = _execCClient(unpackXmlStat = 0,
                           pars = [["-port", "8888"],
                                   ["-cmd", "ARCHIVE"],
                                   ["-fileUri", tmp_fname]])
        refStatFile = "ref/ngamsCClientTest_test_ArchiveCmd_Err_5_1_ref"
        msg = "Incorrect handling of broken write socket in C-Client/API"
        self.assert_client_ref_file(refStatFile, out, msg=msg)


    def test_RetrieveCmd_Err_1_1(self):
        """
        Synopsis:
        Issue RETRIEVE Command/request times out.

        Description:
        The purpose of the test is to verify that the situation where a
        Retrieve Request times out is correctly handled by the C-API.

        Expected Result:
        After the specified timeout is reached, the appropriate error code
        is returned to the client.

        Test Steps:
        - Start special instance of the server, which blocks on a Retrieve
          Request (no response send).
        - Issue a Retrieve Request via the ngamsCClient with timeout 10s.
        - Verify that after 10s the correct error code is returned by the
          C-API (printed by ngamsCClient on stdout).

        Remarks:
        ...
        """
        save_to_tmp("reqCallBack_AccArchiveBlock2", fname="reqCallBack_tmp")
        self.prepExtSrv(srvModule="test.support.ngamsSrvTestDynReqCallBack")
        self.archive("src/SmallFile.fits")
        out =\
            _execCClient(unpackXmlStat = 0,
                         pars = [["-port", "8888"],
                                 ["-cmd", "RETRIEVE"],
                                 ["-fileId", "TEST.2001-05-08T15:25:00.123"],
                                 ["-timeOut", "10"]])
        refStatFile = "ref/ngamsCClientTest_test_RetrieveCmd_Err_1_1_ref"
        msg = "Incorrect handling of timeout of Retrieve Request in C-Client/API"
        self.assert_client_ref_file(refStatFile, out, msg=msg)


    def test_RetrieveCmd_Err_1_2(self):
        """
        Synopsis:
        Issue RETRIEVE Command/server dies during initial handling.

        Description:
        Check that the situation where the server dies during the initial
        handling of a Retrieve Request is correctly handled by the C-API.

        Expected Result:
        The C-API should detect that the server died (=broken socket
        connection) and should produce the appropriate error code, which is
        printed on stdout by the ngamsCClient.

        Test Steps:
        - Start a special instance of the server, which kills itself
          when receiving a RETRIEVE Command.
        - Archive a file.
        - Issue a RETRIEVE Command to retrieve the archived file.
        - Verify that the proper output is produced by ngamsCClient indicating
          the problem.

        Remarks:
        ...
        """
        save_to_tmp("reqCallBack_SrvCrash2", fname="reqCallBack_tmp")
        self.prepExtSrv(srvModule="test.support.ngamsSrvTestDynReqCallBack")
        out =\
            _execCClient(unpackXmlStat = 0,
                         pars = [["-port", "8888"],
                                 ["-cmd", "RETRIEVE"],
                                 ["-fileId", "TEST.2001-05-08T15:25:00.123"],
                                 ["-timeOut", "10"]])
        refStatFile = "ref/ngamsCClientTest_test_RetrieveCmd_Err_1_2_ref"
        msg = "Incorrect handling of timeout of Retrieve Request in C-Client/API"
        self.assert_client_ref_file(refStatFile, out, msg=msg)


    def test_RetrieveCmd_Err_1_3(self):
        """
        Synopsis:
        Issue RETRIEVE Command/server dies (connection broken)
        while the server is sending the data across.

        Description:
        The purpose of the test is to verify that the C-Client/API handle
        properly the situation where the socket connection where the server
        is writing data to the client breaks.

        Expected Result:
        The C-API should detect the problem, and that the file has been
        received incomplete and should return the proper error code, which
        is written by ngamsCClient on stdout.

        Test Steps:
        - Start special instance of the server class, which terminates itself
          while returned requested data.
        - Archive a file.
        - Retrieve the file.
        - Check that the proper error response is created on stdout by
          ngamsCClient.

        Remarks:
        ...
        """
        self.prepExtSrv(srvModule="test.support.ngamsSrvTestBrokSockRetrieve")
        self.archive("src/SmallFile.fits")
        out = _execCClient(unpackXmlStat = 0,
                           pars = [["-port", "8888"],
                                   ["-cmd", "RETRIEVE"],
                                   ["-fileId","TEST.2001-05-08T15:25:00.123"],
                                   ["-timeOut", "5"]])
        refStatFile = "ref/ngamsCClientTest_test_RetrieveCmd_Err_1_3"
        msg = "Incorrect handling of broken read socket in C-Client/API"
        self.assert_client_ref_file(refStatFile, out, msg=msg)


    def test_ServerMultiplexing_01(self):
        """
        Synopsis:
        Test server multiplexing feature of the C-API/Client.

        Description:
        The purpose of the test is to verify the proper functioning of the
        server context switching (multiplexing) provided by the C-API/Client.
        With this feature a list of servers + ports is given, and the C-API
        switches between the specified servers.

        A STATUS Command will be sent out to test the server multiplexing.

        Expected Result:
        After a limited number of attempts, all servers available, should
        have been contacted by the C-Client.

        Test Steps:
        - Start simulated cluster with 5 units.
        - Submit STATUS Command invoking the C-Client with the list of servers.
        - Verify that within 100 attempts, all servers have been contacted.

        Remarks:
        ...

        Test Data:
        ...
        """
        ports = range(8000, 8005)
        self.prepCluster(ports)
        noOfNodes = len(ports)
        nodeCount = 0
        srvList = ",".join(["127.0.0.1:%d" % (p,) for p in ports])
        noOfAttempts = 100
        counts = {p: 0 for p in range(8000, 8005)}
        for _ in range(noOfAttempts):
            statObjList = _execCClient(unpackXmlStat=1,
                                       pars=[["-servers", srvList],
                                             ["-cmd", "STATUS"],
                                             ["-timeOut", "5"],
                                             ["-repeat", "10"]])
            for statObj in statObjList:
                port = int(statObj.getHostId().split(':')[1])
                if (counts[port] == 0):
                    counts[port] = 1
                    nodeCount += 1
                    if (nodeCount == noOfNodes): break
                else:
                    time.sleep(0.100)
            if (nodeCount == noOfNodes): break
        if (nodeCount != noOfNodes):
            self.fail("Not all specified NGAS Nodes were contacted " +\
                      "within %d attempts" % noOfAttempts)