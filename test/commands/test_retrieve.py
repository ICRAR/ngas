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
# "@(#) $Id: ngamsRetrieveCmdTest.py,v 1.7 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  23/04/2002  Created
#
"""
This module contains the Test Suite for the RETRIEVE Command.
"""

import contextlib
import io
import os

from ngamsLib import ngamsHttpUtils
from ngamsLib.ngamsCore import getHostName
from ..ngamsTestLib import ngamsTestSuite, genTmpFilename, unzip, tmp_path


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
        self.prepExtSrv()
        self.archive("src/SmallFile.fits")

        # Retrieve the file.
        trgFile = tmp_path("test_RetrieveCmd_1_1_tmp")
        outFilePath = tmp_path("SmallFile.fits")
        self.retrieve("TEST.2001-05-08T15:25:00.123", targetFile=trgFile)
        unzip(trgFile, outFilePath)
        self.checkFilesEq("src/SmallFile.fits", outFilePath, "Retrieved file incorrect")


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
        self.prepExtSrv()
        self.archive("src/SmallFile.fits")

        # Retrieve the file.
        trgFile = tmp_path("test_RetrieveCmd_1_1_tmp")
        status = self.retrieve_fail("TEST.2001-05-08T15:25:00.123",
                                 fileVersion=2, targetFile=trgFile)

        # Check reply.
        refStatFile = "ref/ngamsRetrieveCmdTest_test_RetrieveCmd_2_1_ref"
        msg = "Incorrect status for RETRIEVE Command/Normal Execution"
        self.assert_status_ref_file(refStatFile, status, msg=msg, status_dump_args=(0, 1, 1))


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
        self.prepCluster((8000, 8011))
        # Archive file into sub-node (port=8011).
        self.archive(8011, "src/TinyTestFile.fits")

        # Retrieve a file.
        trgFile = tmp_path("test_RetrieveCmd_3_1_tmp")
        self.retrieve(8000, "NCU.2003-11-11T11:11:11.111", targetFile=trgFile)
        outFilePath = tmp_path('test_RetrieveCmd_3_1_tmp_unzip')
        unzip(trgFile, outFilePath)
        refFile = "src/TinyTestFile.fits"
        self.checkFilesEq(refFile, outFilePath, "Retrieved file incorrect")


    def test_RetrieveCmd_7(self):
        """
        Synopsis:
        Rerieve file from cluster, Online/Suspended and Offline files
        contained in archive.

        Description:
        The purpose of the test is to verify that the problem that there are
        Online and Offline files in the archive and thus the cross-checking
        of the number of files for which info was queried from the NGAS DB
        detected erroneously a discrepancy.

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
        envDic = self.prepCluster((8000, 8001, (8002, suspPars)))
        for portNo in [8000, 8001, 8002]:
            for _ in range(3):
                self.archive(portNo, "src/SmallFile.fits")
        self.offline(8001)
        self.exit(8001)
        subNode2 = nodes[2]
        self.waitTillSuspended(envDic[subNode2][1], subNode2, 45, nodes)

        # Retrieve file (File ID).
        fileId = "TEST.2001-05-08T15:25:00.123"
        self.retrieve(8000, fileId)
        self.retrieve(8000, fileId, fileVersion=2)


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
        nmuCfgPars = [["NgamsCfg.Server[1].ProxyMode", "0"],
                      ["NgamsCfg.Log[1].LocalLogLevel", "4"]]
        self.prepCluster(((8000, nmuCfgPars), 8011))
        self.archive(8000, "src/SmallFile.fits")
        self.archive(8011, "src/SmallFile.fits")

        # The ngamsPClient handles redirects automatically,
        # but we want to manually check here that things are right
        fileId = "TEST.2001-05-08T15:25:00.123"
        pars = (("file_id", fileId),)
        resp = ngamsHttpUtils.httpGet('127.0.0.1', 8000, 'RETRIEVE', pars=pars)
        self.assertEqual(303, resp.status)
        resp.close()

        # Follow the Location, we should get it now
        host, port = resp.getheader('Location').split('/')[2].split(':')
        port = int(port)
        resp = ngamsHttpUtils.httpGet(host, port, 'RETRIEVE', pars=pars)
        self.assertEqual(200, resp.status)
        resp.close()


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
        cfg = (("NgamsCfg.Processing[1].PlugIn[1].Name", "test.support.ngamsTestDppi1"),
               ("NgamsCfg.Processing[1].PlugIn[1].PlugInPars", "TAG=test_DppiProc_01,TARGET=FILE"))
        self.prepExtSrv(cfgProps=cfg)

        self.archive("src/SmallFile.fits")
        # Retrieve the file specifying to apply the DPPI.
        outFile = genTmpFilename("test_DppiProc_01")
        pars = [["test_suite", "ngamsRetrieveCmdTest"],
                ["test_case", "test_DppiProc_01"]]
        self.retrieve("TEST.2001-05-08T15:25:00.123", targetFile=outFile,
                      processing="test.support.ngamsTestDppi1", pars=pars)
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
        cfg = (("NgamsCfg.Processing[1].PlugIn[1].Name", "test.support.ngamsTestDppi1"),
               ("NgamsCfg.Processing[1].PlugIn[1].PlugInPars", "TAG=test_DppiProc_02,TARGET=BUFFER"))
        self.prepExtSrv(cfgProps=cfg)

        self.archive("src/SmallFile.fits")
        # Retrieve the file specifying to apply the DPPI.
        outFile = genTmpFilename("test_DppiProc_02")
        pars = [["test_suite", "ngamsRetrieveCmdTest"],
                ["test_case", "test_DppiProc_02"]]
        self.retrieve("TEST.2001-05-08T15:25:00.123", targetFile=outFile,
                      processing="test.support.ngamsTestDppi1", pars=pars)
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
                       "test.support.ngamsTestDppi1"],
                      ["NgamsCfg.Processing[1].PlugIn[1].PlugInPars",
                       "TAG=test_DppiProc_02,TARGET=FILE"]]
        self.prepCluster((8000, (8011, ncuCfgPars)))
        self.archive(8011, "src/SmallFile.fits")
        # Retrieve the file specifying to apply the DPPI.
        outFile = genTmpFilename("test_DppiProc_03")
        pars = [["test_suite", "ngamsRetrieveCmdTest"],
                ["test_case", "test_DppiProc_03"]]
        self.retrieve(8000, "TEST.2001-05-08T15:25:00.123", targetFile=outFile,
                      processing="test.support.ngamsTestDppi1", pars=pars)
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

        self.start_volumes_server()

        # Archive a file.
        stat = self.archive("src/SmallFile.fits")
        self.assert_status_ref_file("ref/ngamsRetrieveCmdTest_test_VolumeDir_01_01_ref", stat)

        # Check that the target files have been archived in their
        # appropriate locations.
        trgFile = tmp_path("test_VolumeDir_01_tmp")
        refFile = "src/SmallFile.fits"
        outFilePath = tmp_path("SmallFile.fits")
        stat = self.retrieve("TEST.2001-05-08T15:25:00.123", targetFile=trgFile)

        # unzip the the file and diff against original
        unzip(trgFile, outFilePath)
        self.checkFilesEq(refFile, outFilePath, "Retrieved file incorrect")

    def test_invalid_partial_retrievals(self):

        self.prepExtSrv()
        self.archive("src/SmallFile.fits")

        # Partial retrieval only supports a start offset, so using only
        # a suffix length or a begin/end range should fail
        ranges = ['0-1', '-1']

        # Not a number, missing -, negative number
        ranges += ['a-', 'a', '0', '-100-']

        for r in ranges:
            hdrs = {'Range': 'bytes=' + r}
            status = self.retrieve_fail("TEST.2001-05-08T15:25:00.123", targetFile='tmp', hdrs=hdrs)
            self.assertIn('Invalid Range header', status.getMessage())

    def test_partial_retrieval(self):

        self.prepExtSrv()

        with open(tmp_path("source"), 'wb') as f:
            f.write(os.urandom(1024))
        self.archive(tmp_path("source"), mimeType='application/octet-stream')

        # Retrieve the file fully first into memory
        full = io.BytesIO()
        response = ngamsHttpUtils.httpGet('127.0.0.1', 8888, 'RETRIEVE',
                                          pars=(('file_id', 'source'),))
        with contextlib.closing(response):
            file_size = full.write(response.read())

        # Now check that we can bring the same file in different number of parts
        for n_parts in (1, 2, 3, 7, 11, 13, 14, 20, 100):
            self._test_partial_retrieval(n_parts, file_size, full)

    def _test_partial_retrieval(self, n_parts, file_size, full):

        part_size, mod = divmod(file_size, n_parts)
        if mod:
            part_size += 1

        piece_by_piece = io.BytesIO()
        for n in range(n_parts):
            offset = n * part_size
            response = ngamsHttpUtils.httpGet('127.0.0.1', 8888, 'RETRIEVE',
                                              pars=(('file_id', 'source'),),
                                              hdrs={'Range': 'bytes=%d-' % (offset,)})
            with contextlib.closing(response):
                total_read = 0
                while total_read < part_size:
                    to_read = part_size - total_read
                    data = response.read(to_read)
                    if not data:
                        break
                    total_read += len(data)
                    piece_by_piece.write(data)

        self.assertEqual(file_size, piece_by_piece.tell())
        self.assertEqual(full.getvalue(), piece_by_piece.getvalue())

    def test_internal(self):
        """RETRIEVE?internal only supported with ngamsStatus.dtd"""
        self.prepExtSrv()
        received_dtd = tmp_path("received.tmp.dtd")
        self.retrieve("dummy", pars=(("internal", "ngamsStatus.dtd"),), targetFile=received_dtd)
        self.retrieve_fail("dummy", pars=(("internal", "COPYRIGHT"),))
        self.retrieve_fail("dummy", pars=(("internal", "cfg"),))
        self.retrieve_fail("dummy", pars=(("internal", "log"),))