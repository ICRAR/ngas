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
# "@(#) $Id: ngamsArchiveCmdTest.py,v 1.7 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  23/04/2002  Created
# jknudstr  18/11/2003  Overhaul
#
"""
Contains the Test Suite for the ARCHIVE Command.
"""

import contextlib
import functools
import getpass
import glob
import os
import subprocess
import unittest
from multiprocessing.pool import ThreadPool

from six.moves import cPickle # @UnresolvedImport

from ngamsLib.ngamsCore import getHostName, NGAMS_ARCHIVE_CMD, checkCreatePath, NGAMS_PICKLE_FILE_EXT, rmFile,\
    NGAMS_SUCCESS, getDiskSpaceAvail, mvFile
from ngamsLib import ngamsLib, ngamsStatus, ngamsFileInfo, ngamsHttpUtils
from ..ngamsTestLib import ngamsTestSuite, flushEmailQueue, getEmailMsg, \
    pollForFile, remFitsKey, writeFitsKey, prepCfg, getTestUserEmail, \
    genTmpFilename, execCmd, getNoCleanUp, setNoCleanUp, \
    save_to_tmp, tmp_path
from ngamsServer import ngamsFileUtils


# TODO: See how we can actually set this dynamically in the future
_checkMail = False

# FITS checksum-based unit tests are not run because the hardcoded checksum tool
# used by the ngamsFitsPlugIn is nowhere to be found (even on the internet...)
_check_fits_checksums = False

_crc32c_available = False
_test_checksums = True
try:
    import crc32c  # @UnusedImport
    _crc32c_available = True
except ImportError:
    _test_checksums = False

try:
    _space_available_for_big_file_test = getDiskSpaceAvail(os.path.dirname(tmp_path()), format="GB") >= 4.1
except:
    _space_available_for_big_file_test = False


class generated_file(object):
    """A file-like object generated on the fly"""

    def __init__(self, size):
        self._size = size
        self._read_bytes = 0
    def __len__(self):
        return self._size
    def read(self, n):
        if self._read_bytes == self._size:
            return b''
        n = min(n, self._size - self._read_bytes)
        self._read_bytes += n
        return b'\0' * n


class ngamsArchiveCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Test the ARCHIVE Command.

    Description:
    The Test Suite exercises the ARCHIVE Command. It tests the normal behavior,
    but also more advanced features such as Back-Log Buffering.

    Both Archive Push and Archive Pull is tested.

    The Archiving Proxy Mode/Archiving Multiplexing is also tested.

    Missing Test Cases:

    - Tests with 'real file systems' (simulated disks).

    - Test Back-Log Buffering for all defined cases:

      o NGAMS_ER_PROB_STAGING_AREA
      o NGAMS_ER_PROB_BACK_LOG_BUF
      o NGAMS_AL_MV_FILE

      - M=R/Synch: Check that they are declared as completed
        simultaneously + Disk Change Email Messages sent out.

      - M<R/Synch: Check that there are declared as completed
        simultaneously + Disk Change Email Messages sent out.

      - M<R/Synch: Check that there are declared as completed
        simultaneously + Disk Change Email Messages sent out.

    - TBD: Highlevel Test of Back-Log Buffering:
      - Test that ngas_files.ingestion_date + ngas_files.creation_date
        updated when file re-archived (no_versioning=1).
      - NGAS to NGAS Node archiving: http://ngas1:7777/ARCHIVE?mime_type=application/x-cfits&filename=http://ngasdev2:7777/RETRIEVE?file_id=...
      - An error message is returned indicating that the file has been
        Back-Log Buffered.
      - Server Offline.
      - Check that expected back-log buffered + pickle files found.
      - Online Janitor Suspend time from 180s->1s.
      - Check that back-log buffered file properly archived.
      - Check that back-log buffered + pickle files removed.

      Such a test would replace: test_BackLogBuf_1/2 (check).

    - Implement test in ngasPlugIns/test to test the external plug-ins.
      The NG/AMS Test Framework can be used. Tests are:

      - Archiving of Tar-balls
      - ngasGarArchFitsDapi.py
      - Other plug-ins.
    """

    def test_NormalArchivePushReq_1(self):
        """
        Synopsis:
        Test handling of normal Archive Push Request/check that Disk
        Change Notification Message is sent out.

        Description:
        This Test Case exercises the Archive Push Request. It is checked
        that a disk change is done as expected and that an Email Notification
        Message is sent out in this connection.

        The contents of the DB should be updated (ngas_files, ngas_disks)
        and the NgasDiskInfo file on the completed disk as well.

        Expected Result:
        After the first Archive Request, the first Target Disk Set should
        be marked as completed and an Email Notification sent out
        indicating this (Email Disk Change Notification).

        The information about the Main and Rep. Files should be indicated
        in the DB. The entry for that disk in ngas_disks should be updated.

        The NgasDiskInfo file on the Target Disk Set, which is now completed,
        should be updated with the newest information and marked as completed.

        Test Steps:
        - Start server setting Email Notification up to work on local host and
          sending the emails to the user running the test + set the amount of
          required, free space so high that an immediate Disk Change will take
          place.
        - Archive a file.
        - Receive the Email Notification + check the contents of this.
        - Dump the info for the Main and Rep. Disks and check these.
        - Dump the info for the Main and Rep. Files and check these.
        - Load the NgasDiskInfo files on the Target Disk Set disks and
          check the contents of these.

        Remarks:
        Consider to re-implement this Text Case, using the simulated disks in
        the form of file systems in files.
        """
        testUserEmail = getpass.getuser() + "@" + ngamsLib.getCompleteHostName()
        cfg = (
            ("NgamsCfg.Notification[1].Active", "1"),
            ("NgamsCfg.Notification[1].SmtpHost", "localhost"),
            ("NgamsCfg.Notification[1].DiskChangeNotification[1].EmailRecipient[1].Address", testUserEmail),
            ("NgamsCfg.ArchiveHandling[1].FreeSpaceDiskChangeMb", "10000000")
        )
        _, dbObj = self.prepExtSrv(cfgProps=cfg)
        flushEmailQueue()
        self.archive("src/SmallFile.fits")

        # Check that Disk Change Notification message have been generated.
        if _checkMail:
            mailContClean = getEmailMsg()
            tmpStatFile = "ngamsArchiveCmdTest_test_NormalArchivePushReq_1_tmp"
            refStatFile = "ref/ngamsArchiveCmdTest_test_NormalArchivePushReq_1_ref"
            save_to_tmp(tmpStatFile, mailContClean)
            self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect/missing Disk "+\
                              "Change Notification email msg")

        # Check that DB information is OK (completed=1).
        mainDiskCompl = dbObj.getDiskCompleted(self.ngas_disk_id("FitsStorage1/Main/1"))
        self.assertEqual(1, mainDiskCompl,
                        "Disk completion flag not set for Main Disk")
        repDiskCompl = dbObj.getDiskCompleted(self.ngas_disk_id("FitsStorage1/Rep/2"))
        self.assertEqual(1,repDiskCompl,
                        "Disk completion flag not set for Replication Disk")

        # Check that NgasDiskInfo file is OK (completed=1)
        refStatFile = "ref/ngamsArchiveCmdTest_test_NormalArchivePushReq_1_2_ref"
        statObj = ngamsStatus.ngamsStatus().\
                  load(self.ngas_path("FitsStorage1-Main-1/NgasDiskInfo"),1)
        msg = "Incorrect contents of NgasDiskInfo File/Main Disk"
        self.assert_status_ref_file(refStatFile, statObj, msg=msg)
        refStatFile = "ref/ngamsArchiveCmdTest_test_NormalArchivePushReq_1_3_ref"
        statObj = ngamsStatus.ngamsStatus().\
                  load(self.ngas_path("FitsStorage1-Rep-2/NgasDiskInfo"), 1)
        msg = "Incorrect contents of NgasDiskInfo File/Rep.4 Disk"
        self.assert_status_ref_file(refStatFile, statObj, msg=msg)


    def test_NormalArchivePushReq_2(self):
        """
        Synopsis:
        Test handling of normal Archive Push Request/check that Disk Space
        Notification Message is sent out.

        Description:
        Before a disks get classified as completed, a Disk Space Notification
        Message can be sent out to prepare the operators that soon a Disk
        Set will be full. This test checks if this message is sent out as
        expected.

        Expected Result:
        After having issued an Archive Push Request the disk should
        reach the thresshold value for sending out the Disk Space Notification
        Email.

        Test Steps:
        - Start server with cfg. specifying test user at localhost as
          receiver for the Disk Space Notification Messages + set the threshold
          for sending out Disk Space Notification Messages so high, that this
          will happen immediately.
        - Archive a file (Archive Push).
        - Receive the Notification Message and check the contents.

        Remarks:
        Consider to re-implement this Text Case, using the simulated disks in
        the form of file systems in files.
        """
        testUserEmail = getpass.getuser() + "@" + ngamsLib.getCompleteHostName()
        cfg = (
            ("NgamsCfg.Notification[1].Active", "1"),
            ("NgamsCfg.Notification[1].SmtpHost", "localhost"),
            ("NgamsCfg.Notification[1].DiskSpaceNotification[1].EmailRecipient[1].Address", testUserEmail),
            ("NgamsCfg.ArchiveHandling[1].MinFreeSpaceWarningMb", "100000")
        )
        self.prepExtSrv(cfgProps=cfg)
        flushEmailQueue()
        self.archive("src/SmallFile.fits")

        # Check that Disk Change Notification message have been generated.
        if _checkMail:
            mailContClean = getEmailMsg()
            mailContClean = mailContClean[0:mailContClean.find("space (")]
            refStatFile = "ref/ngamsArchiveCmdTest_test_NormalArchivePushReq_2_ref"
            tmpStatFile = save_to_tmp(mailContClean)
            self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect/missing Disk "+\
                              "Space Notification email msg")


    def test_NormalArchivePushReq_4(self):
        """
        Synopsis:
        Test handling of normal Archive Push Request/no_versioning=1.

        Description:
        It is possible to indicate to the NG/AMS Server in connection with an
        Archive Request, that no new version number should be allocated
        (if this is supported by the DAPI). If this is the case a file
        archived with the same File ID as a previous file archived will
        overwrite this, at least the DB entry, possibly also the file on disk
        if NGAS is still writing to the same disk.

        Expected Result:
        After archiving the file the 2nd time, the first entry should be
        overwritten.

        Test Steps:
        - Start server.
        - Issue Archive Push Request/no_versioning=1.
        - Re-issue the same Archive Push Request/no_versioning=1.
        - Check that the File Info is the same for the 2nd Archive Request
          as for the first.

        Remarks:
        ...
        """
        self.prepExtSrv()
        self.archive("src/SmallFile.fits")
        self.archive("src/SmallFile.fits", noVersioning=1)


    def test_ArchivePushReq_1(self):
        """
        Synopsis:
        Test archiving of file URIs with equal signs in them.

        Description:
        The purpose of this Test Case is to check if the system can handle
        archiving of files with equal signs in the name.

        Expected Result:
        When issuing a file with an equal sign in the URI, the archiving should
        take place as normal.

        Test Steps:
        - Start server.
        - Create test FITS file with equal signs in the name.
        - Archive this.
        - Check that the status (File Info) is as expected.

        Remarks:
        ...
        """
        self.prepExtSrv()
        tmpFile = genTmpFilename(prefix="Tmp=Fits=File", suffix='.fits')
        self.cp("src/SmallFile.fits", tmpFile)
        self.archive(tmpFile)


    def test_BackLogBuf_01(self):
        """
        Synopsis:
        Test handling of Back-Log Buffering (basic test).

        Description:
        The purpose of this test is to check the proper functioning of the
        Back-Log Buffer Feature. In this case, the error ocurring is a
        DB com. problem (NGAMS_ER_DB_COM).

        Expected Result:
        Staging File and Req. Props. File are stored in the Back-Log Buffer.

        Test Steps:
        - Start server with test configuration, which specifies a DAPI that
          raises an exception forcing the server to Back-Log Buffer a file.
          Janitor Thread Suspension Time = very long.
        - Archive file.
        - Check response that the file was Back-Log Buffered by the server.
        - Check that the Staging File and the Req. Props. File are stored
          in the Back-Log Buffer and that the contents is as expected.
        - Stop the server.
        - Start normal server + check that Back-Log Buffered file is archived.

        Remarks:
        ...
        """
        cfgPars = [["NgamsCfg.Streams[1].Stream[2].PlugIn",
                    "test.support.ngamsRaiseEx_NGAMS_ER_DAPI_1"],
                   ["NgamsCfg.JanitorThread[1].SuspensionTime", "0T00:05:00"],
                   ['NgamsCfg.Server[1].RequestDbBackend', 'memory']]
        self.prepExtSrv(cfgProps=cfgPars)
        statObj = self.archive_fail("src/SmallFile.fits")
        refFile = "ref/test_BackLogBuf_01_01_ref"
        msg = "Unexpected reply from server"
        self.assert_status_ref_file(refFile, statObj, msg=msg)

        reqPropsFile = glob.glob(self.ngas_path("back-log/*.pickle"))[0]
        with open(reqPropsFile, 'rb') as fo:
            tmpReqPropObj = cPickle.load(fo)
        refFile = "ref/test_BackLogBuf_01_02_ref"
        msg = "Unexpected contents of Back-Log Buffered Req. Prop. File"
        self.assert_status_ref_file(refFile, tmpReqPropObj, msg=msg, filters=['RequestId'])
        self.checkFilesEq("src/SmallFile.fits",
                          tmpReqPropObj.getStagingFilename(),
                          "Illegal Back-Log Buffered File: %s" %\
                          tmpReqPropObj.getStagingFilename())

        # Cleanly shut down the server, and wait until it's completely down
        old_cleanup = getNoCleanUp()
        setNoCleanUp(True)
        self.termExtSrv(self.extSrvInfo.pop())
        setNoCleanUp(old_cleanup)

        cfgPars = [["NgamsCfg.Permissions[1].AllowArchiveReq", "1"],
                   ["NgamsCfg.ArchiveHandling[1].BackLogBuffering", "1"],
                   ["NgamsCfg.JanitorThread[1].SuspensionTime", "0T00:00:05"]]
        _, dbObj = self.prepExtSrv(delDirs=0, clearDb=0, cfgProps=cfgPars)
        pollForFile(self.ngas_path("back-log/*"), 0, timeOut=30)
        filePat = self.ngas_path("%s/saf/2001-05-08/1/" +\
                  "TEST.2001-05-08T15:25:00.123.fits.gz")
        pollForFile(filePat % "FitsStorage1-Main-1", 1)
        pollForFile(filePat % "FitsStorage1-Rep-2", 1)
        fileId = "TEST.2001-05-08T15:25:00.123"

        disk_paths = ("FitsStorage1/Main/1", "FitsStorage1/Rep/2")
        for disk_path, file_type, ref_no in zip(disk_paths, ('Main', 'Replication'), (3, 4)):
            disk_id = self.ngas_disk_id(disk_path)
            file_info = ngamsFileInfo.ngamsFileInfo().read(getHostName(), dbObj, fileId, 1, disk_id)
            ref_file = "ref/test_BackLogBuf_01_%02d_ref" % ref_no
            msg = "Incorrect info in DB for %s File archived" % file_type
            self.assert_status_ref_file(ref_file, file_info, msg=msg)


    def test_NoBackLogBuf_01(self):
        """
        Synopsis:
        Test correct handling when Back-Log Buffering is disabled.

        Description:
        The purpose of this test is to check the proper functioning when
        Back-Log Buffering is disabled and an error qualifying for Back-Log
        Buffering occurs, in this case DB com. problem (NGAMS_ER_DB_COM).

        Expected Result:
        The DAPI encounters a (simulated) problem with the DB communication
        and raises an exception. NG/AMS should recognize this, and should
        not Back-Log Buffer the file. The Staging Files should be deleted.
        Staging Area, Back-Log Buffer and Bad Files Area should not contain
        any files.

        Test Steps:
        - Start server with test configuration, which specifies a DAPI that
          raises an exception which normally would qualify for Back-Log
          Buffering. Janitor Thread Suspension Time = very long + Back-Log
          Buffering disabled.
        - Archive file.
        - Check response that the file could not be archived due to the
          problem.
        - Check that the Staging Areas, Back-Log Buffer and Bad Files Area
          contains no files.

        Remarks:
        ...
        """
        cfgPars = [["NgamsCfg.Streams[1].Stream[2].PlugIn",
                    "test.support.ngamsRaiseEx_NGAMS_ER_DAPI_1"],
                   ["NgamsCfg.JanitorThread[1].SuspensionTime", "0T00:05:00"],
                   ["NgamsCfg.ArchiveHandling[1].BackLogBuffering", "0"]]
        self.prepExtSrv(cfgProps=cfgPars)
        statObj = self.archive_fail("src/SmallFile.fits")
        refFile = "ref/test_NoBackLogBuf_01_01_ref"
        self.assert_status_ref_file(refFile, statObj, msg="Unexpected reply from server")
        pollForFile(self.ngas_path("FitsStorage*-Main-*/staging/*"), 0)
        pollForFile(self.ngas_path("back-log/*"), 0)
        pollForFile(self.ngas_path("bad-files/*"), 0)


    def test_ArchivePullReq_1(self):
        """
        Synopsis:
        Test Archive Pull Request/file:<Path>.

        Description:
        Files can be archived either via the Archive Push and Archive
        Pull Technique. When using latter, a URL is specified where the
        server will pick up the file.

        Expected Result:
        When issuing the file and specifying the URL (file:filename) the
        Archive Pull Request should be handled as expected.

        Test Steps:
        - Start server.
        - Archive file specifying the file URL pointing to it.
        - Check the contents of the status returned if the file was properly
          archived.

        Remarks:
        ...
        """
        self.prepExtSrv()
        srcFile = "src/SmallFile.fits"
        srcFileUrl = "file:" + self.resource(srcFile)
        self.archive(srcFileUrl)


    def test_ArchivePullReq_2(self):
        """
        Synopsis:
        Test Archive Pull Request/Retrieve Request
        (http://<Host>:<Port>/RETRIEVE?file_id=<ID>).

        Description:
        When using the Archive Pull Technique, it is possible to archive a
        file into an NGAS Node, by specifying its URL its URL in another
        NGAS Node where the file was archived previously.

        Expected Result:
        When the file is archive into an NGAS Node by specifying it URL to
        retrieve it from another, node, the archiving should take place as
        usual for Archive Push/Pull Requests.

        Test Steps:
        - Create a simluated cluster with 2 nodes.
        - Archive a file into one of the nodes.
        - Re-archive on the other node via an Archive Pull Request (via HTTP)
          into the other node.
        - Check the contents of the NG/AMS Status Document returned.

        Remarks:
        ...
        """
        self.prepCluster((8000, 8011))
        self.archive(8011, "src/SmallFile.fits")
        fileUri = "http://127.0.0.1:8011/RETRIEVE?file_id=" +\
                  "TEST.2001-05-08T15:25:00.123&file_version=1"
        status = self.get_status(8000, NGAMS_ARCHIVE_CMD,
                                 [["filename", fileUri],
                                  ["mime_type", "application/x-gfits"]])
        refStatFile = "ref/ngamsArchiveCmdTest_test_ArchivePullReq_2_1_ref"
        msg = "Incorrect status returned for Archive Push Request/HTTP"
        self.assert_status_ref_file(refStatFile, status, msg=msg, port=8000,
                                    filters=["http://", "LogicalName:"])


    def test_ErrHandling_1(self):
        """
        Synopsis:
        Test handling of illegal FITS Files.

        Description:
        The purpose of this test is to check if NG/AMS and the DAPI,
        'ngamsFitsPlugIn', properly handle/detect the following improper FITS
        Files/error conditions:

          - Illegal size of FITS file (not a multiple of 2880).
          - Missing CHECKSUM keyword.
          - Illegal checksum in FITS file.
          - Unknown mime-type.
          - Illegal File URI at Archive Pull.

        Expected Result:
        In all of the cases above, NG/AMS or the DAPI should detect the problem
        and return an error message to the client. The Staging Area should
        be cleaned up.

        Test Steps:
        - Start server.
        - Create test file (size!=2880) and archive this:
          - Check the error response from the server.
          - Check that the Staging Areas are cleaned up.
          - Check that Bad Files Area is empty.
        - Create file with no CHECKSUM keyword in it:
          - Check the error response from the server.
          - Check that the Staging Areas are cleaned up.
          - Check that Bad Files Area is empty.
        - Create FITS File with illegal CHECKSUM in the header:
          - Check the error response from the server.
          - Check that the Staging Areas are cleaned up.
          - Check that Bad Files Area is empty.
        - Create a file with an unknown mime-type:
          - Check the error response from the server.
          - Check that the Staging Areas are cleaned up.
          - Check that Bad Files Area is empty.
        - Issue an Archive Pull Request with an illegal URI:
          - Check the error response from the server.
          - Check that the Staging Areas are cleaned up.
          - Check that Bad Files Area is empty.

        Remarks:
        ...

        """
        self.prepExtSrv()
        stgAreaPat = self.ngas_path("FitsStorage*-Main-*/staging/*")
        badFilesAreaPat = self.ngas_path("bad-files/*")

        # Illegal size of FITS file (not a multiple of 2880).
        illSizeFile = save_to_tmp("dsjhdjsadhaskjdhaskljdhaskjhd", suffix='.fits')
        statObj = self.archive_fail(illSizeFile)
        self.checkTags(statObj.getMessage(), ["NGAMS_ER_DAPI_BAD_FILE",
                                              "not a multiple of 2880 " +\
                                              "(size: 29)"])
        pollForFile(stgAreaPat, 0)
        pollForFile(badFilesAreaPat, 0)

        # Missing CHECKSUM keyword.
        if _check_fits_checksums:
            noChecksumFile = tmp_path("NoChecksum.fits")
            self.cp("src/SmallFile.fits", noChecksumFile)
            remFitsKey(noChecksumFile, "CHECKSUM")
            statObj = self.archive_fail(noChecksumFile)
            self.checkTags(statObj.getMessage(), ["NGAMS_ER_DAPI_BAD_FILE",
                                                  "Illegal CHECKSUM/DATASUM"])
            pollForFile(stgAreaPat, 0)
            pollForFile(badFilesAreaPat, 0)

            # Illegal checksum in FITS file.
            illChecksumFile = tmp_path("IllChecksum.fits")
            self.cp("src/SmallFile.fits", illChecksumFile)
            writeFitsKey(illChecksumFile, "CHECKSUM", "BAD-CHECKSUM!", "TEST")
            statObj = self.archive_fail(illChecksumFile)
            self.checkTags(statObj.getMessage(), ["NGAMS_ER_DAPI_BAD_FILE",
                                                  "Illegal CHECKSUM/DATASU"])
            pollForFile(stgAreaPat, 0)
            pollForFile(badFilesAreaPat, 0)

        # Unknown mime-type.
        unknownMtFile = tmp_path("UnknownMimeType.stif")
        self.cp("src/SmallFile.fits", unknownMtFile)
        statObj = self.archive_fail(unknownMtFile)
        refStatFile = "ref/ngamsArchiveCmdTest_test_ErrHandling_1_4_ref"
        msg = "Incorrect status returned for Archive Push Request/Unknown Mimetype"
        self.assert_status_ref_file(refStatFile, statObj, msg=msg)
        pollForFile(stgAreaPat, 0)
        pollForFile(badFilesAreaPat, 0)

        # Illegal File URI at Archive Pull.
        illFileUri = "http://unknown.domain.com/NonExistingFile.fits"
        status = self.get_status_fail(NGAMS_ARCHIVE_CMD, [["file_uri", illFileUri]])
        refStatFile = "ref/ngamsArchiveCmdTest_test_ErrHandling_1_5_ref"
        msg = "Incorrect status returned for Archive Pull Request/Illegal URI"
        self.assert_status_ref_file(refStatFile, status, msg=msg)
        pollForFile(stgAreaPat, 0)
        pollForFile(badFilesAreaPat, 0)


    def test_ErrHandling_2(self):
        """
        Synopsis:
        Problems creating Replication File.

        Description:
        This test exercises the handling of the situation where the Replication
        File cannot be created, in this case due to that the Replication Area
        is read-only.

        Expected Result:
        When failing in creating the Replication File, the NG/AMS Server
        should detect this, and send back an Error Response to the client.

        Test Steps:
        - Start server.
        - Make the target Replication Disk read-only.
        - Archive file.
        - An error message should be returned, check contents of this.

        Remarks:
        It could be considered if the NG/AMS Server should roll-back the
        Main File, which was archived. This however, is not so easy to
        implement. The most important is that the client is informed that
        the archiving failed, and can re-submit the file. The operator
        would need to intervene to rectify the problem.
        """
        self.prepExtSrv()
        repDiskPath = self.ngas_path("FitsStorage1-Rep-2")

        # TODO: Change these by python-based chmod
        subprocess.call(['chmod', '-R', 'a-rwx', repDiskPath], shell=False)

        # In certain systems when running as root we still have the
        # ability to write in permissions-constrained directories
        # Check if that's the case before proceeding
        try:
            with open(os.path.join(repDiskPath, 'dummy'), 'wb') as f:
                f.write(b'b')
            can_write = True
        except IOError:
            can_write = False

        # There's no point anymore...
        if can_write:
            return

        # We should fail now
        status = self.archive_fail("src/SmallFile.fits")
        subprocess.call(['chmod', '-R', 'a+rwx', repDiskPath], shell=False)
        msg = "Incorrect status returned for Archive Push Request/Replication Disk read-only"
        self.assertEqual(3025, int(status.getMessage().split(":")[1]), msg) # NGAMS_AL_CP_FILE:3025


    def test_ErrHandling_3(self):
        """
        Synopsis:
        No free Storage Sets for mime-type.

        Description:
        The purpose of this Test Case is to check the handling of the situation
        where there are no free Storage Sets for a given mime-type.

        Expected Result:
        The server should detect this and should send back an error message.
        The Staging Areas and the Bad File Areas should be empty.

        Test Steps:
        - Start server.
        - Mark all disks as completed in the DB.
        - Archive a file.
        - Check the contents of the error response from the server.
        - Check that no files are found in the Staging Areas or in the
          Bad Files Area.

        Remarks:
        ...
        """
        _, dbObj = self.prepExtSrv(port=8888)
        host_id = getHostName() + ":8888"
        sqlQuery = "UPDATE ngas_disks SET completed=1 WHERE host_id={0}"
        dbObj.query2(sqlQuery, args=(host_id,))
        statObj = self.archive_fail("src/SmallFile.fits")
        sqlQuery = "UPDATE ngas_disks SET completed=0 WHERE host_id={0}"
        dbObj.query2(sqlQuery, args=(host_id,))
        refStatFile = "ref/ngamsArchiveCmdTest_test_ErrHandling_3_1_ref"
        msg = "Incorrect status returned for Archive Push Request/No Free Storage Sets"
        self.assert_status_ref_file(refStatFile, statObj, msg=msg)
        pollForFile(self.ngas_path("FitsStorage*-Main-*/staging/*"), 0)
        pollForFile(self.ngas_path("bad-files/*"), 0)


    @unittest.skip("Test case requires missing file under src/")
    def test_MainDiskSmallerThanRep_1(self):
        """
        Synopsis:
        Check usage of several Main Disks with one Rep. Disk (different sizes
        of Main and Rep. Disks).

        Description:
        The purpose of this test is to verify that it is possible to use
        several Main Disks together with one Replication Disk. This simulates
        the operational scenario where e.g. 80 GB Main Disks are used together
        with 200 GB Rep. Disks.

        Expected Result:
        When archiving onto the Mixed Disk Set, the Main Disk should become
        completed, but the Rep. Disk not. After changing the Main Disk, it
        should be possible to continue to archive onto the Disk Set until
        either the Main Disk or the Rep. Disk gets completed.

        Test Steps:
        - Start server with following disk cfg.:

          - Main<Rep/!Synchronization.
          - Disk Configuration:

            - Disk Set 1: 1:8MB/2:16MB.
            - Disk Set 2: 3:8MB/4:16MB.
            - Disk Set 3: 5:8MB/6:16MB.

        - Archive data until disk/Slot 1 fills up - check:
          - Disk/Slot 1 is marked as completed.
          - Disk/Slot 2 is not marked as completed.
          - Email Notification Message is sent indicating to change
            only disk/Slot 1.

        - Continue to archive data until disk/Slot 3 fills up - check:
          - That disk/Slot 3 is marked as completed + disk/Slot 4
            is not marked as completed.
          - That Email Notification sent out indicating to change disk/Slot 3.

        - Bring server Offline + 'replace' disks in Slot 1/3 + continue to
          archive until disk/Slot 5 fills up - check:
          - That there is changed to Disk Set 1.
          - That disk/Slot 5 is marked as completed.
          - That disk/Slot 6 is not marked as completed.
          - That Email Notification sent out indicating to replace disk/Slot 5.

        - Continue to archive until disk/Slot 2 fills up - check:
          - That disk/Slot 1 is not marked as completed.
          - That disk/Slot 2 is marked as completed.
          - That Email Notification sent out indicating to replace disk/Slot 2.

        Remarks:
        ...
        """
        #####################################################################
        # Note: Test file is 1.07 MB, min. free space 4 MB (integer). This
        #       means that 3 files can be archived on a 8 MB volume
        #       (8 MB - (3 * 1.07) = 4.79 MB -> 4 MB.
        #                                                    End Result
        #             First Disk         New Disk            (# files)
        #       |-1: [111     ]          [4444445         ]       7
        # Set 1-|
        #       |-2: [1114444445      ]                          10
        #
        #       |-3: [222     ]          [5               ]       1
        # Set 2-|
        #       |-4: [2225            ]                           4
        #
        #       |-5: [344     ]                                   3
        # Set 2-|
        #       |-6: [344             ]                           3
        #####################################################################

        #####################################################################
        # Phase 1: Archive data until disk/Slot 1 fills up.
        #####################################################################
        flushEmailQueue()

        tmpCfgFile = prepCfg("src/ngamsCfg.xml",
                             [["ArchiveHandling[1].FreeSpaceDiskChangeMb","4"],
                              ["Notification[1].Active", "1"],
                              ["Notification[1].SmtpHost","localhost"],
                              ["Notification[1].DiskChangeNotification[1]."+\
                               "EmailRecipient[1].Address",
                               getTestUserEmail()]])
        newCfgFile = self.prepDiskCfg([{"DiskLabel":      None,
                                        "MainDiskSlotId": "1",
                                        "RepDiskSlotId":  "2",
                                        "Mutex":          "1",
                                        "StorageSetId":   "Data1",
                                        "Synchronize":    "0",
                                        "_SIZE_MAIN_":    "8MB",
                                        "_SIZE_REP_":     "16MB"},

                                       {"DiskLabel":      None,
                                        "MainDiskSlotId": "3",
                                        "RepDiskSlotId":  "4",
                                        "Mutex":          "1",
                                        "StorageSetId":   "Data2",
                                        "Synchronize":    "0",
                                        "_SIZE_MAIN_":    "8MB",
                                        "_SIZE_REP_":     "16MB"},

                                       {"DiskLabel":      None,
                                        "MainDiskSlotId": "5",
                                        "RepDiskSlotId":  "6",
                                        "Mutex":          "1",
                                        "StorageSetId":   "Data3",
                                        "Synchronize":    "0",
                                        "_SIZE_MAIN_":    "8MB",
                                        "_SIZE_REP_":     "16MB"}],
                                      cfgFile = tmpCfgFile)
        self.prepExtSrv(cfgFile = newCfgFile, delDirs=0)
        fitsFile = genTmpFilename() + ".fits.gz"
        self.cp("src/1MB-2MB-TEST.fits.gz", fitsFile)
        execCmd("gunzip %s" % fitsFile)
        for _ in range(3):
            self.archive(fitsFile[0:-3])  # #1
        # Check: Disk/Slot 1 is marked as completed.
        # Check: Disk/Slot 2 is not marked as completed.
        preFix = "ref/ngamsArchiveCmdTest_test_"
        for diFiles in [[self.ngas_path("Data1-Main-1/NgasDiskInfo"),
                         preFix + "MainDiskSmallerThanRep_1_1_ref"],
                        [self.ngas_path("Data1-Rep-2/NgasDiskInfo"),
                         preFix + "MainDiskSmallerThanRep_1_2_ref"]]:
            statObj = ngamsStatus.ngamsStatus().load(diFiles[0], 1)
            self.assert_status_ref_file(diFiles[1], statObj, msg="Incorrect contents " +\
                              "of NgasDiskInfo File/Main Disk (%s)"%diFiles[0])
        # Check: Email Notification Message is sent indicating to change
        #        only disk/Slot 1.
        if _checkMail:
            refStatFile = preFix + "MainDiskSmallerThanRep_1_3_ref"
            tmpStatFile = save_to_tmp(getEmailMsg())
            self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect/missing Disk "+\
                              "Change Notification Email Msg")
        #####################################################################

        #####################################################################
        # Phase 2: Continue to archive data until disk/Slot 3 fills up.
        #####################################################################
        for _ in range(3):
            self.archive(fitsFile[0:-3])  # #2
        self.archive(fitsFile[0:-3]) # #3
        # Check: That disk/Slot 3 is marked as completed + disk/Slot 4
        # is not marked as completed.
        for diFiles in [[self.ngas_path("Data2-Main-3/NgasDiskInfo"),
                         preFix + "MainDiskSmallerThanRep_1_4_ref"],
                        [self.ngas_path("Data2-Rep-4/NgasDiskInfo"),
                         preFix + "MainDiskSmallerThanRep_1_5_ref"]]:
            statObj = ngamsStatus.ngamsStatus().load(diFiles[0], 1)
            self.assert_status_ref_file(diFiles[1], statObj, msg="Incorrect contents " +\
                              "of NgasDiskInfo File/Main Disk (%s)"%diFiles[0])
        # Check: That Email Notification sent out indicating to change
        # disk/Slot 3.
        if _checkMail:
            refStatFile = preFix + "MainDiskSmallerThanRep_1_6_ref"
            tmpStatFile = save_to_tmp(getEmailMsg())
            self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect/missing Disk "+\
                              "Change Notification Email Msg")
        #####################################################################

        #####################################################################
        # Phase 3: Bring server Offline + 'replace' disks in Slot 1/3 +
        #          continue to archive until disk/Slot 5 fills up.
        #####################################################################
        self.offline()
        self.prepDiskCfg([{"DiskLabel":      None,
                           "MainDiskSlotId": "1",
                           "RepDiskSlotId":  None,
                           "Mutex":          "1",
                           "StorageSetId":   "Data1",
                           "Synchronize":    "0",
                           "_SIZE_MAIN_":    "16MB",
                           "_SIZE_REP_":     None},

                          {"DiskLabel":      None,
                           "MainDiskSlotId": "3",
                           "RepDiskSlotId":  None,
                           "Mutex":          "1",
                           "StorageSetId":   "Data2",
                           "Synchronize":    "0",
                           "_SIZE_MAIN_":    "16MB",
                           "_SIZE_REP_":     None}],
                         cfgFile = tmpCfgFile)
        self.online()

        for _ in range(8):
            self.archive(fitsFile[0:-3])  # #4
        # Re-init the server to ensure the NgasDiskInfo file has been updated.
        self.init()

        # Check: That disk/Slot 5 is marked as completed.
        # Check: That disk/Slot 6 is not marked as completed.
        for diFiles in [[self.ngas_disk_id("Data3-Main-5/NgasDiskInfo"),
                         preFix + "MainDiskSmallerThanRep_1_7_ref"],
                        [self.ngas_disk_id("Data3-Rep-6/NgasDiskInfo"),
                         preFix + "MainDiskSmallerThanRep_1_8_ref"]]:
            statObj = ngamsStatus.ngamsStatus().load(diFiles[0], 1)
            msg = "Incorrect contents of NgasDiskInfo File/Main Disk (%s)/1"
            self.assert_status_ref_file(diFiles[1], statObj, msg=msg % diFiles[0])

        # Check: That Email Notification sent out indicating to replace
        #        disk/Slot 5.
        if _checkMail:
            refStatFile = preFix + "MainDiskSmallerThanRep_1_9_ref"
            tmpStatFile = save_to_tmp(getEmailMsg())
            self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect/missing Disk "+\
                              "Change Notification Email Msg")
        #####################################################################

        #####################################################################
        # Phase 4: Continue to archive until disk/Slot 2 fills up.
        #####################################################################
        for _ in range(2):
            self.archive(fitsFile[0:-3])  # #5

        # Re-init the server to ensure the NgasDiskInfo file has been updated.
        self.init()

        # Check: That disk/Slot 1 is not marked as completed.
        # Check: That disk/Slot 2 is marked as completed.
        for diFiles in [[self.ngas_disk_id("Data1-Main-1/NgasDiskInfo"),
                         preFix + "MainDiskSmallerThanRep_1_10_ref"],
                        [self.ngas_disk_id("Data1-Rep-2/NgasDiskInfo"),
                         preFix + "MainDiskSmallerThanRep_1_11_ref"]]:
            statObj = ngamsStatus.ngamsStatus().load(diFiles[0], 1)
            msg = "Incorrect contents of NgasDiskInfo File/Main Disk (%s)/2"
            self.assert_status_ref_file(diFiles[1], statObj, msg=msg % diFiles[0])
        # Check: That Email Notification sent out indicating to replace
        #        disk/Slot 2.
        if _checkMail:
            refStatFile = preFix + "MainDiskSmallerThanRep_1_12_ref"
            tmpStatFile = save_to_tmp(getEmailMsg())
            self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect/missing Disk "+\
                              "Change Notification Email Msg")
        #####################################################################


    def test_ArchiveRobustness_01_01(self):
        """
        Synopsis:
        Spurios files found in Staging Area/Offline.

        Description:
        If Staging Files are found in a Staging Area something went wrong
        during handling of an Archive Rerquest. Such files should be moved
        to the Bad Files Area when the server goes Online/Offline.

        Expected Result:
        When the server goes online it should find the spurios files in a
        Staging Area and move these to the Bad Files Area.

        Test Steps:
        - Start server.
        - Create a set of spurios files in all Staging Areas.
        - Stop server.
        - Check that all files have been moved to the Bad Files Area.

        Remarks:
        ...
        """
        self.prepExtSrv()
        stgPat = self.ngas_path("%s/staging/%s.fits")
        diskList = ["FitsStorage1-Main-1", "FitsStorage2-Main-3",
                    "FitsStorage3-Main-5", "PafStorage-Main-7",
                    "LogStorage-Main-9"]
        for diskName in diskList:
            stgFile = stgPat % (diskName, diskName)
            checkCreatePath(os.path.dirname(stgFile))
            self.cp("src/SmallFile.fits", stgFile)
            fo = open("%s.%s" % (stgFile, NGAMS_PICKLE_FILE_EXT), "w")
            fo.write("TEST/DUMMY REQUEST PROPERTIES FILE: %s" % diskName)
            fo.close()

        # Cleanly shut down the server, and wait until it's completely down
        old_cleanup = getNoCleanUp()
        setNoCleanUp(True)
        self.termExtSrv(self.extSrvInfo.pop())
        setNoCleanUp(old_cleanup)

        self.prepExtSrv(delDirs=0, clearDb=0)
        badDirPat = self.ngas_path("bad-files/BAD-FILE-*-%s.fits")
        for diskName in diskList:
            badFile = badDirPat % diskName
            pollForFile(badFile, 1)
            pollForFile("%s.%s" % (badFile, NGAMS_PICKLE_FILE_EXT), 1)


    def test_ArchiveRobustness_02_01(self):
        """
        Synopsis:
        Server dies before cleaning up the Staging Files.

        Description:
        This Test Case is similar to test_ArchiveRobustness_01_01 but
        in this case it is a real test scenario.

        Expected Result:
        When the server is started up, it should find the Request Properties
        File in a Staging Area and move this to the Bad Files Area.

        Test Steps:
        - Start server that crashes before cleaning up the Staging File(s).
        - Archive file, the server should kill itself.
        - Check that the Req. Props. File is in the Staging Area.
        - Start the server.
        - Check that the Req. Props. File is moved to the Bad Files Area.

        Remarks:
        ...
        """
        self.prepExtSrv(srvModule="test.support.ngamsSrvTestKillBeforeArchCleanUp")
        try:
            self.archive("src/SmallFile.fits")
        except:
            pass
        reqPropStgFile = self.ngas_path("FitsStorage1-Main-1/staging/" +\
                         "*-SmallFile.fits.pickle")
        pollForFile(reqPropStgFile, 1)
        self.prepExtSrv(delDirs=0, clearDb=0, force=True)
        reqPropBadFile = self.ngas_path("bad-files/BAD-FILE-*-SmallFile.fits.pickle", port=8888)
        pollForFile(reqPropBadFile, 1)


    def test_NoDapi_01(self):
        """
        Synopsis:
        No DAPI installed to handled request/Archive Push/async=1.

        Description:
        If the specified DAPI cannot be loaded during the Archive Request
        handling it is not possible to handle the Archive Request and
        the server must bail out.

        Expected Result:
        The Archive Request handling should be interrupted and an error
        reply sent back to the client since async=1. The files in connection
        with the request should be removed from the Staging Area.

        Test Steps:
        - Start server with configuration specifying a non-existing DAPI
          to handle FITS files.
        - Archive FITS file (async=1).
        - Check error reply from server.
        - Check that Staging Area is cleaned up.
        - Check that no files were archived.
        - Check that no files moved to Bad Files Area.

        Remarks:
        ...
        """
        cfgPars = [["NgamsCfg.Streams[1].Stream[2].PlugIn", "NonExistingDapi"]]
        self.prepExtSrv(cfgProps=cfgPars)
        stat = self.archive_fail("src/SmallFile.fits")
        refStatFile = "ref/test_NoDapi_01_01_ref"
        msg = "Incorrect status message from NG/AMS Server"
        self.assert_status_ref_file(refStatFile, stat, msg=msg)
        pollForFile(self.ngas_path("FitsStorage1-Main-1/staging/*"), 0)
        arcFileNm = self.ngas_path("%s/saf/2001-05-08/1/*")
        pollForFile(arcFileNm % "FitsStorage1-Main-1", 0)
        pollForFile(arcFileNm % "FitsStorage1-Rep-2", 0)
        pollForFile(self.ngas_path("bad-files/*"), 0)


    #########################################################################
    # Stuff to generate cfg. files for testing Archiving Proxy Mode.
    #########################################################################
    __STR_EL        = "NgamsCfg.Streams[1].Stream[%d]"
    __ARCH_UNIT_ATR = "%s.ArchivingUnit[%d].HostId"
    __STREAM_LIST   = [[["%s.MimeType","image/x-fits"],
                        ["%s.PlugIn", "ngamsFitsPlugIn"],
                        ["%s.PlugInPars", "compression=gzip," +\
                         "checksum_util=utilFitsChecksum," +\
                         "skip_checksum=," +\
                         "checksum_result=0/0000000000000000"]]]

    def _genArchProxyCfg(self, ports):
        """
        Generate a cfg. file.
        """
        cfg = []
        for idx,streamEl in enumerate(self.__STREAM_LIST, 1):
            strEl = self.__STR_EL % idx
            for strAttrEl, attrVal in streamEl:
                nextAttr = strAttrEl % strEl
                cfg.append((nextAttr, attrVal))
            for hostIdx, port in enumerate(ports, 1):
                nauAttr = self.__ARCH_UNIT_ATR % (strEl, hostIdx)
                cfg.append((nauAttr, "%s:%d" % (getHostName(), port)))
                hostIdx += 1
            idx += 1
        return "src/ngamsCfgNoStreams.xml", cfg
    #########################################################################


    def test_ArchiveProxyMode_01(self):
        """
        Synopsis:
        Test Archiving Proxy Mode - 1 Master/no archiving, 4 NAUs.

        Description:
        The purpose of this Test Case is to test that the Archving Proxy Mode
        works as expected.

        One master node is configured to archive onto four sub-nodes. The
        master node itself does not support archiving.

        It is checked that within a limited number of archive attempt, the
        file is archived onto all nodes.

        Expected Result:
        After having configured and started the servers, a test file will
        be archived up to 100 times. It should within these 100 attempts, be
        archived onto all archiving units.

        Test Steps:
        - Start master node configured to archive onto 4 NAUs.
        - Start four NAUs.
        - Archive a small file to the master until it has been archived
          onto all NAUs. Max tries: 100.

        Remarks:
        ...

        Test Data:
        ...
        """
        ports = range(8001, 8005)
        cfg_file, cfg_props = self._genArchProxyCfg(ports)
        self.prepExtSrv(port=8000, cfgFile=cfg_file, cfgProps=cfg_props)
        self.prepCluster(ports, createDatabase = False)
        noOfNodes = len(ports)
        nodeCount = 0
        counts = {p: 0 for p in ports}
        for _ in range(100):
            stat = self.archive(8000, "src/TinyTestFile.fits")
            port = int(stat.getHostId().split(':')[1])
            if (counts[port] == 0):
                counts[port] = 1
                nodeCount += 1
                if (nodeCount == noOfNodes): break
        if (nodeCount != noOfNodes):
            self.fail("Not all specified Archiving Units were contacted " +\
                      "within 100 attempts")

    def test_ArchiveProxyMode_02(self):
        """
        Synopsis:
        Test Archiving Proxy Mode - 1 Master/+archiving, 3 nodes.

        Description:
        The purpose of this Test Case is to test that the Archving Proxy Mode
        works as expected.

        One master node is configured to archive onto three sub-nodes. The
        master node itself supports archiving.

        It is checked that within a limited number of archive attempt, the
        file is archived onto all nodes.

        Expected Result:
        After having configured and started the servers, a test file will
        be archived up to 100 times. It should within these 100 attempts, be
        archived onto all archiving units.

        Test Steps:
        - Start master node configured to archive onto 3 NAUs + having
          own storage sets.
        - Start three NAUs.
        - Archive a small file to the master until it has been archived
          onto all NAUs + the master. Max tries: 100.

        Remarks:
        ...

        Test Data:
        ...
        """
        ports = range(8000, 8004)
        cfg = []
        for i, port in enumerate(ports, 1):
            attr = "NgamsCfg.Streams[1].Stream[2].ArchivingUnit[%d].HostId" % i
            cfg.append((attr, "%s:%d" % (getHostName(), port)))
        self.prepCluster(ports, cfg_props=cfg)

        noOfNodes = len(ports)
        nodeCount = 0
        counts = {p: 0 for p in ports}
        for _ in range(100):
            stat = self.archive(8000, "src/TinyTestFile.fits")
            port = int(stat.getHostId().split(':')[1])
            if (counts[port] == 0):
                counts[port] = 1
                nodeCount += 1
                if (nodeCount == noOfNodes): break
        if (nodeCount != noOfNodes):
            self.fail("Not all specified Archiving Units were contacted " +\
                      "within 100 attempts")


    def test_ArchiveProxyMode_03(self):
        """
        Synopsis:
        Test Archiving Proxy Mode - 1 node no sto. sets, 1 node Offline.

        Description:
        The is virtually the same as test_ArchiveProxyMode_01(). However,
        in this test, one NAU has no free Storage Sets and another is
        Offline.

        Expected Result:
        After having configured and started the servers, a test file will
        be archived up to 100 times. It should within these 100 attempts, be
        archived onto the available archiving units.

        Test Steps:
        - Start master node configured to archive onto 4 NAUs.
        - Start four NAUs.
        - Set all disks for one unit to completed with an SQL query.
        - Bring one unit Offline.
        - Archive a small file to the master until it has been archived
          onto all available NAUs. Max tries: 100.

        Remarks:
        ...

        Test Data:
        ...
        """
        ports = range(8001, 8005)
        cfg_file, cfg_props = self._genArchProxyCfg(ports)
        _, dbObj = self.prepExtSrv(port=8000, cfgFile=cfg_file, cfgProps=cfg_props)
        self.prepCluster(ports, createDatabase = False)
        # Set all Disks in unit <Host>:8002 to completed.
        dbObj.query2("UPDATE ngas_disks SET completed=1 WHERE host_id={0}", args=("%s:8002" % getHostName(),))
        # Set <Host>:8004 to Offline.
        stat = self.offline(8004)

        counts = {8001: 0, 8003: 0}
        noOfNodes = len(counts)
        nodeCount = 0
        for _ in range(100):
            stat = self.archive(8000, "src/TinyTestFile.fits")
            port = int(stat.getHostId().split(':')[1])
            if (counts[port] == 0):
                counts[port] = 1
                nodeCount += 1
                if (nodeCount == noOfNodes): break
        if (nodeCount != noOfNodes):
            self.fail("Not all available Archiving Units were contacted " +\
                      "within 100 attempts")


    def test_VolumeDir_01(self):
        """
        Synopsis:
        Grouping of data volumes under the Volume Dir in the NGAS Root Dir.

        Description:
        The purpose of the test is to verify that it is possible to work
        with the structure:

          <NGAS Root Dir>/<Volume Dir>/<Volume 1>
                                      /<Volume 2>
                                      /...

        In addition it is tested if the handling of a Slot ID as a logical
        name, as opposed to an integer number, is working.

        It is also tested that with this configuration it is possible to
        run with Simulation=0.

        Expected Result:
        When the server goes Online, it should accept the given directory
        structure and it should be possible to archive files into this
        structure.

        Test Steps:
        - Create the volume dirs from an existing structure.
        - Start server with configuration specifying the Volumes Dir in which
          all volumes will be hosted.
        - Archive a FITS file.
        - Check that the files are properly archived into the structure.

        Remarks:
        ...
        """
        # Create basic structure.
        ngasRootDir = tmp_path('NGAS')
        rmFile(ngasRootDir)
        checkCreatePath(ngasRootDir)
        subprocess.check_call(['tar', 'zxf', self.resource('src/volumes_dir.tar.gz')])
        mvFile('volumes', ngasRootDir)

        # Create configuration, start server.
        self.prepExtSrv(delDirs=0, cfgFile="src/ngamsCfg_VolumeDirectory.xml")

        # Archive a file.
        stat = self.archive("src/SmallFile.fits")
        refStatFile = "ref/ngamsArchiveCmdTest_test_VolumeDir_01_01_ref"
        msg = "Incorrect status message from NG/AMS Server"
        self.assert_status_ref_file(refStatFile, stat, msg=msg)

        # Check that the target files have been archived in their
        # appropriate locations.
        checkFile = ngasRootDir + "/volumes/Volume00%d/saf/" +\
                    "2001-05-08/1/TEST.2001-05-08T15:25:00.123.fits.gz"
        for n in (1,2):
            if (not os.path.exists(checkFile % n)):
                self.fail("Did not find archived file as expected: %s" %\
                          (checkFile % n))


    def test_FileSizeZero(self):
        """
        Synopsis:
            Reject files that are of length 0

        Description:
            As above
        """

        self.prepExtSrv(cfgFile = 'src/ngamsCfg.xml')
        zerofile = tmp_path('zerofile.fits')
        open(zerofile, 'a').close()
        for method in ('archive_fail', 'qarchive_fail'):
            status = getattr(self, method)(zerofile, 'application/octet-stream')
            self.assertIn('Content-Length is 0', status.getMessage())


    def test_QArchive(self):
        """
        Synopsis:
            Test QARCHIVE branches

        Description:
            As above
        """
        self.prepExtSrv(cfgFile = 'src/ngamsCfg.xml')

        http_get = functools.partial(ngamsHttpUtils.httpGet, 'localhost', 8888, 'QARCHIVE')

        # No filename given
        params = {'filename': '',
                  'mime_type': 'application/octet-stream'}
        with contextlib.closing(http_get(pars=params, timeout=5)) as resp:
            self.assertEqual(resp.status, 400)
            self.assertEqual(b'NGAMS_ER_MISSING_URI' in resp.read(), True)

        # No mime-type given
        params = {'filename': 'test',
                  'mime_type': ''}
        with contextlib.closing(http_get(pars=params, timeout=5)) as resp:
            self.assertEqual(resp.status, 400)
            self.assertEqual(b'NGAMS_ER_UNKNOWN_MIME_TYPE' in resp.read(), True)

        # File is zero-length
        test_file = tmp_path('zerofile.fits')
        open(test_file, 'a').close()
        params = {'filename': test_file,
                  'mime_type': 'application/octet-stream'}
        with contextlib.closing(http_get(pars=params, timeout=5)) as resp:
            self.assertEqual(resp.status, 400)
            self.assertEqual(b'Content-Length is 0' in resp.read(), True)

        # All is fine
        params = {'filename': self.resource('src/SmallFile.fits'),
                  'mime_type': 'application/octet-stream'}
        with contextlib.closing(http_get(pars=params, timeout=5)) as resp:
            self.assertEqual(resp.status, 200)


    def test_long_archive_quick_fail(self):
        """Tries to archive a huge file, but the server fails quickly and closes the connection"""

        self.prepExtSrv()

        # The server fails quickly due to an unknown exception, so no content
        # is actually read by the server other than the headers
        client = self.get_client(timeout=2)
        test_file = 'dummy_name.unknown_ext'
        self.assert_ngas_status(client.archive_data, generated_file(2**32 + 12),
                                test_file, mimeType='ngas/archive-request',
                                expectedStatus='FAILURE')


    @unittest.skipIf(not _space_available_for_big_file_test,
            "Not enough disk space available to run this test " + \
            "(4 GB are required under %s)" % tmp_path())
    def test_QArchive_big_file(self):

        self.prepExtSrv()

        # For a quicker test
        pars = []
        if _crc32c_available:
            pars.append(('crc_variant', 'crc32c'))
        self.archive_data(generated_file(2**32 + 12), 'dummy_name.fits', pars=pars,
                          mimeType='application/octet-stream', cmd='QARCHIVE')


    def test_filename_with_colons(self):

        self.prepExtSrv()

        test_file = tmp_path('name:with:colons')
        with open(test_file, 'wb') as f:
            f.write(b'   ')

        try:
            for cmd in 'ARCHIVE', 'QARCHIVE':
                for fname in (test_file, 'file:' + os.path.abspath(test_file)):
                    self.archive(fname, mimeType="application/octet-stream", cmd=cmd)
        finally:
            os.unlink(test_file)


    @unittest.skipIf(not _test_checksums, "crc32c not available in your platform")
    def test_checksums(self):
        """
        Check that both the crc32 and crc32c checksums work as expected
        """

        file_id = "SmallFile.fits"
        filename = "src/SmallFile.fits"
        _, db = self.prepExtSrv()
        expected_checksum_crc32 = ngamsFileUtils.get_checksum(4096, self.resource("src/SmallFile.fits"), 'crc32')
        expected_checksum_crc32c = ngamsFileUtils.get_checksum(4096, self.resource("src/SmallFile.fits"), 'crc32c')

        # By default the server is configured to do CRC32
        self.archive(filename, cmd="QARCHIVE", mimeType='application/octet-stream')

        # Try the different user overrides
        self.archive(filename, cmd="QARCHIVE", mimeType='application/octet-stream', pars=[['crc_variant', 'crc32c']])
        self.archive(filename, cmd="QARCHIVE", mimeType='application/octet-stream', pars=[['crc_variant', 1]])
        self.archive(filename, cmd="QARCHIVE", mimeType='application/octet-stream', pars=[['crc_variant', 0]])
        self.archive(filename, cmd="QARCHIVE", mimeType='application/octet-stream', pars=[['crc_variant', -1]])

        # And an old one, which uses the old CRC plugin infrastructure still
        self.archive(filename, mimeType='application/octet-stream')

        res = db.query2("SELECT checksum, checksum_plugin FROM ngas_files WHERE file_id = {} ORDER BY file_version ASC", (file_id,))
        self.assertEqual(7, len(res))
        for idx in (0, 3):
            self.assertEqual(str(expected_checksum_crc32), str(res[idx][0]))
            self.assertEqual('crc32', str(res[idx][1]))
        for idx in (1, 2):
            self.assertEqual(str(expected_checksum_crc32c), str(res[idx][0]))
            self.assertEqual('crc32c', str(res[idx][1]))
        for idx in (4,):
            self.assertEqual(None, res[idx][0])
            self.assertEqual(None, res[idx][1])
        for idx in (5, 6):
            self.assertEqual(str(expected_checksum_crc32), str(res[idx][0]))
            self.assertEqual('ngamsGenCrc32', str(res[idx][1]))

        # Check that the CHECKFILE command works correctly
        # (i.e., the checksums are correctly checked, both new and old ones)
        # In the case we asked for no checksum (file_version==5)
        # we check that the checksum is now calculated
        for version in range(1, 7):
            stat = self.get_status('CHECKFILE', pars=[("file_id", file_id), ("file_version", version)])
            if version == 5:
                self.assertIn('NGAMS_ER_FILE_NOK', stat.getMessage())
            else:
                self.assertNotIn('NGAMS_ER_FILE_NOK', stat.getMessage())

    @unittest.skip("Run manually when necessary")
    def test_performance_of_parallel_crc32(self):

        # Try to use an in-memory filesystem if possible
        size_mb = int(os.environ.get('NGAS_TESTS_CRC32_DATA_SIZE', 300))
        size = size_mb * 1024 * 1024
        test_file = tmp_path('largefile')
        file_uri = 'file://' + test_file
        with open(test_file, 'wb') as f:
            f.seek(size)
            f.write(b'a')

        n_clients = int(os.environ.get('NGAS_TESTS_CRC32_CLIENTS', 1))
        tp = ThreadPool(n_clients)
        for log_blockSize_kb in (2,3):

            blockSize = (2**log_blockSize_kb) * 1024
            cfg = (('NgamsCfg.Server[1].BlockSize', blockSize),
                   ("NgamsCfg.Log[1].LocalLogLevel", "4"))
            self.prepExtSrv(cfgProps=cfg)
            for crc32_variant in (1, 0):
                params = [('crc_variant', crc32_variant)]
                def submit_file(_):
                    return self.qarchive(file_uri, mimeType='application/octet-stream',
                                        pars=params)

                for stat in tp.map(submit_file, range(n_clients)):
                    self.assertEqual(NGAMS_SUCCESS, stat.getStatus())
            self.terminateAllServer()

        tp.close()
        os.unlink(test_file)

    def test_archive_no_versioning(self):
        self._test_archive_no_versioning('ARCHIVE')

    def test_qarchive_no_versioning(self):
        self._test_archive_no_versioning('QARCHIVE')

    def _test_archive_no_versioning(self, cmd):

        _, db = self.prepExtSrv()

        def archive(data, versioning_param=None, version=None, expected_status='SUCCESS'):
            pars = []
            if version is not None:
                pars.append(('file_version', version))
            if versioning_param == 'no_versioning':
                pars.append(('no_versioning', 1))
            elif versioning_param == 'versioning':
                pars.append(('versioning', 0))

            self.archive_data(data, 'file1.txt', 'application/octet-stream',
                              cmd=cmd, pars=pars, expectedStatus=expected_status)

        def assert_retrieve(data, version=None):
            version = -1 if version is None else version
            self.retrieve('file1.txt', fileVersion=version, targetFile=tmp_path())
            with open(tmp_path('file1.txt'), 'rb') as f:
                self.assertEqual(data, f.read())

        # Initial normal archiving of contents, should create versions 1 and 2
        # of the file
        contents1 = os.urandom(64)
        contents2 = os.urandom(64)
        archive(contents1)
        archive(contents2)
        assert_retrieve(contents1, version=1)
        assert_retrieve(contents2, version=2)

        def replace_and_restore(original, new, v1_contents, v2_contents, version=None):

            # Re-archiving using the different parameters that specify we don't
            # a new file version, then put back the original file vesion's contents.
            # We repeat this to ensure that when using QARCHIVE we end up targeting
            # a different disk than the one where the file being replaced is stored in
            file_copies = (2 if cmd == 'ARCHIVE' else 1)
            for _ in range(5):
                for versioning_param in ('no_versioning', 'versioning'):
                    archive(new, versioning_param=versioning_param, version=version)
                    n_files = db.query2('SELECT count(*) FROM ngas_files WHERE file_id={}', ('file1.txt',))[0][0]
                    self.assertEqual(2 * file_copies, n_files)
                    n_files = db.query2('SELECT SUM(number_of_files) FROM ngas_disks')[0][0]
                    self.assertEqual(2 * file_copies, n_files)
                    if version is not None:
                        n_files = db.query2('SELECT count(*) FROM ngas_files WHERE file_id={} AND file_version={}', ('file1.txt', version))[0][0]
                        self.assertEqual(1 * file_copies, n_files)
                    assert_retrieve(v1_contents, version=1)
                    assert_retrieve(v2_contents, version=2)

                    # Put back original contents
                    archive(original, versioning_param=versioning_param, version=version)
                    assert_retrieve(contents1, version=1)
                    assert_retrieve(contents2, version=2)

        data = os.urandom(128)
        # Replace and restore version=1 specifically
        replace_and_restore(contents1, data, data, contents2, version=1)
        # Replace and restore version=2 specifically
        replace_and_restore(contents2, data, contents1, data, version=2)
        # Replace and restore no specific version, should be like version=2
        replace_and_restore(contents2, data, contents1, data)

        # Replace non-existing version, should fail
        for versioning_param in ('no_versioning', 'versioning'):
            archive(data, versioning_param=versioning_param, version=3, expected_status='FAILURE')
            archive(data, versioning_param=versioning_param, version=4, expected_status='FAILURE')
            archive(data, versioning_param=versioning_param, version=50, expected_status='FAILURE')
