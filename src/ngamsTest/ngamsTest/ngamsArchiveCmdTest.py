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

import cPickle
import commands
import getpass
import glob
import os
import shutil
import sys
import urllib
import httplib
from unittest.case import skip
from contextlib import closing

from ngamsLib.ngamsCore import getHostName, cpFile, NGAMS_ARCHIVE_CMD, checkCreatePath, NGAMS_PICKLE_FILE_EXT, rmFile
from ngamsLib import ngamsLib, ngamsConfig, ngamsStatus, ngamsFileInfo,\
    ngamsCore
from ngamsTestLib import ngamsTestSuite, flushEmailQueue, getEmailMsg, \
    saveInFile, filterDbStatus1, sendPclCmd, pollForFile, getClusterName, \
    sendExtCmd, remFitsKey, writeFitsKey, prepCfg, getTestUserEmail, runTest, \
    copyFile, genTmpFilename, execCmd


# TODO: See how we can actually set this dynamically in the future
_checkMail = False

# FITS checksum-based unit tests are not run because the hardcoded checksum tool
# used by the ngamsFitsPlugIn is nowhere to be found (even on the internet...)
_check_checksums = False

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
        baseCfgFile = "src/ngamsCfg.xml"
        tmpCfgFile = "tmp/ngamsArchiveCmdTest_test_" +\
                     "NormalArchivePushReq_1_cfg_tmp.xml"
        testUserEmail = getpass.getuser()+"@"+ngamsLib.getCompleteHostName()
        cfg = ngamsConfig.ngamsConfig().load(baseCfgFile)
        cfg.storeVal("NgamsCfg.Notification[1].Active", "1")
        cfg.storeVal("NgamsCfg.Notification[1].SmtpHost", "localhost")
        cfg.setDiskChangeNotifList([testUserEmail])
        cfg.storeVal("NgamsCfg.ArchiveHandling[1].FreeSpaceDiskChangeMb",
                     "100000")
        cfg.save(tmpCfgFile, 0)
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=tmpCfgFile)
        flushEmailQueue()
        sendPclCmd(port=8888).archive("src/SmallFile.fits")

        # Check that Disk Change Notification message have been generated.
        if _checkMail:
            mailContClean = getEmailMsg()
            tmpStatFile = "tmp/ngamsArchiveCmdTest_test_NormalArchivePushReq_1_tmp"
            refStatFile = "ref/ngamsArchiveCmdTest_test_NormalArchivePushReq_1_ref"
            saveInFile(tmpStatFile, mailContClean)
            self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect/missing Disk "+\
                              "Change Notification email msg")

        # Check that DB information is OK (completed=1).
        mainDiskCompl = dbObj.getDiskCompleted("tmp-ngamsTest-NGAS-" +\
                                               "FitsStorage1-Main-1")
        self.checkEqual(1, mainDiskCompl,
                        "Disk completion flag not set for Main Disk")
        repDiskCompl = dbObj.getDiskCompleted("tmp-ngamsTest-NGAS-" +\
                                              "FitsStorage1-Rep-2")
        self.checkEqual(1,repDiskCompl,
                        "Disk completion flag not set for Replication Disk")

        # Check that NgasDiskInfo file is OK (completed=1)
        tmpStatFile = "tmp/ngamsArchiveCmdTest_test_" +\
                      "NormalArchivePushReq_1_2_tmp"
        refStatFile = "ref/ngamsArchiveCmdTest_test_" +\
                      "NormalArchivePushReq_1_2_ref"
        statObj = ngamsStatus.ngamsStatus().\
                  load("/tmp/ngamsTest/NGAS/FitsStorage1-Main-1/NgasDiskInfo",
                       1)
        saveInFile(tmpStatFile, filterDbStatus1(statObj.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect contents of " +\
                          "NgasDiskInfo File/Main Disk")
        tmpStatFile = "tmp/ngamsArchiveCmdTest_test_" +\
                      "NormalArchivePushReq_1_3_tmp"
        refStatFile = "ref/ngamsArchiveCmdTest_test_" +\
                      "NormalArchivePushReq_1_3_ref"
        statObj = ngamsStatus.ngamsStatus().\
                  load("/tmp/ngamsTest/NGAS/FitsStorage1-Rep-2/NgasDiskInfo",1)
        saveInFile(tmpStatFile, filterDbStatus1(statObj.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect contents of "+\
                          "NgasDiskInfo File/Rep.4 Disk")


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
        baseCfgFile = "src/ngamsCfg.xml"
        tmpCfgFile = "tmp/ngamsArchiveCmdTest_test_" +\
                     "NormalArchivePushReq_2_cfg_tmp.xml"
        testUserEmail = getpass.getuser()+"@"+ngamsLib.getCompleteHostName()
        cfg = ngamsConfig.ngamsConfig().load(baseCfgFile)
        cfg.storeVal("NgamsCfg.Notification[1].Active", "1")
        cfg.storeVal("NgamsCfg.Notification[1].SmtpHost", "localhost")
        cfg.setDiskSpaceNotifList([testUserEmail])
        cfg.storeVal("NgamsCfg.ArchiveHandling[1].MinFreeSpaceWarningMb",
                     "100000")
        cfg.save(tmpCfgFile, 0)
        cfgObj, dbObj = self.prepExtSrv(8888, cfgFile=tmpCfgFile)
        flushEmailQueue()
        sendPclCmd(port=8888).archive("src/SmallFile.fits")

        # Check that Disk Change Notification message have been generated.
        if _checkMail:
            mailContClean = getEmailMsg()
            idx = mailContClean.find("space (")
            mailContClean = mailContClean[0:mailContClean.find("space (")]
            tmpStatFile = "tmp/ngamsArchiveCmdTest_test_NormalArchivePushReq_2_tmp"
            refStatFile = "ref/ngamsArchiveCmdTest_test_NormalArchivePushReq_2_ref"
            saveInFile(tmpStatFile, mailContClean)
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
        cfgObj, dbObj = self.prepExtSrv(8888)
        sendPclCmd().archive("src/SmallFile.fits")
        statObj = sendPclCmd().archive("src/SmallFile.fits", noVersioning=1)
        filePat = "ngamsArchiveCmdTest_test_NormalArchivePushReq"
        tmpStatFile = "tmp/%s_4_1_tmp" % filePat
        refStatFile = "ref/%s_4_1_ref" % filePat
        saveInFile(tmpStatFile, filterDbStatus1(statObj.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile, "File incorrectly " +\
                          "archived/no_versioning=1")


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
        tmpFile = "tmp/Tmp=Fits=File.fits"
        cpFile("src/SmallFile.fits", tmpFile)
        statObj = sendPclCmd().archive(tmpFile)
        tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
        refStatFile = "ref/ngamsArchiveCmdTest_test_ArchivePushReq_1_1_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, "File with ='s in name " +\
                          "incorrectly handled")


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
                    "ngamsTest.ngamsRaiseEx_NGAMS_ER_DAPI_1"],
                   ["NgamsCfg.JanitorThread[1].SuspensionTime", "0T00:05:00"]]
        self.prepExtSrv(cfgProps=cfgPars)
        statObj = sendPclCmd().archive("src/SmallFile.fits")
        tmpFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
        refFile = "ref/test_BackLogBuf_01_01_ref"
        self.checkFilesEq(refFile, tmpFile, "Unexpected reply from server")
        reqPropsFile = glob.glob("/tmp/ngamsTest/NGAS/back-log/*.pickle")[0]
        fo = open(reqPropsFile)
        tmpReqPropObj = cPickle.load(fo)
        fo.close()
        tmpFile = saveInFile(None, filterDbStatus1(tmpReqPropObj.dumpBuf()))
        refFile = "ref/test_BackLogBuf_01_02_ref"
        self.checkFilesEq(refFile, tmpFile, "Unexpected contents of " +\
                          "Back-Log Buffered Req. Prop. File")
        self.checkFilesEq("src/SmallFile.fits",
                          tmpReqPropObj.getStagingFilename(),
                          "Illegal Back-Log Buffered File: %s" %\
                          tmpReqPropObj.getStagingFilename())
        sendPclCmd().offline()
        sendPclCmd().exit()
        cfgPars = [["NgamsCfg.Permissions[1].AllowArchiveReq", "1"],
                   ["NgamsCfg.ArchiveHandling[1].BackLogBuffering", "1"],
                   ["NgamsCfg.JanitorThread[1].SuspensionTime", "0T00:00:05"]]
        cfgObj, dbObj = self.prepExtSrv(delDirs=0, clearDb=0, cfgProps=cfgPars, skip_database_creation=True)
        pollForFile("/tmp/ngamsTest/NGAS/back-log/*", 0)
        filePat = "/tmp/ngamsTest/NGAS/%s/saf/2001-05-08/1/" +\
                  "TEST.2001-05-08T15:25:00.123.fits.gz"
        pollForFile(filePat % "FitsStorage1-Main-1", 1)
        pollForFile(filePat % "FitsStorage1-Rep-2", 1)
        fileId = "TEST.2001-05-08T15:25:00.123"
        mDiskId = "tmp-ngamsTest-NGAS-FitsStorage1-Main-1"
        rDiskId = "tmp-ngamsTest-NGAS-FitsStorage1-Rep-2"
        mFileInfo = ngamsFileInfo.\
                    ngamsFileInfo().read(getHostName(), dbObj, fileId, 1, mDiskId)
        mFileInfoTmp = saveInFile(None, filterDbStatus1(mFileInfo.dumpBuf()))
        mFileInfoRef = "ref/test_BackLogBuf_01_03_ref"
        self.checkFilesEq(mFileInfoRef, mFileInfoTmp, "Incorrect info in DB "+\
                          "for Main File archived")
        rFileInfo = ngamsFileInfo.\
                    ngamsFileInfo().read(getHostName(), dbObj, fileId, 1, rDiskId)
        rFileInfoTmp = saveInFile(None, filterDbStatus1(rFileInfo.dumpBuf()))
        rFileInfoRef = "ref/test_BackLogBuf_01_04_ref"
        self.checkFilesEq(rFileInfoRef, rFileInfoTmp, "Incorrect info in DB "+\
                          "for Replication File archived")


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
                    "ngamsTest.ngamsRaiseEx_NGAMS_ER_DAPI_1"],
                   ["NgamsCfg.JanitorThread[1].SuspensionTime", "0T00:05:00"],
                   ["NgamsCfg.ArchiveHandling[1].BackLogBuffering", "0"]]
        self.prepExtSrv(cfgProps=cfgPars)
        statObj = sendPclCmd().archive("src/SmallFile.fits")
        tmpFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
        refFile = "ref/test_NoBackLogBuf_01_01_ref"
        self.checkFilesEq(refFile, tmpFile, "Unexpected reply from server")
        pollForFile("/tmp/ngamsTest/NGAS/FitsStorage*-Main-*/staging/*", 0)
        pollForFile("/tmp/ngamsTest/NGAS/back-log/*", 0)
        pollForFile("/tmp/ngamsTest/NGAS/bad-files/*", 0)


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
        srcFile = "src/SmallFile.fits"
        self.prepExtSrv(8888)
        srcFileUrl = "file:" + os.path.abspath(srcFile)
        stat = sendPclCmd(port=8888).archive(srcFileUrl)
        self.assertEquals(stat.getStatus(), 'SUCCESS', None)

        srcFile = "src/SmallFile.fits"
        srcFileUrl = "file:" + os.path.abspath(srcFile)
        stat = sendPclCmd(port=8888).archive(srcFileUrl)
        self.assertEquals(stat.getStatus(), 'SUCCESS', None)


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
        self.prepCluster("src/ngamsCfg.xml",
                         [[8000, None, None, getClusterName()],
                          [8011, None, None, getClusterName()]])
        sendPclCmd(port=8011).archive("src/SmallFile.fits")
        fileUri = "http://%s:8011/RETRIEVE?file_id=" +\
                  "TEST.2001-05-08T15:25:00.123&file_version=1"
        tmpStatFile = sendExtCmd(8000, NGAMS_ARCHIVE_CMD,
                                 [["filename", fileUri % getHostName()],
                                  ["mime_type", "application/x-gfits"]],
                                 filterTags = ["http://"])
        refStatFile = "ref/ngamsArchiveCmdTest_test_ArchivePullReq_2_1_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status " +\
                          "returned for Archive Push Request/HTTP")


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
        stgAreaPat = "/tmp/ngamsTest/NGAS/FitsStorage*-Main-*/staging/*"
        badFilesAreaPat = "/tmp/ngamsTest/NGAS/bad-files/*"
        self.prepExtSrv(8888)

        # Illegal size of FITS file (not a multiple of 2880).
        illSizeFile = "tmp/IllegalSize.fits"
        saveInFile(illSizeFile, "dsjhdjsadhaskjdhaskljdhaskjhd")
        statObj = sendPclCmd().archive(illSizeFile)
        self.checkTags(statObj.getMessage(), ["NGAMS_ER_DAPI_BAD_FILE",
                                              "not a multiple of 2880 " +\
                                              "(size: 29)"])
        pollForFile(stgAreaPat, 0)
        pollForFile(badFilesAreaPat, 0)

        # Missing CHECKSUM keyword.
        if _check_checksums:
            noChecksumFile = "tmp/NoChecksum.fits"
            copyFile("src/SmallFile.fits", noChecksumFile)
            remFitsKey(noChecksumFile, "CHECKSUM")
            statObj = sendPclCmd().archive(noChecksumFile)
            self.checkTags(statObj.getMessage(), ["NGAMS_ER_DAPI_BAD_FILE",
                                                  "Illegal CHECKSUM/DATASUM"])
            pollForFile(stgAreaPat, 0)
            pollForFile(badFilesAreaPat, 0)

            # Illegal checksum in FITS file.
            illChecksumFile = "tmp/IllChecksum.fits"
            copyFile("src/SmallFile.fits", illChecksumFile)
            writeFitsKey(illChecksumFile, "CHECKSUM", "BAD-CHECKSUM!", "TEST")
            statObj = sendPclCmd().archive(illChecksumFile)
            self.checkTags(statObj.getMessage(), ["NGAMS_ER_DAPI_BAD_FILE",
                                                  "Illegal CHECKSUM/DATASU"])
            pollForFile(stgAreaPat, 0)
            pollForFile(badFilesAreaPat, 0)

        # Unknown mime-type.
        unknownMtFile = "tmp/UnknownMimeType.stif"
        copyFile("src/SmallFile.fits", unknownMtFile)
        statObj = sendPclCmd().archive(unknownMtFile)
        tmpStatFile = "tmp/ngamsArchiveCmdTest_test_ErrHandling_1_4_tmp"
        refStatFile = "ref/ngamsArchiveCmdTest_test_ErrHandling_1_4_ref"
        saveInFile(tmpStatFile, filterDbStatus1(statObj.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status " +\
                          "returned for Archive Push Request/Unknown Mimetype")
        pollForFile(stgAreaPat, 0)
        pollForFile(badFilesAreaPat, 0)

        # Illegal File URI at Archive Pull.
        illFileUri = "http://unknown.domain.com/NonExistingFile.fits"
        tmpStatFile = sendExtCmd(8888, NGAMS_ARCHIVE_CMD,
                                 [["file_uri", illFileUri]])
        refStatFile = "ref/ngamsArchiveCmdTest_test_ErrHandling_1_5_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status " +\
                          "returned for Archive Pull Request/Illegal URI")
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
        cfgObj, dbObj = self.prepExtSrv()
        repDiskPath = "/tmp/ngamsTest/NGAS/FitsStorage1-Rep-2"
        commands.getstatusoutput("chmod -R a-rwx %s" % repDiskPath)
        statObj = sendPclCmd().archive("src/SmallFile.fits")
        commands.getstatusoutput("chmod -R a+rwx %s" % repDiskPath)
        self.assertEquals(ngamsCore.NGAMS_FAILURE, statObj.getStatus())
        msg = "Incorrect status returned for Archive Push Request/Replication Disk read-only"
        self.assertEquals(4011, int(statObj.getMessage().split(":")[1]), msg) # NGAMS_ER_ARCHIVE_PUSH_REQ:4011


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
        cfgObj, dbObj = self.prepExtSrv()
        sqlQuery = "UPDATE ngas_disks SET completed=1 WHERE host_id={0}"
        dbObj.query2(sqlQuery, args=(getHostName(),))
        statObj = sendPclCmd().archive("src/SmallFile.fits")
        sqlQuery = "UPDATE ngas_disks SET completed=0 WHERE host_id={0}"
        dbObj.query2(sqlQuery, args=(getHostName(),))
        refStatFile = "ref/ngamsArchiveCmdTest_test_ErrHandling_3_1_ref"
        tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status " +\
                          "returned for Archive Push Request/" +\
                          "No Free Storage Sets")
        pollForFile("/tmp/ngamsTest/NGAS/FitsStorage*-Main-*/staging/*", 0)
        pollForFile("/tmp/ngamsTest/NGAS/bad-files/*", 0)


    @skip("Test case requires missing file under src/")
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
        cpFile("src/1MB-2MB-TEST.fits.gz", fitsFile)
        execCmd("gunzip %s" % fitsFile)
        for n in range(3): sendPclCmd().archive(fitsFile[0:-3])  # #1
        # Check: Disk/Slot 1 is marked as completed.
        # Check: Disk/Slot 2 is not marked as completed.
        preFix = "ref/ngamsArchiveCmdTest_test_"
        for diFiles in [["/tmp/ngamsTest/NGAS/Data1-Main-1/NgasDiskInfo",
                         preFix + "MainDiskSmallerThanRep_1_1_ref"],
                        ["/tmp/ngamsTest/NGAS/Data1-Rep-2/NgasDiskInfo",
                         preFix + "MainDiskSmallerThanRep_1_2_ref"]]:
            statObj = ngamsStatus.ngamsStatus().load(diFiles[0], 1)
            tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
            self.checkFilesEq(diFiles[1], tmpStatFile, "Incorrect contents " +\
                              "of NgasDiskInfo File/Main Disk (%s)"%diFiles[0])
        # Check: Email Notification Message is sent indicating to change
        #        only disk/Slot 1.
        if _checkMail:
            refStatFile = preFix + "MainDiskSmallerThanRep_1_3_ref"
            tmpStatFile = saveInFile(None, getEmailMsg())
            self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect/missing Disk "+\
                              "Change Notification Email Msg")
        #####################################################################

        #####################################################################
        # Phase 2: Continue to archive data until disk/Slot 3 fills up.
        #####################################################################
        for n in range(3): sendPclCmd().archive(fitsFile[0:-3])  # #2
        sendPclCmd().archive(fitsFile[0:-3]) # #3
        # Check: That disk/Slot 3 is marked as completed + disk/Slot 4
        # is not marked as completed.
        for diFiles in [["/tmp/ngamsTest/NGAS/Data2-Main-3/NgasDiskInfo",
                         preFix + "MainDiskSmallerThanRep_1_4_ref"],
                        ["/tmp/ngamsTest/NGAS/Data2-Rep-4/NgasDiskInfo",
                         preFix + "MainDiskSmallerThanRep_1_5_ref"]]:
            statObj = ngamsStatus.ngamsStatus().load(diFiles[0], 1)
            tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
            self.checkFilesEq(diFiles[1], tmpStatFile, "Incorrect contents " +\
                              "of NgasDiskInfo File/Main Disk (%s)"%diFiles[0])
        # Check: That Email Notification sent out indicating to change
        # disk/Slot 3.
        if _checkMail:
            refStatFile = preFix + "MainDiskSmallerThanRep_1_6_ref"
            tmpStatFile = saveInFile(None, getEmailMsg())
            self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect/missing Disk "+\
                              "Change Notification Email Msg")
        #####################################################################

        #####################################################################
        # Phase 3: Bring server Offline + 'replace' disks in Slot 1/3 +
        #          continue to archive until disk/Slot 5 fills up.
        #####################################################################
        sendPclCmd().offline()
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
        sendPclCmd().online()

        for n in range(8): sendPclCmd().archive(fitsFile[0:-3])  # #4
        # Re-init the server to ensure the NgasDiskInfo file has been updated.
        sendPclCmd().init()

        # Check: That disk/Slot 5 is marked as completed.
        # Check: That disk/Slot 6 is not marked as completed.
        for diFiles in [["/tmp/ngamsTest/NGAS/Data3-Main-5/NgasDiskInfo",
                         preFix + "MainDiskSmallerThanRep_1_7_ref"],
                        ["/tmp/ngamsTest/NGAS/Data3-Rep-6/NgasDiskInfo",
                         preFix + "MainDiskSmallerThanRep_1_8_ref"]]:
            statObj = ngamsStatus.ngamsStatus().load(diFiles[0], 1)
            tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
            msg = "Incorrect contents of NgasDiskInfo File/Main Disk (%s)/1"
            self.checkFilesEq(diFiles[1], tmpStatFile, msg % diFiles[0])

        # Check: That Email Notification sent out indicating to replace
        #        disk/Slot 5.
        if _checkMail:
            refStatFile = preFix + "MainDiskSmallerThanRep_1_9_ref"
            tmpStatFile = saveInFile(None, getEmailMsg())
            self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect/missing Disk "+\
                              "Change Notification Email Msg")
        #####################################################################

        #####################################################################
        # Phase 4: Continue to archive until disk/Slot 2 fills up.
        #####################################################################
        for n in range(2): sendPclCmd().archive(fitsFile[0:-3])  # #5

        # Re-init the server to ensure the NgasDiskInfo file has been updated.
        sendPclCmd().init()

        # Check: That disk/Slot 1 is not marked as completed.
        # Check: That disk/Slot 2 is marked as completed.
        for diFiles in [["/tmp/ngamsTest/NGAS/Data1-Main-1/NgasDiskInfo",
                         preFix + "MainDiskSmallerThanRep_1_10_ref"],
                        ["/tmp/ngamsTest/NGAS/Data1-Rep-2/NgasDiskInfo",
                         preFix + "MainDiskSmallerThanRep_1_11_ref"]]:
            statObj = ngamsStatus.ngamsStatus().load(diFiles[0], 1)
            msg = "Incorrect contents of NgasDiskInfo File/Main Disk (%s)/2"
            tmpStatFile = saveInFile(None, filterDbStatus1(statObj.dumpBuf()))
            self.checkFilesEq(diFiles[1], tmpStatFile, msg % diFiles[0])
        # Check: That Email Notification sent out indicating to replace
        #        disk/Slot 2.
        if _checkMail:
            refStatFile = preFix + "MainDiskSmallerThanRep_1_12_ref"
            tmpStatFile = saveInFile(None, getEmailMsg())
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
        stgPat = "/tmp/ngamsTest/NGAS/%s/staging/%s.fits"
        diskList = ["FitsStorage1-Main-1", "FitsStorage2-Main-3",
                    "FitsStorage3-Main-5", "PafStorage-Main-7",
                    "LogStorage-Main-9"]
        for diskName in diskList:
            stgFile = stgPat % (diskName, diskName)
            checkCreatePath(os.path.dirname(stgFile))
            cpFile("src/SmallFile.fits", stgFile)
            fo = open("%s.%s" % (stgFile, NGAMS_PICKLE_FILE_EXT), "w")
            fo.write("TEST/DUMMY REQUEST PROPERTIES FILE: %s" % diskName)
            fo.close()
        sendPclCmd().offline()
        sendPclCmd().exit()
        self.prepExtSrv(delDirs=0, clearDb=0)
        badDirPat = "/tmp/ngamsTest/NGAS/bad-files/BAD-FILE-*-%s.fits"
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
        self.prepExtSrv(srvModule="ngamsSrvTestKillBeforeArchCleanUp")
        try:
            sendPclCmd().archive("src/SmallFile.fits")
        except:
            pass
        reqPropStgFile = "/tmp/ngamsTest/NGAS/FitsStorage1-Main-1/staging/" +\
                         "*-SmallFile.fits.pickle"
        pollForFile(reqPropStgFile, 1)
        self.prepExtSrv(delDirs=0, clearDb=0)
        reqPropBadFile = "/tmp/ngamsTest/NGAS/bad-files/BAD-FILE-*" +\
                         "-SmallFile.fits.pickle"
        pollForFile(reqPropBadFile, 1)


    def test_NoDapi_01(self):
        """
        Synopsis:
        No DAPI installed to handled request/Archive Push/wait=1.

        Description:
        If the specified DAPI cannot be loaded during the Archive Request
        handling it is not possible to handle the Archive Request and
        the server must bail out.

        Expected Result:
        The Archive Request handling should be interrupted and an error
        reply sent back to the client since wait=1. The files in connection
        with the request should be removed from the Staging Area.

        Test Steps:
        - Start server with configuration specifying a non-existing DAPI
          to handle FITS files.
        - Archive FITS file (wait=1).
        - Check error reply from server.
        - Check that Staging Area is cleaned up.
        - Check that no files were archived.
        - Check that no files moved to Bad Files Area.

        Remarks:
        ...
        """
        cfgPars = [["NgamsCfg.Streams[1].Stream[2].PlugIn", "NonExistingDapi"]]
        self.prepExtSrv(cfgProps=cfgPars)
        stat = sendPclCmd().archive("src/SmallFile.fits")
        tmpStatFile = saveInFile(None, filterDbStatus1(stat.dumpBuf()))
        refStatFile = "ref/test_NoDapi_01_01_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status " +\
                          "message from NG/AMS Server")
        pollForFile("/tmp/ngamsTest/NGAS/FitsStorage1-Main-1/staging/*", 0)
        arcFileNm = "/tmp/ngamsTest/NGAS/%s/saf/2001-05-08/1/*"
        pollForFile(arcFileNm % "FitsStorage1-Main-1", 0)
        pollForFile(arcFileNm % "FitsStorage1-Rep-2", 0)
        pollForFile("/tmp/ngamsTest/NGAS/bad-files/*", 0)


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

    def _genArchProxyCfg(self,
                         streamElList,
                         nauList):
        """
        Generate a cfg. file.
        """
        baseCfgFile = "src/ngamsCfgNoStreams.xml"
        tmpCfgFile = genTmpFilename("test_cfg_") + ".xml"
        cfg = ngamsConfig.ngamsConfig().load(baseCfgFile)
        for idx,streamEl in enumerate(streamElList,1):
            strEl = self.__STR_EL % idx
            for strAttrEl, attrVal in streamEl:
                nextAttr = strAttrEl % strEl
                cfg.storeVal(nextAttr, attrVal)
            for hostIdx, nau in enumerate(nauList,1):
                nauAttr = self.__ARCH_UNIT_ATR % (strEl, hostIdx)
                cfg.storeVal(nauAttr, nau)
                hostIdx += 1
            idx += 1
        cfg.save(tmpCfgFile, 0)
        return tmpCfgFile
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
        naus = {"%s:8001" % getHostName(): 0, "%s:8002" % getHostName(): 0,
                "%s:8003" % getHostName(): 0, "%s:8004" % getHostName(): 0}
        nmuCfg = self._genArchProxyCfg(self.__STREAM_LIST, naus.keys())
        #extProps = [["NgamsCfg.Log[1].LocalLogLevel", "5"]]
        extProps = []
        self.prepExtSrv(8000, cfgFile=nmuCfg, cfgProps=extProps)
        self.prepCluster("src/ngamsCfg.xml",
                         [[8001, None, None, getHostName()],
                          [8002, None, None, getHostName()],
                          [8003, None, None, getHostName()],
                          [8004, None, None, getHostName()]],
                         createDatabase = False)
        noOfNodes = len(naus.keys())
        nodeCount = 0
        for _ in xrange(100):
            stat = sendPclCmd(port=8000).\
                   archive("src/TinyTestFile.fits")
            self.assertEquals(stat.getStatus(), 'SUCCESS', "Didn't successfully archive file: %s / %s" % (stat.getStatus(), stat.getMessage()))
            if (naus[stat.getHostId()] == 0):
                naus[stat.getHostId()] = 1
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
        naus = {"%s:8000" % getHostName(): 0, "%s:8001" % getHostName(): 0,
                "%s:8002" % getHostName(): 0, "%s:8003" % getHostName(): 0}
        baseCfgFile = "src/ngamsCfg.xml"
        tmpCfgFile = genTmpFilename("test_cfg_") + ".xml"
        cfg = ngamsConfig.ngamsConfig().load(baseCfgFile)
        idx = 1
        for nau in naus.keys():
            attr = "NgamsCfg.Streams[1].Stream[2].ArchivingUnit[%d].HostId" %\
                   (idx)
            cfg.storeVal(attr, nau)
            idx += 1
        cfg.save(tmpCfgFile, 0)
        self.prepCluster(tmpCfgFile,
                         [[8000, None, None, getClusterName()],
                          [8001, None, None, getClusterName()],
                          [8002, None, None, getClusterName()],
                          [8003, None, None, getClusterName()]])

        noOfNodes = len(naus.keys())
        nodeCount = 0
        for n in range(100):
            stat = sendPclCmd(port=8000).\
                   archive("src/TinyTestFile.fits")
            if (naus[stat.getHostId()] == 0):
                naus[stat.getHostId()] = 1
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
        naus = {"%s:8001" % getHostName(): 0, "%s:8002" % getHostName(): 0,
                "%s:8003" % getHostName(): 0, "%s:8004" % getHostName(): 0}
        ncuCfg = self._genArchProxyCfg(self.__STREAM_LIST, naus.keys())
        cfgObj, dbObj = self.prepExtSrv(8000, cfgFile=ncuCfg)
        self.prepCluster("src/ngamsCfg.xml",
                         [[8001, None, None, getHostName()],
                          [8002, None, None, getHostName()],
                          [8003, None, None, getHostName()],
                          [8004, None, None, getHostName()]],
                          createDatabase = False)
        # Set all Disks in unit <Host>:8002 to completed.
        dbObj.query2("UPDATE ngas_disks SET completed=1 WHERE host_id={0}", args=("%s:8002" % getHostName(),))
        # Set <Host>:8004 to Offline.
        stat = sendPclCmd(port=8004).offline()

        del naus["%s:8002" % getHostName()]
        del naus["%s:8004" % getHostName()]
        noOfNodes = len(naus.keys())
        nodeCount = 0
        for n in range(100):
            stat = sendPclCmd(port=8000).\
                   archive("src/TinyTestFile.fits")
            if (naus[stat.getHostId()] == 0):
                naus[stat.getHostId()] = 1
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
        ngasRootDir = "/tmp/ngamsTest/NGAS"
        checkCreatePath(ngasRootDir)
        tarCmd = "tar zxvf src/volumes_dir.tar.gz"
        stat, out = commands.getstatusoutput(tarCmd)
        if stat:
            self.fail("Failed to untar volumes file: " + out)
        rmFile(os.path.normpath("%s/volumes") % ngasRootDir)
        mvCmd = "mv volumes %s" % ngasRootDir
        stat, out = commands.getstatusoutput(mvCmd)
        if stat:
            self.fail("Failed to move volumes to ngasRoot: " + out)

        # Create configuration, start server.
        self.prepExtSrv(delDirs=0, cfgFile="src/ngamsCfg_VolumeDirectory.xml")

        # Archive a file.
        stat = sendPclCmd().archive("src/SmallFile.fits")
        tmpStatFile = saveInFile(None, filterDbStatus1(stat.dumpBuf()))
        refStatFile = "ref/ngamsArchiveCmdTest_test_VolumeDir_01_01_ref"
        self.checkFilesEq(refStatFile, tmpStatFile, "Incorrect status " +\
                          "message from NG/AMS Server")

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

        # Test ARCHIVE
        self.prepExtSrv(8888, cfgFile = 'src/ngamsCfg.xml')

        open('tmp/zerofile.fits', 'a').close()
        client = sendPclCmd(port=8888)
        status = client.archive('tmp/zerofile.fits', 'application/octet-stream', cmd = 'ARCHIVE')
        self.checkEqual(status.getStatus(), 'FAILURE', None)
        self.checkEqual('Content-Length is 0' in status.getMessage(), True, None)

        # Test QARCHIVE
        status = client.archive('tmp/zerofile.fits', 'application/octet-stream', cmd = 'QARCHIVE')
        self.checkEqual(status.getStatus(), 'FAILURE', None)
        self.checkEqual('Content-Length is 0' in status.getMessage(), True, None)


    def test_QArchive(self):
        """
        Synopsis:
            Test QARCHIVE branches

        Description:
            As above
        """
        self.prepExtSrv(8888, cfgFile = 'src/ngamsCfg.xml')

        host = 'localhost:8888'
        method = 'GET'
        cmd = 'QARCHIVE'

        params = {'filename': '',
                  'mime_type': 'application/octet-stream'}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, '', {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 400, None)
            self.checkEqual('NGAMS_ER_MISSING_URI' in resp.read(), True, None)

        params = {'filename': 'test',
                  'mime_type': ''}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, '', {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 400, None)
            self.checkEqual('NGAMS_ER_UNKNOWN_MIME_TYPE' in resp.read(), True, None)

        test_file = 'tmp/zerofile.fits'
        open(test_file, 'a').close()
        params = {'filename': test_file,
                  'mime_type': 'application/octet-stream'}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, open(test_file, 'rb'), {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 400, None)
            self.checkEqual('Content-Length is 0' in resp.read(), True, None)

        test_file = 'src/SmallFile.fits'
        params = {'filename': test_file,
                  'mime_type': 'application/octet-stream'}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, open(test_file, 'rb'), {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 200, None)

        test_file = 'file:/bin/cp'
        params = {'filename': '{0}/?file_version={1}'.format(test_file, 'test'),
                  'mime_type': 'application/octet-stream'}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, '', {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 400, None)
            self.checkEqual('file_version is not an integer' in resp.read(), True, None)

        test_file = 'file:/bin/cp'
        params = {'filename': '{0}?file_id={1}'.format(test_file, 'test'),
                  'mime_type': 'application/octet-stream'}
        params = urllib.urlencode(params)
        selector = '{0}?{1}'.format(cmd, params)
        with closing(httplib.HTTPConnection(host, timeout = 5)) as conn:
            conn.request(method, selector, '', {})
            resp = conn.getresponse()
            self.checkEqual(resp.status, 200, None)


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsArchiveCmdTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
