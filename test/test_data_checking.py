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
# "@(#) $Id: ngamsDataCheckingThreadTest.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/11/2003  Created
#
"""
This module contains the Test Suite for the Data Consistency Checking Thread.
"""

import os
import time

from ngamsLib.ngamsCore import checkCreatePath
from .ngamsTestLib import ngamsTestSuite, getNoCleanUp, setNoCleanUp, \
    tmp_path


class ngamsDataCheckingThreadTest(ngamsTestSuite):
    """
    Synopsis:
    Data Consistency Checking Thread.

    Description:
    This Test Suite exercises the Data Consistency Checking facility.
    It is verified that the various checks are properly functioning, and that
    the DCC Thread is robust towards various errors that might occur.

    Missing Test Cases:
    This Test Suite is very basic. A thorough review should be done and the
    missing Test Cases added.

    In particular Test Cases for detecting the various points checked should
    be added.
    """

    def start_srv(self, *args, **kwargs):
        cfg = (("NgamsCfg.DataCheckThread[1].Active", "1"),
               ("NgamsCfg.DataCheckThread[1].Prio", "1"),
               ("NgamsCfg.DataCheckThread[1].MinCycle", "0T00:00:00"),
               ("NgamsCfg.Log[1].LocalLogLevel", "4"),
               ("NgamsCfg.Db[1].Snapshot", "0"))
        return self.prepExtSrv(cfgProps=cfg, *args, **kwargs)

    def wait_and_count_checked_files(self, cfg, db, checked, unregistered, bad):
        startTime = time.time()
        found = False
        looking_for = "NGAMS_INFO_DATA_CHK_STAT"
        while not found and ((time.time() - startTime) < 60):
            for line in open(cfg.getLocalLogFile(), "r"):
                # The DCC finished
                if looking_for in line:

                    # Nasty...
                    parts = line.split("NGAMS_INFO_DATA_CHK_STAT")
                    parts = parts[1].split(" ")

                    # "6" is what comes after "Number of files checked"
                    # in the log statement
                    nfiles_checked = int(parts[5][:-1])
                    nfiles_unregistered = int(parts[11][:-1])
                    nfiles_bad = int(parts[17][:-1])

                    self.assertEqual(checked, nfiles_checked)
                    self.assertEqual(unregistered, nfiles_unregistered)
                    self.assertEqual(bad, nfiles_bad)
                    found = True
            time.sleep(0.5)
        if not found:
            self.fail("Data Check Thread didn't complete "+\
                      "check cycle within the expected period of time")

        db_bad = db.query2("SELECT count(*) FROM ngas_files WHERE file_status LIKE '1%'")[0][0]
        self.assertEqual(bad, db_bad)

    def _test_data_check_thread(self, registered, unregistered, bad, corrupt=None):

        # Start the server normally without the datacheck thread
        # and perform some archives. Turn off snapshoting also,
        # it messes up with the database updates in one of the tests
        cfg, db = self.prepExtSrv(cfgProps=(("NgamsCfg.Db[1].Snapshot", "0"),))
        for _ in range(3):
            self.archive("src/SmallFile.fits")

        # Cleanly shut down the server, and wait until it's completely down
        old_cleanup = getNoCleanUp()
        setNoCleanUp(True)
        self.termExtSrv(self.extSrvInfo.pop())
        setNoCleanUp(old_cleanup)

        # Potentially corrupt the NGAS data somehow
        if corrupt:
            corrupt(cfg, db)

        # Restart and see what does the data checker thread find
        cfg, db = self.start_srv(delDirs=0, clearDb=0)
        self.wait_and_count_checked_files(cfg, db, registered, unregistered, bad)

    def test_normal_case(self):
        self._test_data_check_thread(6, 0, 0)

    def test_unregistered(self):

        # Manually copy a file into the disk
        trgFile = tmp_path("NGAS/FitsStorage1-Main-1/SmallFile.fits")
        checkCreatePath(os.path.dirname(trgFile))
        self.cp("src/SmallFile.fits", trgFile)

        # It should appear as unregistered when the server checks it
        cfg, db = self.start_srv(delDirs=False)
        self.wait_and_count_checked_files(cfg, db, 0, 1, 0)

    def test_fsize_changed(self):

        # Modify the archived file so it contains extra data
        def add_data(cfg, _):
            root_dir = cfg.getRootDirectory()
            trgFile = os.path.join(root_dir, ('FitsStorage1-Main-1/saf/2001-05-08/1/'
                       'TEST.2001-05-08T15:25:00.123.fits.gz'))
            os.chmod(trgFile, 0o666)
            with open(trgFile, 'ab') as f:
                f.write(os.urandom(16))

        self._test_data_check_thread(6, 0, 1, corrupt=add_data)

    def test_data_changed(self):

        # Modify the archived file so it contains extra data
        def change_data(cfg, _):
            root_dir = cfg.getRootDirectory()
            trgFile = os.path.join(root_dir, ('FitsStorage1-Main-1/saf/2001-05-08/1/'
                       'TEST.2001-05-08T15:25:00.123.fits.gz'))
            os.chmod(trgFile, 0o666)
            with open(trgFile, 'r+b') as f:
                f.seek(-16, 2)
                f.write(os.urandom(16))

        self._test_data_check_thread(6, 0, 1, corrupt=change_data)

    def test_checksum_changed(self):

        # Modify the checksum in the database
        def change_checksum(_, db):
            sql = ('UPDATE ngas_files SET checksum = {0} WHERE file_id = {1} '
                   'AND file_version = 1')
            db.query2(sql, args=('123', 'TEST.2001-05-08T15:25:00.123'))

        self._test_data_check_thread(6, 0, 2, corrupt=change_checksum)