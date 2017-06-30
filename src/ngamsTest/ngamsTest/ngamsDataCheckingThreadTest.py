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

import shutil
import sys
import time

from ngamsLib.ngamsCore import checkCreatePath
from ngamsTestLib import ngamsTestSuite, runTest, sendPclCmd


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
               ("NgamsCfg.Log[1].LocalLogLevel", "4"))
        return self.prepExtSrv(cfgProps=cfg, *args, **kwargs)

    def wait_and_count_checked_files(self, cfg, checked, unregistered, bad):
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

                    self.assertEquals(checked, nfiles_checked)
                    self.assertEquals(unregistered, nfiles_unregistered)
                    self.assertEquals(bad, nfiles_bad)
                    found = True
            time.sleep(1)
        if not found:
            self.fail("Data Check Thread didn't complete "+\
                      "check cycle within the expected period of time")



    def test_DataCheckThread_1(self):
        """
        Synopsis:
        Basic functioning of Data Checking Feature.

        Description:
        Test Test the basic functioning of the Data Check Thread. The
        Data Check Thread is started and it is checked that it performs
        a cycle whereby all files are checked, and a Data Check Entry is
        logged into the NG/AMS Local Log File.

        Expected Result:
        After a given period of time, the DCC Thread should have completed
        one check cycle and have detected possible problems. In this case
        there are no inconsistencies found.

        Test Steps:
        - Start standard NG/AMS Server configured to carry out DCC
          continuosly.
        - Archive a small file 3 times.
        - Wait until the DCC has finished one cycle (NGAMS_INFO_DATA_CHK_STAT
          log written in the log file).
        - Check that the report is OK/that all files were checked.

        Remarks:
        ...
        """
        cfg, _ = self.start_srv()
        client = sendPclCmd()
        for _ in range(3):
            client.archive("src/SmallFile.fits")
        self.wait_and_count_checked_files(cfg, 6, 0, 0)


    def test_inconsistencies(self):
        # Manually copy a file into the disk, it should appear as unregistered
        checkCreatePath('/tmp/ngamsTest/NGAS/FitsStorage1-Main-1/')
        trgFile = "/tmp/ngamsTest/NGAS/FitsStorage1-Main-1/SmallFile.fits"
        shutil.copy("src/SmallFile.fits", trgFile)

        cfg, _ = self.start_srv(delDirs=False)
        self.wait_and_count_checked_files(cfg, 0, 1, 0)


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsDataCheckingThreadTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
