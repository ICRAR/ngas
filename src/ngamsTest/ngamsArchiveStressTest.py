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
# "@(#) $Id: ngamsArchiveStressTest.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  12/07/2004  Created
#

"""
This module contains the Test Suite for testing the robustness of the
Archive Command handling
"""

import os, sys, time, threading, thread
from   ngams import *
from   ngamsTestLib import *


TST_STR1 = "Successfully handled Archive Push Request for data file " +\
           "with URI: %s"

RUN_TEST    = 1
THREADS     = {}
THREAD_STAT = {}


def archiveThread(testObj,
                  no,
                  inc,
                  dummy):
    """
    Archive a file X times.

    testObj:  Reference to instance of ngamsTestSuite object (ngamsTestSuite).

    no:       Number allocated to thread (integer).

    inc:      Increment ARCFILE keyword before submitting each Archive Request
              (0|1/integer).

    Returns:  Void
    """
    if (inc):
        filename = "tmp/ngamsArchiveStressTest_%d.fits" % no
        cpFile("src/TinyTestFile.fits", filename)
        incArcfile(filename, step=(100 * no))
    else:
        filename = "src/TinyTestFile.fits"
    testStr = TST_STR1 % os.path.basename(filename)
   
    for n in range(5):
        if (not RUN_TEST):
            THREAD_STAT[no] = "STOPPED"
            break
        if (inc): incArcfile(filename)
        statObj = sendPclCmd(auth=AUTH).archive(filename)
        if (statObj.getMessage() != testStr):
            THREAD_STAT[no] = "FAILURE: Archive Request failed"
    THREAD_STAT[no] = "SUCCESS"
    thread.exit()


class ngamsArchiveStressTest(ngamsTestSuite):
    """
    Synopsis:
    Archive Stress Tests.

    Description:
    Various Test Cases which exercise an intensive archive activity.

    Due to time constraints, the tests do not stress the system as much
    as desirable. Could be enhanced in the near future if more performant
    HW is used.

    Missing Test Cases:
    ...    
    """

    def test_StressTest_1(self):
        """
        Synopsis:
        Archive a small file 20 times, sequentially
        
        Description:
        The Test Case execises the situation where a (small) FITS file is
        archived sequentially for a certain number of times.

        Expected Result:
        All N Archive Requests should be handled successfully.

        Test Steps:
        - Start server.
        - Issue a small FITS file N times, check that a response is returned
          indicating a successfull execution.

        Remarks:
        To make this test useful, it would be better to archive a much
        higher quantity of files to test also the stability of handling
        1000s requests over a long period of time.
        """
        testStr = TST_STR1 % "TinyTestFile.fits"
        self.prepExtSrv()
        for n in range(20):
            statObj = sendPclCmd(auth=AUTH).archive("src/TinyTestFile.fits")
            self.checkEqual(statObj.getMessage(), testStr,
                            "Archive Request failed")


    def _scheduleTest(self,
                      inc):
        """
        Schedule and control the execution of the parallel test.

        inc:      Increment ARCFILE (0|1/integer).

        Returns:  Void.
        """
        RUN_TEST = 1
        self.prepExtSrv()
        for n in range(10):
            args = (self, n, inc, None)
            THREADS[n] = threading.Thread(None, archiveThread,
                                          "ArchiveThread-%d" % n, args)
            THREADS[n].setDaemon(0)
            THREADS[n].start()
            THREAD_STAT[n] = None
        startTime = time.time()
        while ((time.time() - startTime) < 100):
            for key in THREADS.keys():
                if (not THREADS[key].isAlive()): del THREADS[key]
                if (THREADS == {}): break
            if (THREADS == {}): break
            time.sleep(0.100)
        for thrNo in THREAD_STAT.keys():
            if ((str(THREAD_STAT[thrNo]).find("FAILURE") != -1) or
                (THREAD_STAT[thrNo] == None)):
                self.fail("All archiving sub-threads did not finish " +\
                          "processing")
        RUN_TEST = 0


    def test_StressTest_2(self):
        """
        Synopsis:
        Archive small file 20 times/10 threads simultaneously/same File ID.

        Description:
        The purpose of this test is to test the capability of the server
        to handle simultaneously incoming Archive Requests.

        The ARCFILE keyword is not incremented, which means that the resulting
        File ID (using the FITS DAPI), is constant.

        Expected Result:
        The 20 * 10 Archive Requests going on in parallel should handled
        without problems. Since the resulting File ID is constant, and a new
        File Version thus allocated at each Archive Request, there should be
        200 new files archived with the same File ID but different File
        Version and registered in the NGAS DB.

        Test Steps:
        - Start server.
        - Schedule 10 threads to archive a small FITS file in parallel.
        - Wait until all 10 threads have finsished archiving with a max.
          timeout of 100s.
        - In each thread: Check that the file was successfully archived.

        Remarks:
        This Test Case revealed a problem in the NG/AMS Server, whereby
        if several Archive Requests of a file with the same File ID, may
        lead to a blocking situation:

          - The Sybase interface throws an exception because it is tried to
            insert a duplicate row in ngas_files.
          - The thread tries to reconnect.
          - After reconnecting to the DB, it seems that repeating the
            query blocks.

          => Find out where the blocking origins from.
          => Should be possible to distinguish between DB com. errors and
             semantical errors. Only in the former case a reconnection should
             be attempted.
        """
        print "TODO: Disabled."
        return
        self._scheduleTest(0)


    def test_StressTest_3(self):
        """
        Synopsis:
        Archive small file 20 times/10 threads simultaneously/different File ID

        Description:
        The purpose of this test is to test the capability of the server
        to handle simultaneously incoming Archive Requests.

        The ARCFILE keyword is incremented, which means that the resulting
        File ID (using the FITS DAPI), is different at each Archive Request.

        Expected Result:
        The 20 * 10 Archive Requests going on in parallel should handled
        without problems. Since the resulting File ID is constant, and a new
        File Version thus allocated at each Archive Request, there should be
        200 new files with different File Ids archived and registered in the
        NGAS DB.

        Test Steps:
        - Start server.
        - Schedule 10 threads to archive a small FITS file in parallel.
        - Wait until all 10 threads have finsished archiving with a max.
          timeout of 100s.
        - In each thread: - Increment the value of ARCFILE to obtain a
                            new File ID for each file.
                          - Check that the file was successfully archived.

        Remarks:
        ...
        """
        self._scheduleTest(1)


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsArchiveStressTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
