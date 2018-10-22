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
# "@(#) $Id: ngamsOnlineCmdTest.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  20/11/2003  Created
#
"""
This module contains the Test Suite for the ONLINE Command.
"""

from ..ngamsTestLib import ngamsTestSuite, sendPclCmd


class ngamsOnlineCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite for the ONLINE Command.

    Description:
    ...

    Missing Test Cases:
    - This whole Test Suite should be reviewed and missing Test Cases added.
    - Re-registration DB -> NgasDiskInfo (no NgasDiskInfo).
    - Re-registration DB -> NgasDiskInfo (DB info newer).
    - Re-registration NgasDiskInfo -> DB (no DB info).
    - Re-registration NgasDiskInfo -> DB (NgasDiskInfo info newer).

    - Make dummy Online Plug-In, which returns no physical disks, and check
      the SW behaves as expected (goes online, reports that there are no disks,
      ...).

    - Check that if there are no Disk Sets available and Archiving is disabled,
      no problems are reported.

    - Following Test Cases +/- completed disks:
      - M Slot: M Disk + R Slot: M Disk
      - M Slot: R Disk + R Slot: R Disk
      - R Slot: M Disk + R Slot: M Disk
      - more combinations illegal/legal ones ...
    """

    def test_OnlineCmd_1(self):
        """
        Synopsis:
        Test basic handling of Online Command.

        Description:
        The purpose of the test is to verify that the server goes Online
        initializing with the specified cfg. file when the ONLINE Command is
        issued.

        Expected Result:
        After being started up and in Offline/Idle State and receiving the
        ONLINE Command, the server should re-load the cfg. and should bring
        the system to Online State according to the cfg. file.

        Test Steps:
        - Start server (Auto Online=0).
        - Send ONLINE Command.
        - Check that the response from the server is as expected.

        Remarks:
        TODO: Check that the server is Online (DB + STATUS Command).
        """
        self.prepExtSrv(autoOnline=0)
        self.assert_ngas_status(sendPclCmd().online)