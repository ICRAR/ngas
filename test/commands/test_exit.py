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
# "@(#) $Id: ngamsExitCmdTest.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  20/11/2002  Created
#
"""
This module contains the Test Suite for the EXIT Command.
"""

from ngamsLib import ngamsCore
from ..ngamsTestLib import ngamsTestSuite


class ngamsExitCmdTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite for the  EXIT Command.

    Description:
    The purpose of this Test Suite is to exercise the EXIT Command.
    Both the normal case where exit is granted should be tested, as well
    as when exit is not allowed (if the server is in Online State).

    Missing Test Cases:
    ...
    """

    def test_ExitCmd_1(self):
        """
        Synopsis:
        Normal execution EXIT Command/Offline.

        Description:
        Test that the server terminates if the EXIT Command is submitted
        while the server is in Offline State.

        Expected Result:
        The server in Offline State, should accept the EXIT Command and
        perform a clean shut down.

        Test Steps:
        - Start server (Auto Online = 0).
        - Submit EXIT Command.
        - Check that the response is as expected.
        - Check that the server is no longer running.

        Remarks:
        TODO!: Test that the server is no longer running.
        """
        self.prepExtSrv(autoOnline=0)
        client = self.get_client(timeout=10)
        self.assert_ngas_status(client.exit)
        # TODO: Check that NG/AMS Server has terminated


    def test_ExitCmd_2(self):
        """
        Synopsis:
        Server in Online State -> EXIT Command rejected.

        Description:
        Test that the EXIT Command is rejected when submitted when the
        server is in Online State.

        Expected Result:
        The server should generate an Error Response indicating that the
        command cannot be handled when the server is in Online State.

        Test Steps:
        - Start server (Auto Online = 1).
        - Issue EXIT Command.
        - Check that the request is rejected.
        - Check that the server is still running.

        Remarks:
        TODO!: Check that the server is still running after the EXIT Command.
        """
        self.prepExtSrv(autoOnline=1)
        client = self.get_client(timeout=10)
        stat = client.exit()
        self.assertStatus(stat, ngamsCore.NGAMS_FAILURE)
        self.assertEqual(4015, int(stat.getMessage().split(":")[1])) # NGAMS_ER_IMPROPER_STATE

        # The server is still running
        self.assert_ngas_status(client.status)