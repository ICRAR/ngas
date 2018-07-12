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
# "@(#) $Id: ngamsJanitorThreadTest.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  23/04/2002  Created
#
"""
This module contains the Test Suite for the NG/AMS Janitor Thread.
"""

from . import ngamsTestLib


class ngamsJanitorThreadTest(ngamsTestLib.ngamsTestSuite):
    """
    Synopsis:
    Test Suite NG/AMS Janitor Thread.

    Description:
    The purpose of the Test Suite is to verify that the Janitor Thread
    carries out all the tasks assigned to it as expected. This, both under
    normal conditions and under abnormal conditions.

    Missing Test Cases:
    - Test detection of Lost Files.
    - Test all actions/tasks carried out by the Janitor Thread (not
      handling of DB Snapshot - TBD).
    """

    def test_1(self):
        """
        Synopsis:
        ...

        Description:
        ...

        Expected Result:
        ...

        Test Steps:
        - ...

        Remarks:
        ...
        """
        # TODO: Implement ngamsJanitorThreadTest()!!!!