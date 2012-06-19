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
# "@(#) $Id: ngamsEmailNotificationTest.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/11/2003  Created
#

"""
This module contains the Test Suite for the Email Notification Feature.
"""

import os, sys
from   ngams import *
from   ngamsTestLib import *


class ngamsEmailNotificationTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite for the Email Notification Feature.

    Description:
    The purpose of this Test Suite is to exercise the Email Notification
    Service. It should be found out if all cases where Email Notification
    Messages can be send out, are working properly.

    Also the control flags in the configuration should be tested.

    In particular it should also be verified that the Email Retention Service
    works.

    Missing Test Cases:
      - Review all Test Cases in other Test Suite exercizing the Email
        Notification Service, and add the missing ones.
      - Test Email Notification Retention (max. number of emails for
        emitting obtained, time-out for emitting obtained).
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
        info(1,"TODO: Implement ngamsEmailNotificationTest()!!!!")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsEmailNotificationTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
