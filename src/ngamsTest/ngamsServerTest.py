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
# "@(#) $Id: ngamsServerTest.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/11/2003  Created
#

"""
This module contains the Test Suite for the NG/AMS Server.
"""

import os, sys
from   ngams import *
from   ngamsTestLib import *


class ngamsServerTest(ngamsTestSuite):
    """
    Synopsis:
    Test Suite of NG/AMS Server.

    Description:
    The purpose of this Test Suite is to exercise specific features of the
    NG/AMS Server not exercised while testing other features (commands etc).

    Missing Test Cases:
    - Analyze if this Test Suite is relevant.
    - Test HTTP authorization
    - Test loading of NG/AMS Configuration from DB (different combinations
      of parameters).
    - Test of handling of Request DB/Info.
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
        info(1,"TODO: Implement ngamsServerTest()!!!!")


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsServerTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
