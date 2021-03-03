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
# "@(#) $Id: ngasUtil_test_template.py,v 1.2 2008/08/19 20:37:46 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  **/**/200*  Created
#

"""
<TEMPLATE FOR TEST SUITE SOURCE CODE FILES - REMOVE THIS LINE>

This module contains the Test Suite for ...
"""

import os, sys
from   ngams import *
import ngasUtils, ngasUtilsLib
from   ngamsTestLib import *
from   ngasUtilsTestLib import *


class ngas_test_tpl(ngasUtilsTestSuite):
    """
    Synopsis:
    ...

    Description:
    ...

    Missing Test Cases:
    ...
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
        pass


def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngams_test_tpl"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)


# EOF
