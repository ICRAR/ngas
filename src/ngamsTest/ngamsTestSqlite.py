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
# "@(#) $Id: ngamsTestSqlite.py,v 1.2 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  23/04/2002  Created
#
"""
Test program that runs the NG/AMS Functional Tests, using SQLite as DB.
"""

import sys
from ngams import *
try:
    import ngamsTestLib
except Exception, e:
    print str(e)
    sys.exit(1)
import ngamsTest
    

# Disabled because some tests involving simulated clusters won't run with
# SQLite as a federated DB.
SKIPLIST1 =\
          "ngamsArchiveCmdTest.test_ArchiveProxyMode_01,\
          ngamsArchiveCmdTest.test_ArchiveProxyMode_02,\
          ngamsArchiveCmdTest.test_ArchiveProxyMode_03"


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    # Specify the SQLite cfg. as default. Can be overwritten if the -cfg
    # option is specified.
    argv = sys.argv[0:1] + ["-cfg", "src/ngamsCfgSqlite.xml"]
    if (len(sys.argv) > 2): argv += sys.argv[2:]
    skip, status, tests, notifEmail = ngamsTest.parseCommandLine(sys.argv)
    if (tests != []):
        for testMod in tests:
            exec "import " + testMod
            exec testMod + ".run()"
    else:
        if (skip):
            skip += SKIPLIST
        else:
            skip = SKIPLIST
        ngamsTest.runAllTests(notifEmail, skip)

# EOF
