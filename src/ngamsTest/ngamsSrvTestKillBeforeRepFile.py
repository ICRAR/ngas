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
# "@(#) $Id: ngamsSrvTestKillBeforeRepFile.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/10/2004  Created
#

"""
Child class of ngamsServer killing itself immediately before handling
the Replication File.
"""

import os, sys, time
from   ngams import *
import ngamsServer
from   ngamsTestLib import *


class ngamsSrvTestKillBeforeRepFile(ngamsServer.ngamsServer):
    """
    Child class of ngamsServer killing itself immediately before handling
    the Replication File.
    """

    def __init__(self):
        """
        Constructor method.
        """
        ngamsServer.ngamsServer.__init__(self)

    
    def test_BeforeRepFile(self):
        """
        Tets method killing server.

        Returns:  Void.
        """
        self.killServer()
        raise Exception, "test_BeforeRepFile: TEST METHOD KILLING SERVER"

 
if __name__ == '__main__':
    """
    Main program executing the test NG/AMS Server
    """
    ngamsTestSrv = ngamsSrvTestKillBeforeRepFile()
    ngamsTestSrv.init(sys.argv)

# EOF
