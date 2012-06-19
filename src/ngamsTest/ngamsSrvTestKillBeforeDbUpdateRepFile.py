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
# "@(#) $Id: ngamsSrvTestKillBeforeDbUpdateRepFile.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/10/2004  Created
#

"""
Child class of ngamsServer killing itself after creating the Replication File,
before updating its info in the DB.
"""

import os, sys, time
from   ngams import *
import ngamsServer
from   ngamsTestLib import *


class ngamsSrvTestKillBeforeDbUpdateRepFile(ngamsServer.ngamsServer):
    """
    Child class of ngamsServer killing itself after creating the Replication
    File, before updating its info in the DB.
    """

    def __init__(self):
        """
        Constructor method.
        """
        ngamsServer.ngamsServer.__init__(self)

    
    def test_BeforeDbUpdateRepFile(self):
        """
        Tets method killing server.

        Returns:  Void.
        """
        self.killServer()
        raise Exception, "test_BeforeDbUpdateRepFile: TEST METHOD KILLING " +\
              "SERVER"

 
if __name__ == '__main__':
    """
    Main program executing the test NG/AMS Server
    """
    ngamsTestSrv = ngamsSrvTestKillBeforeDbUpdateRepFile()
    ngamsTestSrv.init(sys.argv)

# EOF
