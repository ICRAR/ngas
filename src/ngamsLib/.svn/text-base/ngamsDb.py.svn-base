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
# "@(#) $Id: ngamsDb.py,v 1.6 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  03/02/2006  Created
#

"""
Front-end class for the DB access module. It loads all DB sub-modules needed
to expose only one class (module) to the rest of the SW.
"""

from ngams import *
from ngamsDbBase import *


class ngamsDb(ngamsDbBase):
    """
    Class to handle the connection to the NGAS DB for the Sybase interface.
    """
  
    def __init__(self,
                 server,
                 db,
                 user,
                 password,
                 createSnapshot = 1,
                 interface = "ngamsSybase",
                 tmpDir = "/tmp",
                 maxRetries = 10,
                 retryWait = 1.0,
                 parameters = None,
                 multipleConnections = False):
        """
        Constructor method.

        server:              DB server name (string).

        db:                  DB name (string).
        
        user:                DB user (string).
        
        password:            DB password (string).

        createSnapshot:      Indicates if a DB Snapshot (temporary snapshot
                             files) should be created (integer/0|1).

        interface:           NG/AMS DB Interface Plug-In (string).
        
        tmpDir:              Name of NGAS Temporary Directory (string).

        maxRetries:          Max. number of retries in case of failure
                             (integer).

        retryWait:           Time in seconds to wait for next retry (float).

        parameters:          Plug-in parameters for the connection (usually for
                             the NG/AMS DB Driver Plug-In).

        multipleConnections: Allow multiple connections or only one (boolean).
        """
        ngamsDbBase.__init__(self, server, db, user, password, createSnapshot,
                             interface, tmpDir, maxRetries, retryWait,
                             parameters, multipleConnections)
    
# EOF
