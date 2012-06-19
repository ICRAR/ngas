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
# "@(#) $Id: ngamsDb_sqlite.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  03/02/2006  Created
#

"""
Contains the the specific implementation for the DB engine for the NGAS
SQLite interface.

Make a link to this file names 'ngamsDb.py' in a directory, which is searched
before the ngams main directory.
"""

from   ngams import *
from   ngamsDbBase import *


class ngamsDb(ngamsDbBase):
    """
    Class to handle the connection to the NGAS DB for the SQLite interface.
    """
  
    def __init__(self,
                 server,
                 db,
                 user,
                 password,
                 createSnapshot = 1,
                 interface = "ngamsSqlite",
                 tmpDir="/tmp",
                 maxRetries = 10,
                 retryWait = 1.0):
        """
        Constructor method.

        server:          DB server name (string).

        db:              DB name (string).
        
        user:            DB user (string).
        
        password:        DB password (string).

        createSnapshot:  Indicates if a DB Snapshot (temporary snapshot
                         files) should be created (integer/0|1).

        interface:       NG/AMS DB Interface Plug-In (string).
        
        tmpDir:          Name of NGAS Temporary Directory (string).

        maxRetries:      Max. number of retries in case of failure (integer).

        retryWait:       Time in seconds to wait for next retry (float).
        """
        ngamsDbBase.__init__(self, server, db, user, password, createSnapshot,
                             interface, tmpDir, maxRetries, retryWait)


    def getMaxDiskNumber(self,
                         cat = None):
        """
        Get the maximum disk index (number) in connection with the
        Logical Disk Names in the DB.

        cat:       'M' for Main, 'R' for Replication (string).

        Returns:   The maximum disk number or None if this could not
                   be generated (integer).
        """
        T = TRACE()

        sqlQuery = "SELECT logical_name FROM ngas_disks"
        if (cat):
            sqlQuery += " WHERE logical_name LIKE '%" + cat + "-%'"
        else:
            sqlQuery += " WHERE logical_name LIKE '%M-%' or " +\
                        "logical_name LIKE '%R-%'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (len(res[0]) == 0):
            retVal = None
        else:
            logNameDic = {}
            for subRes in res[0]:
                tmpName = subRes[0]
                logNameDic[tmpName[(len(tmpName) - 6):]] = 1
            logNames = logNameDic.keys()
            logNames.sort()
            retVal = int(logNames[-1])
        return retVal
  
# EOF
