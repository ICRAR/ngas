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
# "@(#) $Id: ngamsDbNgasCache.py,v 1.6 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/03/2008  Created
#

"""
Contains queries for accessing the NGAS Cache Table.

This class is not supposed to be used standalone in the present implementation.
It should be used as part of the ngamsDbBase parent classes.
"""

import time, random

from   ngams import *
import ngamsDbCore


class ngamsDbNgasCache(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS Cache Tables.
    """

    def entryInCacheTable(self,
                          diskId,
                          fileId,
                          fileVersion):
        """
        Check if a given cache entry (defined by its Disk ID, File ID,
        File Version) is found in the NGAS Cache Table.

        diskId:        Disk ID of the cache entry (string).
        
        fileId:        File ID of the cache entry (string).
        
        fileVersion:   File Version  of the cache entry (string).

        Returns:       Flag indicating if the entry is found in the table
                       (boolean).
        """
        T = TRACE()

        sqlQuery = "SELECT disk_id FROM ngas_cache " +\
                   "WHERE disk_id = '%s' AND file_id = '%s' AND " +\
                   "file_version = %d"
        sqlQuery = sqlQuery % (diskId, fileId, fileVersion)
        res = self.query(sqlQuery, ignoreEmptyRes = 0)
        return (len(res[0]) == 1)
        

    def insertCacheEntry(self,
                         diskId,
                         fileId,
                         fileVersion,
                         cacheTime,
                         delete,
                         check = True):
        """
        Insert a new cache entry into the NGAS Cache Table.

        diskId:        Disk ID of the cache entry (string).
        
        fileId:        File ID of the cache entry (string).
        
        fileVersion:   File Version  of the cache entry (string).

        cacheTime:     Time the file entered in the cache
                       (= ngas_files.ingestion_time) (float).

        delete:        Flag indicating if the entry is scheduled for
                       deletion (boolean).
                       
        check:         Check if the entry is already in the table. In case yes,
                       just update it (boolean).
                     
        Returns:       Reference to object itself.
        """
        T = TRACE()

        if (check):
            if (self.entryInCacheTable(diskId, fileId, fileVersion)):
                self.updateCacheEntry(diskId, fileId, fileVersion, delete)
                return self
                
        # The entry must be inserted.    
        sqlQuery = "INSERT INTO ngas_cache (disk_id, file_id, " +\
                   "file_version, cache_time, cache_delete) VALUES " +\
                   "('%s', '%s', %d, %.6f, %d)"
        if (delete):
            delete = 1
        else:
            delete = 0
        sqlQuery = sqlQuery % (diskId, fileId, fileVersion, cacheTime, delete)
        self.query(sqlQuery, ignoreEmptyRes = 0)
        
        return self
    

    def updateCacheEntry(self,
                         diskId,
                         fileId,
                         fileVersion,
                         delete):
        """
        Update the online status of this cached data object.
        
        diskId:        Disk ID for the cached data object (string).
        
        fileId:        File ID for the cached data object (string).
        
        fileVersion:   Version of the cached data object (integer).
        
        delete:        Entry scheduled for deletion (integer/0|1).

        Returns:       Reference to object itself.
        """
        T = TRACE()

        sqlQuery = "UPDATE ngas_cache SET cache_delete = %d WHERE " +\
                   "disk_id = '%s' AND file_id = '%s' AND file_version = %d"
        if (delete):
            delete = 1
        else:
            delete = 0
        sqlQuery = sqlQuery % (delete, diskId, fileId, int(fileVersion))
        self.query(sqlQuery, ignoreEmptyRes = 0)
        
        return self


    def deleteCacheEntry(self,
                         diskId,
                         fileId,
                         fileVersion):
        """
        Delete an entry from the NGAS Cache Table.

        diskId:        Disk ID for the cached data object (string).
        
        fileId:        File ID for the cached data object (string).
        
        fileVersion:   Version of the cached data object (integer).
        
        Returns:       Reference to object itself.
        """
        T = TRACE()

        sqlQuery = "DELETE FROM ngas_cache WHERE disk_id = '%s' AND " +\
                   "file_id = '%s' AND file_version = %d"
        sqlQuery = sqlQuery % (diskId, fileId, int(fileVersion))
        try:
            self.query(sqlQuery, ignoreEmptyRes = 0)
        except:
            # Just ignore if the entry is not in the cache table.
            pass

        return self


# EOF
