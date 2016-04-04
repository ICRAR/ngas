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

from   ngamsCore import TRACE
import ngamsDbCore


class ngamsDbNgasCache(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS Cache Tables.
    """

    def insertCacheEntry(self,
                         diskId,
                         fileId,
                         fileVersion,
                         cacheTime,
                         delete):
        """
        Insert a new cache entry into the NGAS Cache Table.

        diskId:        Disk ID of the cache entry (string).

        fileId:        File ID of the cache entry (string).

        fileVersion:   File Version  of the cache entry (string).

        cacheTime:     Time the file entered in the cache
                       (= ngas_files.ingestion_time) (float).

        delete:        Flag indicating if the entry is scheduled for
                       deletion (boolean).

        Returns:       Reference to object itself.
        """
        T = TRACE()

        # The entry must be inserted.
        sqlQuery = "INSERT INTO ngas_cache (disk_id, file_id, " +\
                   "file_version, cache_time, cache_delete) VALUES " +\
                   "({0}, {1}, {2}, {3}, {4})"
        delete = 1 if delete else 0
        self.query2(sqlQuery, args=(diskId, fileId, fileVersion, cacheTime, delete))

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

        sqlQuery = "UPDATE ngas_cache SET cache_delete = {0} WHERE " +\
                   "disk_id = {1} AND file_id = {2} AND file_version = {3}"
        delete = 1 if delete else 0
        self.query2(sqlQuery, args=(delete, diskId, fileId, int(fileVersion)))

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

        sqlQuery = "DELETE FROM ngas_cache WHERE disk_id = {0} AND " +\
                   "file_id = {1} AND file_version = {2}"
        self.query2(sqlQuery, args=(diskId, fileId, int(fileVersion)))
        return self


# EOF
