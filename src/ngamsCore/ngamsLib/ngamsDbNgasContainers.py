#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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
"""
Module containing SQL queries against the ngas_containers table

@author rtobar, May 2015
"""

import logging
import time, uuid
from ngamsCore import fromiso8601
import ngamsDbCore
import ngamsFileInfo, ngamsContainer


logger = logging.getLogger(__name__)

class ngamsDbNgasContainers(ngamsDbCore.ngamsDbCore):
    """
    A class containing SQL queries against the ngas_containers table
    """

    def containerExists(self, containerId):
        """
        Returns whether the container with ID containerId
        exists (True) or not (False).

        :param containerId: string
        """
        sql = "SELECT container_id FROM ngas_containers WHERE container_id = {0}"
        rows = self.query2(sql, args=(containerId,))
        return bool(rows)

    def getContainerIdForUniqueName(self, containerName):
        """
        Returns the ID of the container that can be uniquely identified
        by containerName. If no container with such a name exists, or if
        more than one exists, an error is raised

        :param containerName: string
        :return: string
        """
        sql = "SELECT container_id FROM ngas_containers WHERE container_name = {0}"
        rows = self.query2(sql, args=(containerName,))
        if not rows:
            errMsg = 'No container found with name ' + containerName
            raise Exception(errMsg)
        if len(rows) > 1:
            errMsg = 'More than one container with name ' + containerName + ' found, cannot proceed with unique fetching'
            raise Exception(errMsg)
        return rows[0][0]

    def getContainerName(self, containerId):
        """
        Returns the name of the container pointed by containerId.
        If no container with such ID exists, an error is raised

        :param containerId: string
        """
        sql = "SELECT container_name FROM ngas_containers nc WHERE nc.container_id = {0}"
        rows = self.query2(sql, args=(containerId,))
        if not rows:
            errMsg = 'No container found with id ' + containerId
            raise Exception(errMsg)
        return rows[0][0]

    def read(self, containerId):
        """
        Reads a single ngamsContainer object from the database

        :param containerId: string
        :return ngamsContainer.ngamsContainer
        """

        sql = "SELECT container_name, container_size, parent_container_id, ingestion_date FROM ngas_containers WHERE container_id = {0}"
        res = self.query2(sql, args=(containerId,))
        if not res:
            return None

        res = res[0]
        cont = ngamsContainer.ngamsContainer()
        cont.setContainerId(containerId)
        cont.setContainerName(res[0])
        cont.setContainerSize(res[1])
        if res[2]:
            parentContainer = ngamsContainer.ngamsContainer()
            parentContainer.setContainerId(res[2])
            cont.setParentContainer(parentContainer)
        if res[3]:
            cont.setIngestionDate(fromiso8601(res[3], local=True))
        return cont

    def readHierarchy(self, containerId, includeFiles=False):
        """
        Reads an ngamsContainer object from the database
        and recursively populates it with its children containers.

        :param containerId: string
        :return ngamsContainer.ngamsContainer
        """

        container = self.read(containerId)
        if includeFiles:

            # Always get the latest version of the files
            # We do this on the software side to avoid any complex SQL query
            # that might not work in some engines
            sql = "SELECT %s FROM ngas_files nf WHERE container_id = {0} ORDER BY nf.file_id, nf.file_version DESC"
            sql = sql % (ngamsDbCore.getNgasFilesCols(self._file_ignore_columnname),)
            res = self.query2(sql, args=(containerId,))
            prevFileId = None
            for r in res:
                fileId = r[ngamsDbCore.NGAS_FILES_FILE_ID]
                if fileId == prevFileId:
                    continue
                prevFileId = fileId
                fileInfo = ngamsFileInfo.ngamsFileInfo().unpackSqlResult(r)
                container.addFileInfo(fileInfo)

        # Check if it contains children
        res = self.query2("SELECT container_id FROM ngas_containers WHERE parent_container_id = {0}", args=(containerId,))
        for r in res:
            container.addContainer(self.readHierarchy(r[0], includeFiles))
        return container

    def createContainer(self, containerName, containerSize=0, ingestionDate=None, parentContainerId=None, parentKnownToExist=False):
        """
        Creates a single container with name containerName.
        The ingestionDate parameter given to this method is a floating
        point number representing the number of seconds since
        the UNIX epoch, as returned by time.time()

        If parentContainerId is given the new container will
        point to it as its parent. The parent container ID is
        checked for existence, unless parentKnownToExist indicates
        that the check is not necessary

        :param containerName: string
        :param containerSize: integer
        :param ingestionDate: float
        :param parentContainerId: string
        :param parentKnownToExist: bool
        :return: uuid.uuid4
        """

        # Sanitize to None if string is empty
        parentContainerId = parentContainerId or None

        # Check that the given parent container ID exists
        if parentContainerId and not parentKnownToExist:
            sql = "SELECT container_id FROM ngas_containers WHERE container_id = {0}"
            res = self.query2(sql, args=(parentContainerId,))
            if not res:
                raise Exception("No container with id '" + parentContainerId + "' exists, cannot use it as parent_container_id")

        # If we're associating the container to a parent, check
        # that the parent container doesn't have already a sub-container
        # with the given name
        if parentContainerId:
            sql = "SELECT container_name FROM ngas_containers WHERE parent_container_id = {0} and container_name = {1}"
            res = self.query2(sql, args=(parentContainerId, containerName))
            if res:
                msg = "A container with name '" + containerName + "' already exists as subcontainer of '" +\
                      parentContainerId + "', cannot add a new container with the same name"
                raise Exception(msg)

        # Do the insert with a fresh new UUID, no clash should
        # exist with any existing container
        containerId = str(uuid.uuid4())

        sql = "INSERT INTO ngas_containers (" +\
                    "container_id," +\
                    "parent_container_id," +\
                    "container_name," +\
                    "container_size," +\
                    "ingestion_date," +\
                    "container_type) " +\
               "VALUES ({0},{1},{2},{3},{4},'logical')"
        self.query2(sql, args=(containerId, parentContainerId, containerName, containerSize, self.asTimestamp(ingestionDate)))

        logger.debug("Created container '%s' with id '%s'", containerName, containerId)
        return containerId

    def destroySingleContainer(self, containerId, checkForChildren):
        """
        Destroys a single container with id containerId.

        If the container contains subcontainers an error is issued.

        Before destroying the container, all files associated to the
        container are removed from it first. If the container has
        subcontainers, it is not removed though

        :param containerId: string
        :param checkForChildren: bool
        """

        # Check that the given parent container ID exists
        if checkForChildren:
            sql = "SELECT container_id FROM ngas_containers WHERE parent_container_id = {0}"
            res = self.query2(sql, args=(containerId,))
            if res:
                raise Exception("Container with id '" + containerId + "' has sub-containers, use the recursive option to remove them")

        # Remove the files that are currently part of the container from it
        sql = "UPDATE ngas_files SET container_id = null WHERE container_id = {0}"
        self.query2(sql, args=(containerId,))

        # Remove the container
        sql = "DELETE FROM ngas_containers WHERE container_id = {0}"
        self.query2(sql, args=(containerId,))
        logger.debug("Destroyed container '%s'", containerId)

    def setContainerSize(self, containerId, containerSize):
        """
        Updates the size of the indicated container
        """
        sql = "UPDATE ngas_containers SET container_size = {0} WHERE container_id = {1}"
        self.query2(sql, args=(containerSize, containerId))

    def addToContainerSize(self, containerId, amount):
        """
        Updates the size of the indicated container by the given amount
        """
        sql = ["UPDATE ngas_containers SET container_size = (container_size "]
        if amount >= 0:
            sql.append("+")
        else:
            sql.append("-")
            amount = -amount
        sql.append(" {0}) WHERE container_id = {1}")
        self.query2(''.join(sql), args=(amount, containerId))

    def closeContainer(self, containerId):
        """
        Marks the container as "closed"; that is, it sets an
        ingestion date on it equals to the current time
        """
        ts = time.time()
        sql = "UPDATE ngas_containers SET ingestion_date = {0} WHERE container_id = {1}"
        self.query2(sql, args=(self.asTimestamp(ts), containerId))

# EOF
