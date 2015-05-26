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

import uuid
from ngams import info, error, timeRef2Iso8601, iso8601ToSecs
import ngamsDbCore
import ngamsFileInfo, ngamsContainer

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
        sql = "SELECT container_id FROM ngas_containers WHERE container_id = '" + containerId + "'"
        res = self.query(sql)
        return bool(res[0])

    def getContainerIdForUniqueName(self, containerName):
        """
        Returns the ID of the container that can be uniquely identified
        by containerName. If no container with such a name exists, or if
        more than one exists, an error is raised

        :param containerName: string
        :return: string
        """
        sql = "SELECT container_id FROM ngas_containers nc WHERE nc.container_name='" + containerName + "'"
        cursor = self.query(sql)
        if len(cursor[0]) == 0:
            errMsg = 'No container found with name ' + containerName
            error(errMsg)
            raise Exception, errMsg
        if len(cursor[0]) > 1:
            errMsg = 'More than one container with name ' + containerName + ' found, cannot proceed with unique fetching'
            error(errMsg)
            raise Exception, errMsg
        return cursor[0][0][0]

    def getContainerName(self, containerId):
        """
        Returns the name of the container pointed by containerId.
        If no container with such ID exists, an error is raised

        :param containerId: string
        """
        SQL = "SELECT container_name FROM ngas_containers nc WHERE nc.container_id='" + containerId + "'"
        cursor = self.query(SQL)
        if not cursor[0]:
            errMsg = 'No container found with id ' + containerId
            error(errMsg)
            raise Exception, errMsg
        return cursor[0][0][0]

    def read(self, containerId):
        """
        Reads a single ngamsContainer object from the database

        :param containerId: string
        :return ngamsContainer.ngamsContainer
        """

        sql = "SELECT container_id, container_name, container_size, parent_container_id, ingestion_date FROM ngas_containers WHERE container_id = '" + containerId + "'"
        res = self.query(sql)
        if not res[0]:
            return None

        res = res[0][0]
        cont = ngamsContainer.ngamsContainer()
        cont.setContainerId(res[0])
        cont.setContainerName(res[1])
        cont.setContainerSize(res[2])
        if res[3]:
            parentContainer = ngamsContainer.ngamsContainer()
            parentContainer.setContainerId(res[3])
            cont.setParentContainer(parentContainer)
        if res[4]:
            cont.setIngestionDate(iso8601ToSecs(res[4]))
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
            res = self.query("SELECT " + ngamsDbCore.getNgasFilesCols() + " FROM ngas_files nf WHERE container_id = '" + containerId + "'")
            for r in res[0]:
                fileInfo = ngamsFileInfo.ngamsFileInfo().unpackSqlResult(r)
                container.addFileInfo(fileInfo)

        # Check if it contains children
        res = self.query("select container_id FROM ngas_containers WHERE parent_container_id = '" + container.getContainerId() + "'")
        for r in res[0]:
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

        # Check that the given parent container ID exists
        if parentContainerId and not parentKnownToExist:
            sql = "SELECT container_id FROM ngas_containers WHERE container_id = '" + parentContainerId + "'"
            res = self.query(sql)
            if not res[0]:
                raise Exception("No container with id '" + parentContainerId + "' exists, cannot use it as parent_container_id")

        # If we're associating the container to a parent, check
        # that the parent container doesn't have already a sub-container
        # with the given name
        if parentContainerId:
            sql = "SELECT container_name FROM ngas_containers WHERE parent_container_id ='" + parentContainerId+ "' and container_name='" + containerName +"'"
            res = self.query(sql)
            if res[0]:
                msg = "A container with name '" + containerName + "' already exists as subcontainer of '" +\
                      parentContainerId + "', cannot add a new container with the same name"
                raise Exception(msg)

        # Do the insert with a fresh new UUID, no clash should
        # exist with any existing container
        containerId = uuid.uuid4()
        parentContainerId = "'" + parentContainerId + "'" if parentContainerId else 'null'
        ingestionDate = "'" + timeRef2Iso8601(ingestionDate) + "'" if ingestionDate else 'null'

        sql = "INSERT INTO ngas_containers (" +\
                    "container_id," +\
                    "parent_container_id," +\
                    "container_name," +\
                    "container_size," +\
                    "ingestion_date," +\
                    "container_type) " +\
               "VALUES ('" +\
                     str(containerId) + "'," +\
                     parentContainerId + ",'" +\
                     containerName + "'," +\
                     str(containerSize) + "," +\
                     ingestionDate + "," +\
                     "'logical')"
        res = self.query(sql)

        info(3, "Created container '" + containerName + "' with id '" + str(containerId) + "'")
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
            sql = "SELECT container_id FROM ngas_containers WHERE parent_container_id = '" + containerId + "'"
            res = self.query(sql)
            if res[0]:
                raise Exception("Container with id '" + containerId + "' has sub-containers, use the recursive option to remove them")

        # Remove the files that are currently part of the container from it
        sql = "UPDATE ngas_files SET container_id = null WHERE container_id = '" + containerId + "'"
        self.query(sql)

        # Remove the container
        sql = "DELETE FROM ngas_containers WHERE container_id = '" + containerId + "'"
        self.query(sql)
        info(3, "Destroyed container '" + containerId + "'")

    def setContainerSize(self, containerId, containerSize):
        """
        Updates the size of the indicated container
        """
        sql = "UPDATE ngas_containers SET container_size = " + str(containerSize) + " WHERE container_id = '" + containerId + "'"
        self.query(sql)

    def addToContainerSize(self, containerId, amount):
        """
        Updates the size of the indicated container by the given amount
        """
        amountSql = "+ " + str(amount) if amount >= 0 else "- " + str(amount)
        sql = "UPDATE ngas_containers SET container_size = (container_size " + amountSql + ") WHERE container_id = '" + containerId + "'"
        self.query(sql)

# EOF