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
Module implementing the CDESTROY command
"""

from ngams import genLog, error, info

def destroySingleContainer(srvObj, containerId, checkForChildren):
    """
    Destroys a single container with id containerId.

    If the container contains subcontainers an error is issued.

    Before destroying the container, all files associated to the
    container are removed from it first. If the container has
    subcontainers, it is not removed though

    @param srvObj: ngamsServer.ngamsServer
    @param containerName: string
    @param parentContaienrId: string
    @param parentKnownToExist: bool
    @return: uuid.uuid4
    """

    # Check that the given parent container ID exists
    if checkForChildren:
        sql = "SELECT container_id FROM ngas_containers WHERE parent_container_id = '" + containerId + "'"
        res = srvObj.getDb().query(sql)
        if res[0]:
            raise Exception("Container with id '" + containerId + "' has sub-containers, use the recursive option to remove them")

    # Remove the files that are currently part of the container from it
    sql = "UPDATE ngas_files SET container_id = null WHERE container_id = '" + containerId + "'"
    res = srvObj.getDb().query(sql)

    # Remove the container
    sql = "DELETE FROM ngas_containers WHERE container_id = '" + containerId + "'"
    res = srvObj.getDb().query(sql)

    info(3, "Deleted container '" + containerId + "'")

def destroyContainer(srvObj, containerId, recursive):

    if recursive:
        sql = "SELECT container_id FROM ngas_containers WHERE parent_container_id = '" + containerId + "'"
        res = srvObj.getDb().query(sql)
        for r in res[0]:
            destroyContainer(srvObj, r[0], recursive)

    # If we are recursive, we already ensure that
    # the children containers are gone, so there's no need
    # to check again
    destroySingleContainer(srvObj, containerId, not recursive)

def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Handles the CDESTROY command

    @param srvObj: ngamsServer.ngamsServer
    @param reqPropsObj: ngamsLib.ngamsReqProps
    @param httpRef: ngamsLib.ngamsHttpRequestHandler
    """

    # Check that we have been given either a containerId or a containerName
    containerId = containerName = None
    if reqPropsObj.hasHttpPar("container_id") and reqPropsObj.getHttpPar("container_id").strip():
        containerId = reqPropsObj.getHttpPar("container_id").strip()
    elif reqPropsObj.hasHttpPar("container_name") and reqPropsObj.getHttpPar("container_name").strip():
        containerName = reqPropsObj.getHttpPar("container_name").strip()
    if not containerId and not containerName:
        errMsg = genLog("NGAMS_ER_RETRIEVE_CMD")
        error(errMsg)
        raise Exception, errMsg

    # Check if we have been asked to be recursive
    recursive = False
    if reqPropsObj.hasHttpPar('recursive') and reqPropsObj.getHttpPar('recursive') == '1':
        recursive = True

    # If container_name is specified, and maps to more than one container,
    # (or to none) an error is issued
    if not containerId:
        SQL = "SELECT container_id FROM ngas_containers nc WHERE nc.container_name='" + containerName + "'"
        cursor = srvObj.getDb().query(SQL)
        if len(cursor[0]) == 0:
            errMsg = 'No container found with name ' + containerName
            error(errMsg)
            raise Exception, errMsg
        if len(cursor[0]) > 1:
            errMsg = 'More than one container with name ' + containerName + ' found, cannot proceed with unique fetching'
            error(errMsg)
            raise Exception, errMsg
        containerId = cursor[0][0][0]

    destroyContainer(srvObj, containerId, recursive)

# EOF