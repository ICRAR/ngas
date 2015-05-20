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

'''
Module handling CAPPEND commands

Created on 20 May 2015

:author: rtobar
'''

from ngams import error, genLog
from xml.dom import minidom

def addFilesToContainer(srvObj, containerId, fileIds, containerIdKnownToExist, force):
    """
    Adds the files pointed by fileIds to the container
    pointed by containerId. If a file doesn't exist

    :param srvObj: ngamsServer.ngamsServer
    :param containerId: string
    :param filesIds: list
    :param containerIdKnownToExist: bool
    :param force bool
    """

    # If necessary, check that the container exists
    if not containerIdKnownToExist:
        sql = "SELECT container_id FROM ngas_containers WHERE container_id = '" + containerId + "'"
        res = srvObj.getDb().query(sql)
        if not res[0]:
            msg = "No container with containerId '" + containerId + "' found, cannot append files to it"
            error(msg)
            raise Exception(msg)

    # Append each file
    for fileId in fileIds:

        # Check if the file exists, and if
        # it already is contained in another container
        sql = "SELECT container_id FROM ngas_files WHERE file_id = '" + fileId + "'"
        res = srvObj.getDb().query(sql)
        if not res[0]:
            msg = "No file with fileId '" + fileId + "' found, cannot append it to container"
            error(msg)
            raise Exception(msg)

        prevConatinerId = res[0][0][0]
        if prevConatinerId and not force:
            msg = "File '" + fileId + "' is already associated to container '" + prevConatinerId + "'. To override the 'force' parameter must be given"
            error(msg)
            raise Exception(msg)

        sql = "UPDATE ngas_files SET container_id = '" + containerId + "' WHERE file_id = '" + fileId + "'"
        res = srvObj.getDb().query(sql)

def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Handles the CAPPEND command

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

    # Check if we have been asked to force the operation
    force = False
    if reqPropsObj.hasHttpPar('force') and reqPropsObj.getHttpPar('force') == '1':
        force = True

    # If container_name is specified, and maps to more than one container,
    # an error is issued
    containerIdKnownToExist = False
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
        containerIdKnownToExist = True

    # Read the file list.
    # TODO: Do this properly
    size = reqPropsObj.getSize()
    fileListStr = reqPropsObj.getReadFd().read(size)
    fileList = minidom.parseString(fileListStr)
    fileIds = [el.getAttribute('FileId') for el in fileList.getElementsByTagName('File')]

    addFilesToContainer(srvObj, containerId, fileIds, containerIdKnownToExist, force)
