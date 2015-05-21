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
from ngamsPlugIns.ngamsCmd_CARCHIVE import createContainers
"""
Module implementing the CCREATE command
"""

import uuid
from ngams import info, NGAMS_HTTP_GET
import ngamsMIMEMultipart
from xml.dom import minidom

def insertSingleContainer(srvObj, containerName, parentContainerId=None, parentKnownToExist=False):
    """
    Creates a single container with name containerName.

    If parentContainerId is given the new container will
    point to it as its parent. The parent container ID is
    checked for existence, unless parentKnownToExist indicates
    that the check is not necessary

    @param srvObj: ngamsServer.ngamsServer
    @param containerName: string
    @param parentContaienrId: string
    @param parentKnownToExist: bool
    @return: uuid.uuid4
    """

    # Check that the given parent container ID exists
    if parentContainerId and not parentKnownToExist:
        sql = "SELECT container_id FROM ngas_containers WHERE container_id = '" + parentContainerId + "'"
        res = srvObj.getDb().query(sql)
        if not res[0]:
            raise Exception("No container with id '" + parentContainerId + "' exists, cannot use it as parent_container_id")

    # If we're associating the container to a parent, check
    # that the parent container doesn't have already a sub-container
    # with the given name
    if parentContainerId:
        sql = "SELECT container_name FROM ngas_containers WHERE parent_container_id ='" + parentContainerId+ "' and container_name='" + containerName +"'"
        res = srvObj.getDb().query(sql)
        if res[0]:
            msg = "A container with name '" + containerName + "' already exists as subcontainer of '" +\
                  parentContainerId + "', cannot add a new container with the same name"
            raise Exception(msg)

    # Do the insert
    containerId = uuid.uuid4()
    parentContainerId = "'" + parentContainerId + "'" if parentContainerId else 'null'
    sql = "INSERT INTO ngas_containers (container_id, parent_container_id, container_name, container_size, container_type) " +\
          "VALUES ('" + str(containerId) + "', " + parentContainerId + ", '" + containerName + "', 0, 'logical')"
    res = srvObj.getDb().query(sql)

    info(3, "Created container '" + containerName + "' with id '" + str(containerId) + "'")
    return containerId

def _handleSingleContainer(srvObj, reqPropsObj):
    """
    Handles the request for creation of a single container.
    The name should be given in the container_name request parameter.
    It can optionally indicate a parent container via the
    parent_container_id request paramter

    @param srvObj: ngamsServer.ngamsServer
    @param reqPropsObj: ngamsLib.ngamsReqProps
    @param httpRef: ngamsLib.ngamsHttpRequestHandler
    """

    # Get request paremeters, check that at least the container name has been given
    containerName = parentContainerId = None
    if reqPropsObj.hasHttpPar('container_name'):
        containerName = reqPropsObj.getHttpPar('container_name').strip()
    if reqPropsObj.hasHttpHdr('parent_container_id'):
        parentContainerId = reqPropsObj.getHttpHdr('parent_container_id').strip()
    if not containerName:
        raise Exception('No container_name parameter given, cannot create a nameless container')

    insertSingleContainer(srvObj, containerName, parentContainerId, False)

def _handleComplexContainer(srvObj, reqPropsObj):
    """
    Handles the request for creation of a hierarchy of containers.
    The new hierarchy is specified as an XML document included in the
    request body. The top-level container of the hierarchy can optionally
    include a parent container, which must already exist

    @param srvObj: ngamsServer.ngamsServer
    @param reqPropsObj: ngamsLib.ngamsReqProps
    @param httpRef: ngamsLib.ngamsHttpRequestHandler
    """

    # Parse the message body into a hierarchy of containers
    # TODO: Do this properly; that is, giving the fd to minidom but without it hanging
    # TODO: Use a SAX parser to immediately construct the container
    #       hierarchy instead of passing through the DOM representation first
    size = reqPropsObj.getSize()
    contHierarchyStr = reqPropsObj.getReadFd().read(size)
    contHierarchyDoc = minidom.parseString(contHierarchyStr)
    rootContNode = contHierarchyDoc.childNodes[0]
    parentContainerId = rootContNode.getAttribute('ParentContainerId')

    def parseContainerDoc(contEl):
        contName = contEl.getAttribute('ContainerName')
        cont = ngamsMIMEMultipart.Container(contName)
        if contEl.hasChildNodes():
            for childContEl in [n for n in contEl.childNodes if n.nodeType == minidom.Node.ELEMENT_NODE]:
                cont.addContainer(parseContainerDoc(childContEl))
        return cont
    root = parseContainerDoc(rootContNode)

    # Create all containers
    def createContainers(cont, parentContainerId, parentKnownToExist):
        containerId = insertSingleContainer(srvObj, cont.getContainerName(), parentContainerId, parentKnownToExist)
        cont.setContainerId(containerId)
        for childCont in cont.getContainers():
            createContainers(childCont, str(containerId), True)
    createContainers(root, parentContainerId, False)

def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Handles the CCREATE command

    @param srvObj: ngamsServer.ngamsServer
    @param reqPropsObj: ngamsLib.ngamsReqProps
    @param httpRef: ngamsLib.ngamsHttpRequestHandler
    """

    # If a single fileId has been given via URL parameters
    # and the request is a GET we update that single file
    # Otherwise, we assume a list of files is given in the
    # body of he request
    if reqPropsObj.getHttpMethod() == NGAMS_HTTP_GET:
        _handleSingleContainer(srvObj, reqPropsObj)
    else:
        _handleComplexContainer(srvObj, reqPropsObj)

# EOF