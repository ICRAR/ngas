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
Module implementing the CCREATE command
"""

from ngams import NGAMS_HTTP_GET
import ngamsContainer
from xml.dom import minidom

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

    srvObj.getDb().createContainer(containerName, parentContainerId=parentContainerId, parentKnownToExist=False)

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
        cont = ngamsContainer.ngamsContainer(contName)
        if contEl.hasChildNodes():
            for childContEl in [n for n in contEl.childNodes if n.nodeType == minidom.Node.ELEMENT_NODE]:
                cont.addContainer(parseContainerDoc(childContEl))
        return cont
    root = parseContainerDoc(rootContNode)

    # Create all containers
    def createContainers(cont, parentContainerId, parentKnownToExist):
        containerId = srvObj.getDb().createContainer(cont.getContainerName(),
                                                     parentContainerId=parentContainerId,
                                                     parentKnownToExist=parentKnownToExist)
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