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
Module handling CREMOVE commands

Created on 20 May 2015

:author: rtobar
'''

from xml.dom import minidom

from ngamsLib.ngamsCore import NGAMS_HTTP_GET
from .. import containers

def removeFileFromContainer(srvObj, fileId, containerId):
    """
    Removes the file from the container and updates the container size
    accordingly

    @param srvObj: ngamsServer.ngamsServer
    @param fileId: string
    @param containerId: string
    """
    # Remove the file from the container and reduce the total size of the container
    # If the file wasn't already part of any container, fileSize is 0 and no
    # further update is necessary
    fileSize = srvObj.getDb().removeFileFromContainer(fileId, containerId)
    if fileSize:
        srvObj.getDb().addToContainerSize(containerId, -fileSize)

def _handleSingleFile(srvObj, containerId, reqPropsObj):
    """
    Handles the CREMOVE command for the case of
    a single file being given in the URL parameter of
    a GET request

    @param srvObj: ngamsServer.ngamsServer
    @param containerId: string
    @param reqPropsObj: ngamsLib.ngamsReqProps
    """
    fileId = None
    if reqPropsObj.hasHttpPar("file_id") and reqPropsObj.getHttpPar("file_id").strip():
        fileId = reqPropsObj.getHttpPar("file_id")
    if not fileId:
        msg = 'No file_id given in GET request, one needs to be specified'
        raise Exception(msg)

    removeFileFromContainer(srvObj, fileId, containerId)

def _handleFileList(srvObj, containerId, reqPropsObj, httpRef):
    """
    Handles the CREMOVE command for the case of
    file list being given in the body of POST request

    @param srvObj: ngamsServer.ngamsServer
    @param containerId: string
    @param reqPropsObj: ngamsLib.ngamsReqProps
    """
    size = reqPropsObj.getSize()
    fileListStr = httpRef.rfile.read(size)
    fileList = minidom.parseString(fileListStr)
    fileIds = [el.getAttribute('FileId') for el in fileList.getElementsByTagName('File')]
    for fileId in fileIds:
        removeFileFromContainer(srvObj, fileId, containerId)

def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Handles the CREMOVE command

    @param srvObj: ngamsServer.ngamsServer
    @param reqPropsObj: ngamsLib.ngamsReqProps
    @param httpRef: ngamsLib.ngamsHttpRequestHandler
    """

    containerId = containers.get_container_id(reqPropsObj, srvObj.db)

    # If a single fileId has been given via URL parameters
    # and the request is a GET we update that single file
    # Otherwise, we assume a list of files is given in the
    # body of he request
    if reqPropsObj.getHttpMethod() == NGAMS_HTTP_GET:
        _handleSingleFile(srvObj, containerId, reqPropsObj)
    else:
        _handleFileList(srvObj, containerId, reqPropsObj, httpRef)

# EOF
