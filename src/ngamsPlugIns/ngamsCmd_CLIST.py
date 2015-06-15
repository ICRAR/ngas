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

from ngamsLib.ngamsCore import error, NGAMS_HTTP_SUCCESS, NGAMS_XML_MT, NGAMS_SUCCESS

def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Handles the CLIST command

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
        errMsg = "Either container_id or container_name should be given to indicate a unique container"
        error(errMsg)
        raise Exception, errMsg

    # If container_name is specified, and maps to more than one container,
    # (or to none) an error is issued
    containerIdKnownToExist = False
    if not containerId:
        containerId = srvObj.getDb().getContainerIdForUniqueName(containerName)
        containerIdKnownToExist = True

    # If necessary, check that the container exists
    if not containerIdKnownToExist:
        if not srvObj.getDb().containerExists(containerId):
            msg = "No container with containerId '" + containerId + "' found, cannot append files to it"
            error(msg)
            raise Exception(msg)

    # Do it!
    rootCont = srvObj.getDb().readHierarchy(containerId, True)

    statusObj = srvObj.genStatus(NGAMS_SUCCESS, "Successfully retrieved containers list")
    statusObj.addContainer(rootCont)
    statusXml = statusObj.genXml().toxml(encoding="utf8")
    srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, statusXml, NGAMS_XML_MT)

# EOF