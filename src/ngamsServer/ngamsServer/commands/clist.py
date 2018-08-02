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

from ngamsLib.ngamsCore import NGAMS_XML_MT, NGAMS_SUCCESS
from .. import containers


def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Handles the CLIST command

    @param srvObj: ngamsServer.ngamsServer
    @param reqPropsObj: ngamsLib.ngamsReqProps
    @param httpRef: ngamsLib.ngamsHttpRequestHandler
    """

    # Do it!
    container_id = containers.get_container_id(reqPropsObj, srvObj.db)
    rootCont = srvObj.getDb().readHierarchy(container_id, True)

    statusObj = srvObj.genStatus(NGAMS_SUCCESS, "Successfully retrieved containers list")
    statusObj.addContainer(rootCont)
    statusXml = statusObj.genXml().toxml(encoding="utf8")
    httpRef.send_data(statusXml, NGAMS_XML_MT)

# EOF