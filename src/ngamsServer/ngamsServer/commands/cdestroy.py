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

from .. import containers


def destroyContainer(srvObj, containerId, recursive):
    """
    Destroys the container indicated by containerId, and its
    children containers if indicated by the recursive flag.

    If the container to be destroyed contains subcontainers, but
    the recursive flag hasn't been set, an error is issued

    @param srvObj: ngamsServer.ngamsServer
    @param containerId: string
    @param recursive: bool
    """
    if recursive:
        # TODO: move inside the ngamsDbNgasContainer class
        sql = "SELECT container_id FROM ngas_containers WHERE parent_container_id={0}"
        for r in srvObj.getDb().query2(sql, args=(containerId,)):
            destroyContainer(srvObj, r[0], recursive)

    # If we are recursive, we already ensure that
    # the children containers are gone, so there's no need
    # to check again
    srvObj.getDb().destroySingleContainer(containerId, not recursive)

def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Handles the CDESTROY command

    @param srvObj: ngamsServer.ngamsServer
    @param reqPropsObj: ngamsLib.ngamsReqProps
    @param httpRef: ngamsLib.ngamsHttpRequestHandler
    """

    # Check if we have been asked to be recursive
    recursive = False
    if reqPropsObj.hasHttpPar('recursive') and reqPropsObj.getHttpPar('recursive') == '1':
        recursive = True

    container_id = containers.get_container_id(reqPropsObj, srvObj.db)
    destroyContainer(srvObj, container_id, recursive)

# EOF