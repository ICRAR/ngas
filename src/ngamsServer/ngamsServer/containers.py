#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2018
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
"""container-related common functionality"""

from . import InvalidParameter

def get_container_id(request, db, check_id_exists=True):

    # container_id and container_name can be specified, the first takes precedence
    container_id = container_name = None
    if 'container_id' in request:
        container_id = request["container_id"].strip()
    elif 'container_name' in request:
        container_name = request["container_name"].strip()
    if not container_id and not container_name:
        raise InvalidParameter('container_id or container_name needed')

    # Check for existence
    if not container_id:
        container_id = db.getContainerIdForUniqueName(container_name)
    else:
        if check_id_exists and not db.containerExists(container_id):
            raise InvalidParameter("No container with id '%s'" % (container_id,))

    return container_id