#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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

import cStringIO

import pkg_resources

from ngamsLib import ngamsDbCore


def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Handle SubscriberUI command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """

    # We write all contents into a buffer
    # which we write back to the client and at the end
    f = cStringIO.StringIO()

    # Write header contents
    f_PageHdr = pkg_resources.resource_stream(__name__, 'subscription_ui/header.html')  # @UndefinedVariable
    with f_PageHdr:
        for line in f_PageHdr:
            f.write(line)

    # Write the column names as the table header
    col_name_list = ngamsDbCore.getNgasSubscribersCols().split(',')
    for name in col_name_list:
        f.write('<th>' + name + '</th>\n')
    f.write('</tr>\n')

    # Write all subscribers' contents
    for row in srvObj.getDb().query2("SELECT * from ngas_subscribers"):
        f.write('<tr>\n')
        for i, row in enumerate(row):
            # All fields can be updated except subscr_id, which is the PK
            if i != 3:
                f.write('<td contentEditable>%s</td>' % str(row))
            else:
                f.write('<td>%s</td>' % str(row))
        f.write('</tr>\n')
    f.write ('</table>\n')
    f.write ('</body>\n')

    # Writer footer
    f_JScript = pkg_resources.resource_stream(__name__, 'subscription_ui/footer.html')  # @UndefinedVariable
    with f_JScript:
        for line in f_JScript:
            f.write(line)

    # Get final buffer contents and write it back to the client
    s = f.getvalue()
    srvObj.httpReplyGen(reqPropsObj, httpRef, 200, contentType='text/html', dataRef=s)