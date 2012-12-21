#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
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

#******************************************************************************
#
# "@(#) $Id: ngamsPClientEx.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  26/02/2002  Created
#

"""
Small example application to archive a file.
"""

import sys
from   ngams import *
import ngamsPClient

if __name__ == '__main__':
    """
    Main function.
    """
    # Check the input parameters.
    if (len(sys.argv) != 4):
        print "Correct usage is:\n"
        print "ngamsPClientEx <host> <port> <file URI>\n"
        sys.exit(1)

    # Get the parameters for handling the archiving.
    host    = sys.argv[1]
    port    = sys.argv[2]
    fileUri = sys.argv[3] 

    # Create instance of NG/AMS Python API.
    client = ngamsPClient.ngamsPClient(host, port)

    # Execute the command.
    status = client.archive(fileUri)

    # Handle result - here we simply print the XML status message to stdout.
    print status.genXml(0, 1, 1, 1).toprettyxml('  ', '\n')[0:-1]


# EOF
