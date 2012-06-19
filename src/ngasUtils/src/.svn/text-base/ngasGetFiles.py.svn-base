

#
#    ALMA - Atacama Large Millimiter Array
#    (c) European Southern Observatory, 2002
#    Copyright by ESO (in the framework of the ALMA collaboration),
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
# "@(#) $Id: ngasGetFiles.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  04/11/2003  Created
#

"""
Small script to retrieve files listed in a file as File IDs.
"""

import sys

from ngams import *
import ngamsPClient


if __name__ == '__main__':
    """
    Main program.
    """
    client = ngamsPClient.ngamsPClient(getHostName(), 7777)
    fileList = open(sys.argv[1]).readlines()
    setLogCond(0, "", 4, "/tmp/RetrieveFiles.nglog", 4)

    for fileId in fileList:
        fileId = fileId.strip()
        if (not fileId): continue
        info(1,"Requesting file with ID: " + fileId)
        res = client.retrieve2File(fileId)
        try:
            if (res.getStatus() != NGAMS_SUCCESS):
                error("Error retrieving file with ID: " + fileId +\
                      ". Error: " + res.getMessage())
            else:
                info(1,"Successfully retrieved file with ID: " + fileId)
        except:
            pass


# EOF
