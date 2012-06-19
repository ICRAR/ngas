#******************************************************************************
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

#******************************************************************************
#
# "@(#) $Id: __init__.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  27/09/2002  Created

"""
Module init script to enable viewing of the SQL creation scripts by means
of Pydoc.
"""

import os, glob, commands

from ngams import *

sqlScriptList = [NGAMS_SRC_DIR + "/ngamsSql/ngamsCreateBaseTables.sql",
                 NGAMS_SRC_DIR + "/ngamsSql/ngamsCreateOpsLogTable.sql"]


__doc__ = "\nNG/AMS DB TABLES:\n\n" +\
          "In the following a description of each table in the NGAS DB\n" +\
          "is contained. Each column in the table is documented. Also\n" +\
          "the SQL table creation statement is shown. From this the\n" +\
          "type and the default value for each column can be seen.\n\n"
          
for sqlScript in sqlScriptList:
    fo = open(sqlScript)
    script = fo.readlines()
    fo.close()
    idx = 0
    while (idx < len(script)):
        # Get comment field.
        if (script[idx].find("/*-") == 0):
            __doc__ += "\n\n\n"
            idx += 1
            while (1):
                if (script[idx].find("-*/") == 0): break
                if (script[idx][3:] == ""):
                    __doc__ += "\n"
                else:
                    __doc__ += script[idx][3:]
                idx += 1
        # Get table definition.
        elif (script[idx].find("Create table") == 0):
            __doc__ += "\n"
            while (1):
                __doc__ += script[idx]
                if (script[idx].find(")") == 0): break
                idx += 1            
        idx += 1

# EOF
