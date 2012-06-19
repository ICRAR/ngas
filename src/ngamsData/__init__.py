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
# "@(#) $Id: __init__.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  27/09/2002  Created

import os, glob, commands

from ngams import *

cfgList = glob.glob(NGAMS_SRC_DIR + "/ngamsData/*.xml") +\
          glob.glob(NGAMS_SRC_DIR + "/ngamsData/*.dtd")

__doc__ = "\n\nNG/AMS Data Files:\n\n"
for cfgFile in cfgList:
    __doc__ += "    - " + os.path.basename(cfgFile) + "\n"

    # Create a dummy .py file containing the contents of the file.
    fo = open(cfgFile)
    cfgBuf = fo.read()
    fo.close()
    
    cfgDocFile = cfgFile.replace(".", "_") + ".py"
    commands.getstatusoutput("rm -rf " + cfgDocFile)
    fo = open(cfgDocFile, "w")
    fo.write('"""\n' + cfgBuf + '\n"""\n\n# EOF\n')
    fo.close()

__doc__ += "\nIt is possible to view the contents of these "+\
           "files, by clicking on their corresponding links in this page.\n"

# EOF
