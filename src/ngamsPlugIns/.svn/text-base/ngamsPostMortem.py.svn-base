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
# "@(#) $Id: ngamsPostMortem.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  30/08/2005  Created
#

"""
Implements the NG/AMS Server Post Mortem Script.

The purpose of this script is to ensure that there is cleaned up after the
server terminates.

Specifically now, it is ensured that all NGAS Data Volumes are unmounted.
"""

import sys

from ngams import *
import ngamsDb, ngamsConfig


if __name__ == '__main__':
    """
    Main program.
    """
    cfgFile = sys.argv[2]
    if (len(sys.argv) >= 5):
        cfgDbId = sys.argv[4]
    else:
        cfgDbId = ""
    cfgObj = ngamsConfig.ngamsConfig().load(cfgFile)
    if ((cfgDbId != "None") and (cfgDbId != "")):
        dbCon = ngamsDb.ngamsDb(cfgObj.getDbServer(), cfgObj.getDbName(),
                                cfgObj.getDbUser(), cfgObj.getDbPassword(),
                                cfgObj.getDbSnapshot(),
                                cfgObj.getDbInterface())
        cfgObj.loadFromDb(cfgDbId, dbCon)
    mtRootPt = cfgObj.getRootDirectory()
    import ngamsLinuxSystemPlugInApi
    ngamsLinuxSystemPlugInApi.umount(mtRootPt)

# EOF
