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
# "@(#) $Id: ngamsGenericOfflinePlugIn.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  28/02/2007  Created.
#

"""
Module that contains a generic Offline Plug-In for NGAS.
"""

from   ngams import *
import ngamsPlugInApi
import ngamsLinuxSystemPlugInApi, ngamsEscaladeUtils
from ngamsGenericPlugInLib import notifyRegistrationService


def ngamsMWAOfflinePlugIn(srvObj,
                              reqPropsObj = None):
    """
    Generic NGAS Offline Plug-In.

    srvObj:        Reference to instance of the NG/AMS Server class
                   (ngamsServer).

    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).

    Returns:       Void.
    """
    T = TRACE()
    notifyRegistrationService(srvObj, 'offline')

if __name__ == '__main__':
    """
    Main function.
    """
    import sys
    import ngamsConfig, ngamsDb
    import ngamsServer

    setLogCond(0, "", 0, "", 1)
    
    if (len(sys.argv) != 2):
        print "\nCorrect usage is:\n"
        print "% python ngamsMWAOfflinePlugIn <NGAMS Cfg.>\n"
        sys.exit(0)

    srvObj = ngamsServer.ngamsServer()  
    ngamsCfgObj = ngamsConfig.ngamsConfig().load(sys.argv[1])
    dbConObj = ngamsDb.ngamsDb(ngamsCfgObj.getDbServer(),
                               ngamsCfgObj.getDbName(),
                               ngamsCfgObj.getDbUser(),
                               ngamsCfgObj.getDbPassword(),
                               interface=ngamsCfgObj.getDbInterface())
    srvObj.setCfg(ngamsCfgObj).setDb(dbConObj)
    ngamsMWAOfflinePlugIn(srvObj)


# EOF
