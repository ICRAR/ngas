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
# "@(#) $Id: ngamsAlmaCacheCtrlPI.py,v 1.1 2010/06/01 13:17:32 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  24/07/2008  Created
#

"""
This is an example Cache Control Plug-In, which can be used as template
when implementing this type of plug-in.

It simply deletes files from the cache after a given expiration time.
"""

import os, string
import PccUtTime
from   ngams import *
import ngamsPlugInApi, ngamsDiskUtils, ngamsDiskInfo


def ngamsAlmaCacheCtrlPI(srvObj,cacheEntryObj):
    """
    srvObj:         Reference to NG/AMS Server Object (ngamsServer).

    cacheEntryObj:  Cache Entry Object containing the information for the
                    cached file (ngamsCacheEntry).

    Returns:        Returns True if the file can be deleted from the cache,
                    otherwise False (boolean). 
    """
    T = TRACE()
    
    plugInPars = srvObj.getCfg().getVal("Caching[1].CacheControlPlugInPars")
    plugInParDic = ngamsPlugInApi.parseRawPlugInPars(plugInPars)
    dblinks = plugInParDic["db_links_list"]
    dblinks_list = dblinks.split(":")
    info(3, "db links found: %s" % dblinks_list)
    
    for dblink in dblinks_list:
        query = "select count(*) from ngas_files@" + dblink
        info(3, "Executing SQL query: %s" % query)
        srvObj.getDb().query(query, maxRetries=1, retryWait=0)

# EOF
