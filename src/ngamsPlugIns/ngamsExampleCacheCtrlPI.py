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
# "@(#) $Id: ngamsExampleCacheCtrlPI.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
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


# Parameter to hold the parameter between each invocation for efficiency.
ngamsExampleCacheCtrlPI_maxCacheTime = None


# Plug-in function.
def ngamsExampleCacheCtrlPI(srvObj,
                            cacheEntryObj):
    """
    Example Cache Control Plug-in to control the cache holding based on
    an expiration time for each cached file.

    srvObj:         Reference to NG/AMS Server Object (ngamsServer).

    cacheEntryObj:  Cache Entry Object containing the information for the
                    cached file (ngamsCacheEntry).

    Returns:        Returns True if the file can be deleted from the cache,
                    otherwise False (boolean). 
    """
    T = TRACE()

    try:
        global ngamsExampleCacheCtrlPI_maxCacheTime
        if (not ngamsExampleCacheCtrlPI_maxCacheTime):
            plugInPars = srvObj.getCfg().\
                         getVal("Caching[1].CacheControlPlugInPars")
            plugInParDic = ngamsPlugInApi.parseRawPlugInPars(plugInPars)
            try:
                _maxCacheTime = float(plugInParDic["max_cache_time"])
            except:
                msg = "Missing plug-in parameter: max_cache_time"
                raise Exception, msg

        # Check if the cache time is exceeded.
        if ((time.time() - cacheEntryObj.getCacheTime()) > _maxCacheTime):
            info(1, "Scheduling entry for deletion from cache: %s/%s/%d" %\
                 (cacheEntryObj.getDiskId(), cacheEntryObj.getFileId(),
                  cacheEntryObj.getFileVersion()))
            return True

        return False
    except Exception, e:
        msg = "Error ocurred executing Cache Control Plug-In. Error: %s" %\
              str(e)
        error(msg)
        raise Exception, msg


# EOF
