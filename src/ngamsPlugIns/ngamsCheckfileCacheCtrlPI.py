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
# "@(#) $Id: ngamsCheckfileCacheCtrlPI.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  29/07/2008  Created
#

"""
This is a Cache Control Plug-In, which can be used to check if the file
analyzed is available in one or more other archives.

This is done by sending a CHECKFILE Command to the specified contact nodes.
If the file being checked is found in the specified number of valid copies,
the plug-in return True to signal to the Cache Control Thread that the file
can be removed from the cache.

The plug-in can do the check at several levels:

  1. Check if the file is available in one cluster, specified by a server list.

  2. Check if the file is available in a set of server list.

  3. Check if the file is available according to 1. or 3. in several instances.


For 1. the plug-in parameters should look like this:

  nodes=[[['host1:port1'; 'host2:port2'; ...]]]

For 2. the plug-in parameters should look like this:

  nodes=[[['host1_1:port1_1'; 'host1_2:port1_2'; ...];
          ['host2_1:port2_1'; 'host2_2:port2_2'; ...]]]

For 3. the plug-in parameters should look like this:

  nodes=[[['host1_1_1:port1_1_1'; 'host1_1_2:port1_1_2'; ...];
          ['host1_2_1:port1_2_1'; 'host1_2_2:port1_2_2'; ...]];
         [['host2_1_1:port2_1_1'; 'host2_1_2:port2_1_2'; ...];
          ['host2_2_1:port2_2_1'; 'host2_2_2:port2_2_2'; ...]]]
          
In this example two sets of server list are specified:

Set#1: [['host1_1_1:port1_1_1'; 'host1_1_2:port1_1_2'; ...];
        ['host1_2_1:port1_2_1'; 'host1_2_2:port1_2_2'; ...]]

- and:

Set#2: [['host2_1_1:port2_1_1'; 'host2_1_2:port2_1_2'; ...];
        ['host2_2_1:port2_2_1'; 'host2_2_2:port2_2_2'; ...]]

If the plug-in has a successful result contacting one node in set Set#1 _and_
in Set#2, the file is signalled for removal from the cache.
"""

import os, string, random, time
import PccUtTime
from   ngams import *
import ngamsLib, ngamsStatus, ngamsPlugInApi, ngamsDiskUtils, ngamsDiskInfo


# Compiled list with the nodes to contact for check for availability of
# the given file.
CheckfileCacheCtrlPI_initialized    = False
CheckfileCacheCtrlPI_sets           = None
CheckfileCacheCtrlPI_checkingPeriod = None

NGAMS_INFO_FILE_OK = "NGAMS_INFO_FILE_OK"
NGAMS_ER_FILE_NOK  = "NGAMS_ER_FILE_NOK"
LAST_CHECK         = "LAST_CHECK"


def _unpackCheckPlugInPars(srvObj):
    """
    Unpack the Cache Control Plug-In Parameters and set some internal
    variables for easy handling.

    srvObj:       Reference to NG/AMS Server Object (ngamsServer).

    Returns:      Void.
    """
    global CheckfileCacheCtrlPI_initialized
    if (CheckfileCacheCtrlPI_initialized): return

    # Unpack the parameters.
    plugInPars = srvObj.getCfg().getVal("Caching[1].CacheControlPlugInPars")
    parList = plugInPars.split(",")
    for par in parList:
        try:
            par, val = par.split("=")
            if (par.lower() == "nodes"):
                global CheckfileCacheCtrlPI_sets
                CheckfileCacheCtrlPI_sets = eval(val.replace(";", ","))
            elif (par.lower() == "checking_period"):
                global CheckfileCacheCtrlPI_checkingPeriod
                CheckfileCacheCtrlPI_checkingPeriod = float(val)
            else:
                msg = "Illegal plug-in parameter specified: %s"
                raise Exception, msg % str(par)
        except Exception, e:
            msg = "Error unpacking Cache Control Plug-In Parameters: %s. " +\
                  "Around parameter: %s. Error: %s"
            raise Exception, msg % (str(plugInPars), str(par), str(e))
    CheckfileCacheCtrlPI_initialized = True
    

def _sendCheckFileCmd(node,
                      fileId,
                      fileVersion):
    """
    Send a CHECKFILE Command to the specified node. One of the following is
    returned:

      1. File OK:     NGAMS_INFO_FILE_OK.
      2. File Not OK: NGAMS_ER_FILE_NOK
      3. Error:       FAILURE.

    node:        Node to be contacted (node:port) (string).
    
    fileId:      ID of file to check (string).
    
    fileVersion: Version of file to check (integer).

    Returns:     See above (NGAMS_INFO_FILE_OK | NGAMS_ER_FILE_NOK | FAILURE).
    """
    T = TRACE(5)

    cmdPars = [["file_id", fileId],
               ["file_version", fileVersion]]
    data = None
    try:
        host, port = node.split(":")
        info(4, "Sending CHECKFILE Command for file: %s/%s to node: %s:%s" %\
             (fileId, str(fileVersion), host, str(port)))
        code, msg, hdrs, data = ngamsLib.httpGet(host, port,
                                                 NGAMS_CHECKFILE_CMD,
                                                 pars = cmdPars)
    except Exception, e:
        err = "Error contacting node: %s . Error: %s" % (str(node), str(e))
        info(4, "NOTICE: %s" % err)
                                                         
    if (data):
        tmpStatObj = ngamsStatus.ngamsStatus().unpackXmlDoc(data)
        if (tmpStatObj.getMessage().find(NGAMS_INFO_FILE_OK) != -1):
            return NGAMS_INFO_FILE_OK
        else:
            # Assume: NGAMS_ER_FILE_NOK.
            return NGAMS_ER_FILE_NOK
    else:
        return NGAMS_FAILURE


def _fileAvailableInSet(set,
                        cacheEntryObj):
    """
    Check if one valid copy is available in one of the sets.

    set:            List with sub-lists with contact nodes (list/list).

    cacheEntryObj:  Cache Entry Object containing the information for the
                    cached file (ngamsCacheEntry).

    Returns:        Returns True if the file is found in one valid copy in
                    one of the name space (server lists) specified (boolean).
    """
    T = TRACE(5)

    # Shuffle the set list to obtain a simple load balancing.
    random.shuffle(set)
    foundValidCopy = False
    for srvList in set:
        if (foundValidCopy): break
        # Shuffle the list to obtain a simple load balancing.
        random.shuffle(srvList)
        for node in srvList:
            res = _sendCheckFileCmd(node, cacheEntryObj.getFileId(),
                                    cacheEntryObj.getFileVersion() )
            if (res == NGAMS_INFO_FILE_OK):
                # File is OK, stop checking this set.
                foundValidCopy = True
                break
            elif (res == NGAMS_ER_FILE_NOK):
                # Proceed to check the next server list.
                break
            else:
                # FAILURE: A problem occurred contacting the given node.
                #          Try the next node in the list.
                continue

    return foundValidCopy

    
# Plug-in function.
def ngamsCheckfileCacheCtrlPI(srvObj,
                              cacheEntryObj):
    """
    Cache Control Plug-In, which checks for the availability of a file in
    one or more NGAS Archives. If a valid copy of the file is found in the
    specified locations, the file is signalled for deletion.

    srvObj:         Reference to NG/AMS Server Object (ngamsServer).

    cacheEntryObj:  Cache Entry Object containing the information for the
                    cached file (ngamsCacheEntry).

    Returns:        Returns True if the file can be deleted from the cache,
                    otherwise False (boolean). 
    """
    T = TRACE()

    try:
        _unpackCheckPlugInPars(srvObj)

        # Should the check be done?
        global CheckfileCacheCtrlPI_checkingPeriod
        if (CheckfileCacheCtrlPI_checkingPeriod):
            lastCheck = cacheEntryObj.getPar(LAST_CHECK)
            if (lastCheck):
                # Check if it is at least checking-period seconds since the
                # last check was done. If this is the case, the check is not
                # yet due.
                timeNow = time.time()
                if ((timeNow - lastCheck) <
                    CheckfileCacheCtrlPI_checkingPeriod):
                    return False
            else:
                # First time the plug-in is invoked on this file. Just set the
                # time to the current time, then the file will be checked
                # approx. after checking-period seconds.
                cacheEntryObj.addPar(LAST_CHECK, time.time())
                return False

        # Loop over the given lists.
        global CheckfileCacheCtrlPI_sets
        okCount = 0
        for set in CheckfileCacheCtrlPI_sets:
            if (_fileAvailableInSet(set, cacheEntryObj)):
                okCount += 1
            else:
                # No need to check further ...
                break

        # Update the checking time.
        cacheEntryObj.addPar(LAST_CHECK, time.time())

        # Check the result.
        if (okCount == len(CheckfileCacheCtrlPI_sets)):
            # File was available in at least one valid copy in all systems.
            return True
        else:
            # File was not available in at least one valid copy in all sets.
            return False

    except Exception, e:
        msg = "Error ocurred executing Cache Control Plug-In. Error: %s" %\
              str(e)
        error(msg)
        raise Exception, msg


# EOF
