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
# "@(#) $Id: ngamsCmdHandling.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/01/2002  Created
#
"""
Contains various functions for handling commands.
"""

import logging
import sys

import ngamsArchiveCmd, ngamsCacheDelCmd, ngamsCheckFileCmd, ngamsDiscardCmd
import ngamsConfigCmd, ngamsCloneCmd
import ngamsExitCmd, ngamsHelpCmd, ngamsInitCmd, ngamsLabelCmd, ngamsOfflineCmd
from ngamsLib.ngamsCore import genLog, TRACE, \
    NGAMS_RETRIEVE_CMD, NGAMS_ARCHIVE_CMD, NGAMS_CACHEDEL_CMD, \
    NGAMS_CHECKFILE_CMD, NGAMS_CLONE_CMD, NGAMS_CONFIG_CMD, NGAMS_DISCARD_CMD, \
    NGAMS_EXIT_CMD, NGAMS_HELP_CMD, NGAMS_INIT_CMD, NGAMS_LABEL_CMD, \
    NGAMS_OFFLINE_CMD, NGAMS_ONLINE_CMD, NGAMS_REARCHIVE_CMD, NGAMS_REGISTER_CMD, \
    NGAMS_REMDISK_CMD, NGAMS_REMFILE_CMD, NGAMS_STATUS_CMD, NGAMS_SUBSCRIBE_CMD, \
    NGAMS_UNSUBSCRIBE_CMD
import ngamsOnlineCmd, ngamsRearchiveCmd, ngamsRegisterCmd, ngamsRemDiskCmd
import ngamsRemFileCmd, ngamsRetrieveCmd, ngamsStatusCmd, ngamsSubscribeCmd
import ngamsUnsubscribeCmd


logger = logging.getLogger(__name__)

def cmdHandler(srvObj,
               reqPropsObj,
               httpRef):
    """
    Handle a command.

    srvObj:        Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:   Request Property object to keep track of
                   actions done during the request handling
                   (ngamsReqProps).

    httpRef:       Reference to the HTTP request handler
                   object (ngamsHttpRequestHandler).

    Returns:       Void.
    """
    T = TRACE()

    # Interpret the command + parameters.
    cmd = reqPropsObj.getCmd()
    logger.info("Received command: %s", cmd)
    if (cmd == "ngamsInternal.dtd"):
        # Special handling.
        reqPropsObj.setCmd(NGAMS_RETRIEVE_CMD).addHttpPar("internal", cmd)
        ngamsRetrieveCmd.handleCmdRetrieve(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_ARCHIVE_CMD):
        ngamsArchiveCmd.handleCmdArchive(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_CACHEDEL_CMD):
        ngamsCacheDelCmd.handleCmdCacheDel(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_CHECKFILE_CMD):
        ngamsCheckFileCmd.handleCmdCheckFile(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_CLONE_CMD):
        ngamsCloneCmd.handleCmdClone(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_CONFIG_CMD):
        ngamsConfigCmd.handleCmdConfig(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_DISCARD_CMD):
        ngamsDiscardCmd.handleCmdDiscard(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_EXIT_CMD):
        ngamsExitCmd.handleCmdExit(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_HELP_CMD):
        ngamsHelpCmd.handleCmdHelp(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_INIT_CMD):
        ngamsInitCmd.handleCmdInit(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_LABEL_CMD):
        ngamsLabelCmd.handleCmdLabel(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_OFFLINE_CMD):
        ngamsOfflineCmd.handleCmdOffline(srvObj,reqPropsObj, httpRef)
    elif (cmd == NGAMS_ONLINE_CMD):
        ngamsOnlineCmd.handleCmdOnline(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_REARCHIVE_CMD):
        ngamsRearchiveCmd.handleCmdRearchive(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_REGISTER_CMD):
        ngamsRegisterCmd.handleCmdRegister(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_REMDISK_CMD):
        ngamsRemDiskCmd.handleCmdRemDisk(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_REMFILE_CMD):
        ngamsRemFileCmd.handleCmdRemFile(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_RETRIEVE_CMD):
        ngamsRetrieveCmd.handleCmdRetrieve(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_STATUS_CMD):
        ngamsStatusCmd.handleCmdStatus(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_SUBSCRIBE_CMD):
        ngamsSubscribeCmd.handleCmdSubscribe(srvObj, reqPropsObj, httpRef)
    elif (cmd == NGAMS_UNSUBSCRIBE_CMD):
        ngamsUnsubscribeCmd.handleCmdUnsubscribe(srvObj, reqPropsObj, httpRef)
    else:
        try:
            if cmd == 'robots.txt':
                cmd = 'robots'
            if cmd == 'favicon.ico':
                cmd = 'favicon'
            cmdMod = "ngamsPlugIns.ngamsCmd_%s" % cmd
            # Reload the module if requested.
            reloadMod = 0
            if (reqPropsObj.hasHttpPar("reload")):
                if (int(reqPropsObj.getHttpPar("reload")) == 1):
                    reloadMod = 1
            if not sys.modules.has_key(cmdMod):
                logger.debug("Importing dynamic command module: %s", cmdMod)
                mod = __import__(cmdMod, fromlist=[__name__])
            elif reloadMod == 1:
                logger.debug("Re-loading dynamic command module: %s", cmdMod)
                mod = reload(sys.modules[cmdMod])
            else:
                mod = __import__(cmdMod, fromlist=[__name__]) # just make sure that mod is initialized
                logger.debug("Using loaded dynamic command module: %s", cmdMod)

            srvObj.getDynCmdDic()[cmdMod] = 1
        except Exception:
            errMsg = genLog("NGAMS_ER_ILL_CMD", [cmd])
            raise Exception(errMsg)
        mod.handleCmd(srvObj, reqPropsObj, httpRef)

# EOF