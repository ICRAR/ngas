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
# "@(#) $Id: ngamsCmdHandling.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/01/2002  Created
#

"""
Contains various functions for handling commands.
"""

from   ngams import *
import ngamsLib
import ngamsArchiveCmd, ngamsCacheDelCmd, ngamsCheckFileCmd, ngamsDiscardCmd
import ngamsConfigCmd, ngamsCloneCmd
import ngamsExitCmd, ngamsHelpCmd, ngamsInitCmd, ngamsLabelCmd, ngamsOfflineCmd
import ngamsOnlineCmd, ngamsRearchiveCmd, ngamsRegisterCmd, ngamsRemDiskCmd
import ngamsRemFileCmd, ngamsRetrieveCmd, ngamsStatusCmd, ngamsSubscribeCmd
import ngamsUnsubscribeCmd


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
    info(1,"Received command: " + cmd)
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
            cmdMod = "ngamsCmd_%s" % cmd
            # Reload the module if requested.
            reloadMod = 0
            if (reqPropsObj.hasHttpPar("reload")):
                if (int(reqPropsObj.getHttpPar("reload")) == 1):
                    reloadMod = 1
            info(2,"Loading dynamic command module: %s" % cmdMod)
            exec "import %s" % cmdMod
            srvObj.getDynCmdDic()[cmdMod] = 1
            if (reloadMod):
                info(2,"Reloading dynamic command module: %s" % cmdMod)
                reloadCmd = "reload(%s)" % cmdMod
                stat = eval(reloadCmd)
                info(4,"Status of reloading command module: %s" % str(stat))
        except Exception, e:
            warning("Error encountered loading dynamic command module: %s" %\
                    str(e))
            errMsg = genLog("NGAMS_ER_ILL_CMD", [cmd])
            error(errMsg)
            raise Exception, errMsg
        eval("%s.handleCmd(srvObj, reqPropsObj, httpRef)" % cmdMod)
        

# EOF
