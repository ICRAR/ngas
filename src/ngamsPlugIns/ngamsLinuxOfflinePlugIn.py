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
# "@(#) $Id: ngamsLinuxOfflinePlugIn.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  10/05/2001  Created.
#

"""
Module that contains a System Offline Plug-In used by the ESO NGAS
installations.
"""

from   ngams import *
import ngamsPlugInApi
import ngamsLinuxSystemPlugInApi, ngamsEscaladeUtils


def ngamsLinuxOfflinePlugIn(srvObj,
                            reqPropsObj = None):
    """
    Function unmounts all NGAMS disks and removes the kernel module for
    the IDE controller card.

    srvObj:        Reference to instance of the NG/AMS Server class
                   (ngamsServer).

    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).

    Returns:       Void.
    """
    rootMtPt = srvObj.getCfg().getRootDirectory()    
    parDicOnline = ngamsPlugInApi.\
                   parseRawPlugInPars(srvObj.getCfg().getOnlinePlugInPars())

    # Old format = unfortunately some Disk IDs of WDC/Maxtor were
    # generated wrongly due to a mistake by IBM, which lead to a wrong
    # implementation of the generation of the Disk ID.
    if (not parDicOnline.has_key("old_format")):
        raise Exception, "Missing Online Plug-In Parameter: old_format=0|1"
    else:
        oldFormat = int(parDicOnline["old_format"])

    # The controllers Plug-In Parameter, specifies the number of controller
    # in the system.
    if (not parDicOnline.has_key("controllers")):
        controllers = None
    else:
        controllers = parDicOnline["controllers"]

    # Select between 3ware WEB Interface and 3ware Command Line Tool.
    if (parDicOnline["uri"].find("http") != -1):
        diskDic = ngamsEscaladeUtils.\
                  parseHtmlInfo(parDicOnline["uri"], rootMtPt)
    else:
        diskDic = ngamsEscaladeUtils.parseCmdLineInfo(rootMtPt, controllers,
                                                      oldFormat, rescan=0)
        
    parDicOffline = ngamsPlugInApi.\
                    parseRawPlugInPars(srvObj.getCfg().getOfflinePlugInPars())
    
    # This is only unmounting the NGAMS disks and may lead to problems
    # if someone mounts other disks off-line.
    if (parDicOffline.has_key("unmount")):
        unmount = int(parDicOffline["unmount"])
    else:
        unmount = 1
    if (unmount):
        try:
            ngamsLinuxSystemPlugInApi.ngamsUmount(diskDic,
                                                  srvObj.getCfg().getSlotIds())
            if (parDic.has_key("module")):
                stat = ngamsLinuxSystemPlugInApi.rmMod(parDicOnline["module"])
            else:
                stat = 0
            if (stat):
                errMsg = "Problem executing ngamsLinuxOfflinePlugIn! " +\
                         "The system is in not in a safe state!"
                errMsg = genLog("NGAMS_ER_OFFLINE_PLUGIN", [errMsg])
                error(errMsg)
                raise Exception, errMsg
            if (parDic.has_key("module")):
                msg = "Kernel module " + parDicOnline["module"] + " unloaded"
                info(1,msg)
        except Exception, e:
            pass
        
        # Fallback umount.
        ngamsLinuxSystemPlugInApi.umount(rootMtPt)


if __name__ == '__main__':
    """
    Main function.
    """
    import sys
    import ngamsConfig, ngamsDb

    setLogCond(0, "", 0, "", 1)

    if (len(sys.argv) != 2):
        print "\nCorrect usage is:\n"
        print "% python ngamsLinuxOfflinePlugIn <NGAMS cfg>\n"
        sys.exit(0)    
    
    ngamsCfgObj = ngamsConfig.ngamsConfig()
    ngamsCfgObj.load(sys.argv[1])
    dbConObj = ngamsDb.ngamsDb(ngamsCfgObj.getDbServer(),
                               ngamsCfgObj.getDbName(),
                               ngamsCfgObj.getDbUser(),
                               ngamsCfgObj.getDbPassword())
    dbConObj.query("use " + ngamsCfgObj.getDbName())
    ngamsLinuxOfflinePlugIn(dbConObj, ngamsCfgObj)


# EOF
