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
# "@(#) $Id: ngamsGenericOnlinePlugIn.py,v 1.5 2008/08/19 20:46:04 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  26/02/2007  Created.
# cwu       22/06/2012  Added registration service notification

"""
Contains the Generic Online Plug-In.

The main features of this plug-in are:

- The mounting of the Data Volumes is left to the system. Usually these will be
mounted at boot time.

- The Data Volumes should be mounted (or located) under the directories:

  /<NGAS Root Dir>/<Volume Dir> or /<NGAS Root Dir>

- A Data Volume is accepted if it contains a file named '.ngas_volume_id'.

- This file should be created by the tool 'ngasPrepareVolume'.

- Other directories found in the specified directory, not containing the
'.ngas_volume_id' file, are ignored.

"""

import os, glob
import httplib, urllib #this is only used for notifying the registration service

from   ngams import *
import ngamsPlugInApi, ngamsPhysDiskInfo
import ngamsServer
from ngamsGenericPlugInLib import *


def ngamsMWAOnlinePlugIn(srvObj,
                             reqPropsObj = None):
    """
    Scan the specified Root Directory/Volume Directory for NGAS Volumes
    and register these as operational volumes.

    srvObj:        Reference to instance of the NG/AMS Server
                   class (ngamsServer).

    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).

    Returns:       Disk info dictionary (dictionary).
    """
    T = TRACE()

    rootDir = srvObj.getCfg().getRootDirectory()
    volumeDir = srvObj.getCfg().getVolumeDirectory()

    # Build the root directory for the NGAS Volumes and make a glob to
    # get directories under this.
    ngasVolDir = os.path.normpath(rootDir + os.sep + volumeDir)
    dirList = glob.glob(ngasVolDir + os.sep + "*")
    diskInfoDic = {}
    for dir in dirList:
        # Check if a '.ngas_volume_id' is found under the directory.
        volInfoFile = os.path.\
                      normpath(dir + os.sep + NGAS_VOL_INFO_FILE)
        if (os.path.exists(volInfoFile)):
            # - It exists, load it
            volInfoDic = loadVolInfoFile(volInfoFile)
            # Create an ngamsPhysDiskInfo object with the information about
            # the slot.
            diskId     = volInfoDic[NGAS_VOL_INFO_ID]
            devName    = NGAS_VOL_INFO_IGNORE
            portNo     = -1
            slotId     = dir.split(os.sep)[-1]
            mtPt       = dir
            status     = "OK"
            capGb      = -1
            model      = NGAS_VOL_INFO_IGNORE
            serialNo   = NGAS_VOL_INFO_IGNORE
            if (volInfoDic.has_key(NGAS_VOL_INFO_TYPE)):
                diskType = volInfoDic[NGAS_VOL_INFO_TYPE]
            else:
                diskType = NGAS_VOL_INFO_IGNORE
            if (volInfoDic.has_key(NGAS_VOL_INFO_MANUFACT)):
                manufact = volInfoDic[NGAS_VOL_INFO_MANUFACT]
            else:  
                manufact = NGAS_VOL_INFO_IGNORE
            msg = "Registering volume with parameters: Disk ID: %s, " +\
                  "Device: %s, Port No: %s, Slot ID: %s, Mount Point: %s, "+\
                  "Status: %s, Capacity (GB): %s, Model: %s, Serial#: %s, " +\
                  "Type: %s, Manufacturer: %s"
            info(3, msg % (diskId, devName, str(portNo), slotId, mtPt,
                           status, str(capGb), model, serialNo, diskType,
                           manufact))
            diskInfoDic[str(slotId)] = ngamsPhysDiskInfo.\
                                       ngamsPhysDiskInfo().\
                                       setPortNo(portNo).\
                                       setSlotId(slotId).\
                                       setMountPoint(mtPt).\
                                       setStatus(status).\
                                       setCapacityGb(capGb).\
                                       setModel(model).\
                                       setSerialNo(serialNo).\
                                       setType(diskType).\
                                       setManufacturer(manufact).\
                                       setDiskId(diskId).\
                                       setDeviceName(devName)

    notifyRegistrationService(srvObj)
    return diskInfoDic

def notifyRegistrationService(srvObj, svrStatus = 'online'):
    """
    to notify the ngas registration service that I am online now
    
    svrStatus = online|offline
    """
    
    parDicOnline = ngamsPlugInApi.\
                    parseRawPlugInPars(srvObj.getCfg().getOnlinePlugInPars())
    if (parDicOnline.has_key("regsvr_host")):
        
        if (svrStatus == "online"):
            errTag = "NGAMS_ER_ONLINE_PLUGIN"
        else: 
            errTag = "NGAMS_ER_OFFLINE_PLUGIN"
        
        regsvr_host = parDicOnline["regsvr_host"]
        regsvr_port = parDicOnline["regsvr_port"]
        regsvr_path = parDicOnline["regsvr_path"]
        host_name = getHostName()
        host_port = srvObj.getCfg().getPortNo()
        
        params = urllib.urlencode({'ngas_host': host_name, 'ngas_port': host_port, 'status': svrStatus}) 
        headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
        conn = httplib.HTTPConnection(regsvr_host+':'+regsvr_port)
        try:
            conn.request("POST", regsvr_path, params, headers)
            response = conn.getresponse() 
            #print response.status, response.reason
            if (response.status != 200):
                errMsg = "Problem notifying registration service! Error " + response.reason                               
                errMsg = genLog(errTag, [errMsg])    
                error(errMsg)
                #raise Exception, errMsg
            else:
                info(3, "Successfully notified registration service: %s" % svrStatus) 
                data = response.read() #for debug
                print data #for debug
        except Exception, e:
            errMsg = "Cannot connect to the registration service " +\
                         ": %s" % str(e)
            errMsg = genLog(errTag, [errMsg])             
            error(errMsg)             
        finally:
            if (conn is not None):
                conn.close()
                
        
    return


if __name__ == '__main__':
    """
    Main function.
    """
    import sys
    import ngamsConfig, ngamsDb

    setLogCond(0, "", 0, "", 1)
    
    if (len(sys.argv) != 2):
        print "\nCorrect usage is:\n"
        print "% python ngamsMWAOnlinePlugIn <NGAMS Cfg.>\n"
        sys.exit(0)

    srvObj = ngamsServer.ngamsServer()  
    ngamsCfgObj = ngamsConfig.ngamsConfig().load(sys.argv[1])
    dbConObj = ngamsDb.ngamsDb(ngamsCfgObj.getDbServer(),
                               ngamsCfgObj.getDbName(),
                               ngamsCfgObj.getDbUser(),
                               ngamsCfgObj.getDbPassword(),
                               interface=ngamsCfgObj.getDbInterface())
    srvObj.setCfg(ngamsCfgObj).setDb(dbConObj)
    diskDic = ngamsMWAOnlinePlugIn(srvObj)
    slotIds = []
    for slotId in diskDic.keys(): slotIds.append(slotId)
    slotIds.sort()
    for slotId in slotIds:
        print "=Slot ID: %s:\n%s" % (slotId, diskDic[slotId].dumpBuf())

# EOF
