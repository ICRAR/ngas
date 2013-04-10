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
import urllib
import cPickle as pickle

from   ngams import *
import ngamsPlugInApi, ngamsPhysDiskInfo
import ngamsServer
from ngamsGenericPlugInLib import *

import ngamsCmd_ASYNCLISTRETRIEVE


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
    
    cmdMod = "ngamsCmd_ASYNCLISTRETRIEVE"
    srvObj.getDynCmdDic()[cmdMod] = 1
    
    #host = getHostName()
    #port = srvObj.getCfg().getPortNo()
    
    #startAsyncRetrListUrl = "http://" + host + ":" + str(port) + "/ASYNCLISTRETRIEVE?ngassystem=start"
    info(3, "Sending system starting request ")
    myRes = ngamsCmd_ASYNCLISTRETRIEVE.startAsyncQService(srvObj, reqPropsObj)
    #strRes = urllib.urlopen(startAsyncRetrListUrl).read()
    #myRes = pickle.loads(strRes)
    info(3, "Starting async retrieve list result - %s" % myRes)
    
    _restoreSubscriptionInfoFromDisk(srvObj)
    
    return diskInfoDic

def _restoreSubscriptionInfoFromDisk(srvObj):
    ngas_root_dir =  srvObj.getCfg().getRootDirectory()
    myDir = ngas_root_dir + "/SubscriptionInfo" 
    if (not os.path.exists(myDir)):
        return
    saveFile = myDir + "/SubscriptionInfoObj"
    if (not os.path.exists(saveFile)):
        return
    
    info(3, 'Restoring subscription info from disks ...')
    saveObj = None
    try:
        pkl_file = open(saveFile, 'rb')
        saveObj = pickle.load(pkl_file)   
        pkl_file.close() 
    except Exception, e:
        ex = str(e)
        alert('Fail to restore subscription info from disks, Exception: %s' % ex)
    
    if (saveObj == None or len(saveObj) == 0):
        return
    
    info(3, 'Appending subscription info to info list kept internally ...')
    try:  
        srvObj._subscriptionSem.acquire()
        srvObj._subscriptionFileList += saveObj  
        cmd = "rm " + saveFile
        ngamsPlugInApi.execCmd(cmd, -1)    
    except Exception, e:
        alert('Fail to append filelist to subscription info list, Exception: %s' % str(e))
    finally:
        srvObj._subscriptionSem.release()

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
