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
# "@(#) $Id: ngamsGenericOfflinePlugIn.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  28/02/2007  Created.
#
"""
Module that contains a generic Offline Plug-In for NGAS.
"""

import os

import cPickle as pickle
import ngamsCmd_ASYNCLISTRETRIEVE
from ngamsGenericPlugInLib import notifyRegistrationService
from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import info, alert, TRACE
from ngamsServer import ngamsSubscriptionThread


def _saveSubscriptionInfoToDisk(srvObj):
    """
    Before ngas server is shutdown, write files from to-be-delivered-list to disks
    otherwise, this information will be lost when server is restarted again
    """
    # get the files currently being transferred but not yet completed
    # save them in the subscriptionInfo so that they can be resumed when server starts up
    li = []
    idx_fileId = ngamsSubscriptionThread.FILE_ID
    idx_filever = ngamsSubscriptionThread.FILE_VER
    for k, fileInfo in srvObj._subscrDeliveryFileDic.items():
        if (fileInfo == None):
            continue
        fileInfo = ngamsSubscriptionThread._convertFileInfo(fileInfo)
        li += [(fileInfo[idx_fileId], fileInfo[idx_filever])]
        info(3, '%s is added to the subscriptionInfoList' % fileInfo[idx_fileId])

    # also add to files that have not yet been in the queue
    srvObj._subscriptionSem.acquire()
    srvObj._subscriptionFileList += li
    if (len(srvObj._subscriptionFileList) == 0):
        srvObj._subscriptionSem.release()
        info(3, "**** subscription list is empty!!")
        return

    info(3, "Saving subscription info to disks ...")
    ngas_root_dir =  srvObj.getCfg().getRootDirectory()
    myDir = ngas_root_dir + "/SubscriptionInfo"
    saveFile = myDir + "/SubscriptionInfoObj"

    try:
        if (os.path.exists(saveFile)):
            cmd = "rm " + saveFile
            ngamsPlugInApi.execCmd(cmd, -1)
        if (not os.path.exists(myDir)):
            os.makedirs(myDir)
        output = open(saveFile, 'wb')
        pickle.dump(srvObj._subscriptionFileList, output)
        output.close()
    except Exception, e:
        ex = str(e)
        alert('Fail to save subscription info to disks, Exception: %s' % ex)
    finally:
        srvObj._subscriptionSem.release()

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

    #cmdMod = "ngamsCmd_ngamsCmd_ASYNCLISTRETRIEVE"
    #srvObj.getDynCmdDic()[cmdMod] = 1

    #host = getHostName()
    #port = srvObj.getCfg().getPortNo()

    #startAsyncRetrListUrl = "http://" + host + ":" + str(port) + "/ASYNCLISTRETRIEVE?ngassystem=start"
    #info(3, "Sending system stopping request ")
    #myRes = ngamsCmd_ASYNCLISTRETRIEVE.stopAsyncQService(srvObj, reqPropsObj)
    #strRes = urllib.urlopen(startAsyncRetrListUrl).read()
    #myRes = pickle.loads(strRes)
    #info(3, "Stopping async retrieve list result - %s" % myRes)

    #_saveSubscriptionInfoToDisk(srvObj)