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
# "@(#) $Id: ngamsLabelCmd.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  13/05/2003  Created
#

"""
Functions to handle the LABEL Command.
"""

from ngams import *


# Semaphore to avoid that the server tries to access with several threads
# simulating the label printer (maybe only needed for the Brother printer).
_labelPrinterSem = threading.Semaphore(1)


def printLabel(srvObj,
               diskId,
               hostId,
               slotId,
               reqPropsObj = None):
    """
    Print a label by invoking the label printer.

    srvObj:         Reference to instance of NG/AMS Server class (ngamsServer).

    diskId:         ID of disk to print label for (string).

    hostId:         Host ID of host hosting the disk (string).

    slotId:         Slot ID of slot in which the disk is inserted (string).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
                    
    Returns:        Void.
    """
    T = TRACE()
    
    label = srvObj.getDb().getLogicalNameFromDiskId(diskId)
    if (not label):
        errMsg = "Empty Logical Name returned for Disk ID: %s" % diskId
        raise Exception, errMsg
    info(2,"Generating label for disk with ID: " + str(diskId) +\
         " - Label text (Logical Name): " + str(label) + " ...")
    plugIn = srvObj.getCfg().getLabelPrinterPlugIn()
    prStr = label + "   " + hostId + ":" + slotId 
    info(3,"Invoking Label Printer Plug-In: " + plugIn +\
         "(srvObj, " + prStr + ")")
    exec "import " +  plugIn
    global _labelPrinterSem
    _labelPrinterSem.acquire()
    try:
        eval(plugIn + "." + plugIn + "(srvObj, prStr, reqPropsObj)")
    except Exception, e:
        _labelPrinterSem.release()
        raise e
    _labelPrinterSem.release()

    info(2,"Generated label for disk with ID: " + diskId +\
         " - Label text (Logical Name): " + label + " ...")


def handleCmdLabel(srvObj,
                   reqPropsObj,
                   httpRef):
    """
    Handle LABEL command.
        
    srvObj:         Reference to NG/AMS server class object (ngamsServer).
    
    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).
        
    Returns:        Void.
    """
    T = TRACE()
    
    srvObj.checkSetState("Command LABEL",
                         [NGAMS_OFFLINE_STATE, NGAMS_ONLINE_STATE],
                         [NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE])

    # Get the Disk ID corresponding to the Slot ID.
    diskId   = ""
    slotId   = ""
    hostId   = ""
    reLabel  = ""
    newLabel = ""
    for httpPar in reqPropsObj.getHttpParNames():
        if (httpPar == "initiator"): continue
        if (httpPar == "disk_id"):
            diskId = reqPropsObj.getHttpPar("disk_id")
        elif (httpPar == "slot_id"):
            slotId = reqPropsObj.getHttpPar("slot_id")
        elif (httpPar == "host_id"):
            hostId = reqPropsObj.getHttpPar("host_id")
        elif (httpPar == "relabel"):
            reLabel = reqPropsObj.getHttpPar("relabel")
        elif (httpPar == "new_label"):
            newLabel = reqPropsObj.getHttpPar("new_label")
        elif (httpPar == "SUBMIT"):
            # Ignore this parameter (added by the NGAS Zope interface).
            pass
        else:
            pass
        
    # Check/interpret the parameters.
    if (diskId and (slotId or hostId)):
        errMsg = genLog("NGAMS_ER_REQ_HANDLING",
                        ["It is not allowed to specify Disk ID together " +\
                         "with Host Id and/or Slot ID"])
        raise Exception, errMsg
    if ((slotId and (not hostId)) or ((not slotId) and hostId)):
        errMsg = genLog("NGAMS_ER_REQ_HANDLING",
                        ["Must refer to physical disk location with " +\
                         "Host Id and Slot ID"])
        raise Exception, errMsg

    # If Slot ID/Host ID specified, get the Disk ID from these parameters.
    if (slotId):
        diskId = srvObj.getDb().getDiskIdFromSlotId(hostId, slotId)
        if (diskId == None):
            errMsg = genLog("NGAMS_ER_REQ_HANDLING",
                            ["Could not find Disk ID corresponding to " +\
                             "host/slot: " + hostId + "/" + slotId])
            raise Exception, errMsg

    # Print the label.
    if (diskId):
        if (not slotId): slotId = "-"
        if (not hostId): hostId = "-"
        printLabel(srvObj, diskId, hostId, slotId, reqPropsObj)
    elif (reLabel and newLabel):
        if (srvObj.getDb().diskInDb(reLabel)):
            srvObj.getDb().setLogicalNameForDiskId(reLabel, newLabel)
        else:
            errMsg = genLog("NGAMS_ER_REQ_HANDLING",
                            ["Could not find Disk ID corresponding to " +\
                             "host/slot: " + hostId + "/" + slotId])
            raise Exception, errMsg
    else:
        errMsg = genLog("NGAMS_ER_MIS_PAR",
                        ["slot_id=<ID>[&host_id=<ID>]|" +\
                         "relabel=<Disk ID>&new_label=<Label>", "LABEL"])
        raise Exception, errMsg

    msg = "Successfully handled command LABEL"
    srvObj.reply(reqPropsObj.setCompletionTime(), httpRef, NGAMS_HTTP_SUCCESS,
                 NGAMS_SUCCESS, msg)
    srvObj.updateRequestDb(reqPropsObj)
    info(1,msg)


# EOF
