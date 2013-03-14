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
# "@(#) $Id: ngamsUnsubscribeCmd.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  05/11/2002  Created
#

"""
This module contains functions used in connection with the
UNSUBSCRIBE Command.
"""

import pcc, PccUtTime
from ngams import *
import ngamsLib, ngamsCacheControlThread


def delSubscriber(srvObj,
                  subscrId):
    """
    Remove a Susbcriber from the internal list + from the DB.

    srvObj:      Reference to NG/AMS Server object (ngamsServer).

    subscrId:    Subscriber ID (string).

    Returns:     Void.
    """
    T = TRACE()
    
    err = 0
    errMsg = ''
    try:
        srvObj.getDb().deleteSubscriber(subscrId)
    except Exception, e:
        estr = " Error deleting Subscriber information from the DB. " +\
                "Subscriber ID: " + subscrId + ". Exception: " + str(e)
        warning(estr)
        err += 1
        errMsg += estr
    # remove all entries associated with this subscriber from in-memory dictionaries
    try:
        del srvObj.getSubscriberDic()[subscrId]
        del srvObj._subscrScheduledStatus[subscrId]
        del srvObj._subscrQueueDic[subscrId]
        srvObj._subscrSuspendDic[subscrId].set() # resume all suspended deliveryThreads (if any) so they can know the subscriber is removed
        del srvObj._subscrDeliveryThreadDic[subscrId] # this does not kill those deliveryThreads, but only the list container
    except Exception, e:
        estr = " Error deleting Subscriber information kept internally. " +\
                "Subscriber ID: " + subscrId + ". Exception: " + str(e)
        warning(estr)
        err += 1
        errMsg += estr
    
    # reduce the file reference count by 1 for all files that are back logged for this subscriber
    if (srvObj.getCachingActive()):
        errOld = err
        filelist = srvObj.getDb().getSubscrBackLogBySubscrId(subscrId)
        fileDeliveryCountDic = srvObj._subscrFileCountDic
        fileDeliveryCountDic_Sem = srvObj._subscrFileCountDic_Sem
        for fi in filelist:
            fileId = fi[0]
            fileVersion = fi[1]
            fkey = fileId + "/" + str(fileVersion) 
            fileDeliveryCountDic_Sem.acquire()
            try:
                if (fileDeliveryCountDic.has_key(fkey)):
                    fileDeliveryCountDic[fkey]  -= 1
                    if (fileDeliveryCountDic[fkey] == 0):
                        del fileDeliveryCountDic[fkey]
                        # mark deletion
                        diskId = fi[2]
                        sqlFileInfo = (diskId, fileId, fileVersion)
                        ngamsCacheControlThread.scheduleFileForDeletion(srvObj, sqlFileInfo)
            except Exception, e:
                warning(" Error reducing the reference count by 1 for file: %s" % fileId)
                err += 1
            finally:
                fileDeliveryCountDic_Sem.release()                        
        if ((err - errOld) > 0):
            errMsg += ' Error reducing file reference count for some files, check NGAS log to find out which files'
    
    # remove all backlog entries associated with this subscriber
    try:
        srvObj.getDb().delSubscrBackLogEntries(subscrId, getHostId(), srvObj.getCfg().getPortNo())
    except Exception, e:
        estr = " Error deleting entries from the subscr_back_log table for subscriber %s" % subscrId
        warning(estr)
        err += 1
        errMsg += estr
    if (not err):               
        info(2,"Subscriber with ID: " + subscrId +\
             " successfully unsubscribed")
    return [err, errMsg]


def handleCmdUnsubscribe(srvObj,
                         reqPropsObj,
                         httpRef):
    """
    Handle UNSUBSCRIBE command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).
        
    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    T = TRACE()
    
    # added by chen.wu@icrar.org
    if (reqPropsObj.hasHttpPar("subscr_id")):
        id = reqPropsObj.getHttpPar("subscr_id")
        err, errStr = delSubscriber(srvObj, id)
        ###########
    else:
        if (reqPropsObj.hasHttpPar("url")):
            url = reqPropsObj.getHttpPar("url")
        else:
            errMsg = genLog("NGAMS_ER_CMD_SYNTAX",
                            [NGAMS_SUBSCRIBE_CMD, "Missing parameter: url"])
            raise Exception, errMsg
        err, errStr = delSubscriber(srvObj, ngamsLib.getSubscriberId(url))

    if (not err):
        srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_SUCCESS,
                 "Successfully handled UNSUBSCRIBE command")
    else:
        srvObj.reply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE, #let HTTP returns OK so that curl can continue printing XML code
                 'UNSUBSCRIBE command failed: ' + errStr)


# EOF
