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

import logging

from six.moves import queue as Queue  # @UnresolvedImport

from . import ngamsSubscriptionThread
from ngamsLib.ngamsCore import TRACE, NGAMS_DELIVERY_THR, \
    genLog, NGAMS_SUBSCRIBE_CMD, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE
from ngamsLib import ngamsLib


logger = logging.getLogger(__name__)

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
    except Exception:
        estr = " Error deleting Subscriber information from the DB. " +\
                "Subscriber ID: %s"
        logger.exception(estr, str(subscrId))
        err += 1
        errMsg += estr
    # remove all entries associated with this subscriber from in-memory dictionaries

    numThreads = 0
    if (srvObj.getSubscriberDic().has_key(subscrId)):
        subscriber = srvObj.getSubscriberDic()[subscrId]
        numThreads = subscriber.getConcurrentThreads()
        del srvObj.getSubscriberDic()[subscrId]
    else:
        estr = " Cannot find Subscriber with an ID '%s' kept internally. " % subscrId
        err += 1
        errMsg += estr

    if (srvObj._subscrScheduledStatus.has_key(subscrId)):
        del srvObj._subscrScheduledStatus[subscrId]
    else:
        estr = " Cannot find scheduled status for the subscriber '%s' kept internally. " % subscrId
        err += 1
        errMsg += estr

    if (srvObj._subscrSuspendDic.has_key(subscrId)):
        srvObj._subscrSuspendDic[subscrId].set() # resume all suspended deliveryThreads (if any) so they can know the subscriber is removed
        del srvObj._subscrDeliveryThreadDic[subscrId] # this does not kill those deliveryThreads, but only the list container
    else:
        estr = " Cannot find delivery threads for the subscriber '%s' kept internally. " % subscrId
        err += 1
        errMsg += estr

    deliveryThreadRefDic = srvObj._subscrDeliveryThreadDicRef
    deliveryFileDic = srvObj._subscrDeliveryFileDic
    for tid in range(int(numThreads)):
        thrdName = NGAMS_DELIVERY_THR + subscrId + str(tid)
        if (deliveryThreadRefDic.has_key(thrdName)):
            del deliveryThreadRefDic[thrdName]
        if (deliveryFileDic.has_key(thrdName)):
            del deliveryFileDic[thrdName]

    fileDeliveryCountDic = srvObj._subscrFileCountDic
    fileDeliveryCountDic_Sem = srvObj._subscrFileCountDic_Sem

    # reduce the file reference count by 1 for all files that are in the queue to be delivered
    # and in the meantime, clear the queue before deleting it
    if (srvObj._subscrQueueDic.has_key(subscrId)):
        if (srvObj.getCachingActive()):
            errOld = err
            qu = srvObj._subscrQueueDic[subscrId]

            while (1):
                fileinfo = None
                try:
                    fileinfo = qu.get_nowait()
                except Queue.Empty:
                    break
                if (fileinfo is None):
                    break
                #fileInfo = ngamsSubscriptionThread._convertFileInfo(fileinfo)
                fileId = fileinfo[ngamsSubscriptionThread.FILE_ID]
                fileVersion = fileinfo[ngamsSubscriptionThread.FILE_VER]
                err += _reduceRefCount(fileDeliveryCountDic, fileDeliveryCountDic_Sem, fileId, fileVersion)
            if ((err - errOld) > 0):
                errMsg += ' Error reducing file reference count for some files in the queue, check NGAS log to find out which files'
        del srvObj._subscrQueueDic[subscrId]
    else:
        estr = " Cannot find delivery queue for the subscriber '%s' kept internally. " % subscrId
        err += 1
        errMsg += estr

    filelist = srvObj.getDb().getSubscrBackLogBySubscrId(subscrId)

    # Mark back-logged files that have been in the queue (but not yet dequeued)
    if (srvObj._subscrBlScheduledDic.has_key(subscrId)):
        errOld = err
        myDic = srvObj._subscrBlScheduledDic[subscrId]
        srvObj._subscrBlScheduledDic_Sem.acquire()
        try:
            for fi in filelist:
                fileId = fi[0]
                fileVersion = fi[1]
                k = ngamsSubscriptionThread._fileKey(fileId, fileVersion)
                if (myDic.has_key(k)):
                    del myDic[k]
        except:
            estr = " Error marking back-logged files that have been in the queue for subscriber %s"
            logger.exception(estr, subscrId)
            err += 1
            errMsg += estr
        finally:
            srvObj._subscrBlScheduledDic_Sem.release()

    # reduce the file reference count by 1 for all files that are back logged for this subscriber
    if (srvObj.getCachingActive()):
        errOld = err
        for fi in filelist:
            fileId = fi[0]
            fileVersion = fi[1]
            err += _reduceRefCount(fileDeliveryCountDic, fileDeliveryCountDic_Sem, fileId, fileVersion)
        if ((err - errOld) > 0):
            errMsg += ' Error reducing file reference count for some files in the backlog, check NGAS log to find out which files'

    # remove all backlog entries associated with this subscriber
    try:
        srvObj.getDb().delSubscrBackLogEntries(srvObj.getHostId(), srvObj.getCfg().getPortNo(), subscrId)
    except Exception:
        estr = " Error deleting entries from the subscr_back_log table for subscriber %s"
        logger.exception(estr, subscrId)
        err += 1
        errMsg += estr
    if (not err):
        logger.info("Subscriber with ID: %s successfully unsubscribed", subscrId)
    return [err, errMsg]

def _reduceRefCount(fileDeliveryCountDic, fileDeliveryCountDic_Sem, fileId, fileVersion):
    """
    Reduce the reference count for files that have been scheduled for delivery

    return: 0 success, 1 failed
    """
    fkey = fileId + "/" + str(fileVersion)
    fileDeliveryCountDic_Sem.acquire()
    try:
        if (fileDeliveryCountDic.has_key(fkey)):
            fileDeliveryCountDic[fkey]  -= 1
            if (fileDeliveryCountDic[fkey] == 0):
                del fileDeliveryCountDic[fkey]
                # mark deletion -- should not mark deletion. The ONLY POSSIBLE place to mark deletion is when a file is successfully delivered
                # diskId = fi[2]
                # sqlFileInfo = (diskId, fileId, fileVersion)
                # ngamsCacheControlThread.scheduleFileForDeletion(srvObj, sqlFileInfo)
    except Exception:
        logger.exception(" Error reducing the reference count by 1 for file: %s", fileId)
        return 1
    finally:
        fileDeliveryCountDic_Sem.release()
    return 0


def handleCmd(srvObj,
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
            raise Exception(errMsg)
        err, errStr = delSubscriber(srvObj, ngamsLib.getSubscriberId(url))

    if err:
        httpRef.send_status('UNSUBSCRIBE command failed: ' + errStr, status=NGAMS_FAILURE, code=NGAMS_HTTP_SUCCESS)
    else:
        return "Successfully handled UNSUBSCRIBE command"