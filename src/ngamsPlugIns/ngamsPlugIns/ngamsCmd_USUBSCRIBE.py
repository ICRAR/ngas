#
#    ICRAR - International Centre for Radio Astronomy Research
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
# Who       When        What
# --------  ----------  -------------------------------------------------------
# chen.wu@icrar.org   14-Mar-2012    created
"""
this command updates an existing subscriber's information
including priority, url, start_date, and num_concurrent_threads
"""

import logging
import threading

from ngamsLib.ngamsCore import NGAMS_DELIVERY_THR, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE,\
    fromiso8601
from ngamsLib import ngamsSubscriber
from ngamsServer import ngamsSubscriptionThread


logger = logging.getLogger(__name__)

def changeNumThreads(srvObj, subscrId, oldNum, newNum):
    # key: threadName (unique), value - dummy 0
    deliveryThreadRefDic = srvObj._subscrDeliveryThreadDicRef
    # key: subscriberId, value - a List of deliveryThreads for that subscriber
    if (subscrId not in srvObj._subscrDeliveryThreadDic): # threads have not started yet
        return
    deliveryThreadList = srvObj._subscrDeliveryThreadDic[subscrId]

    if (oldNum > newNum):
        for tid in range(oldNum - 1, -1, -1):
            if (tid >= newNum):
                thrdName = NGAMS_DELIVERY_THR + subscrId + str(tid)
                del deliveryThreadRefDic[thrdName] # set the condition _deliveryThread will exit, see ngamsSubscriptionThread._checkStopDataDeliveryThread()
                del deliveryThreadList[tid]
    elif (oldNum < newNum):
        num_threads = newNum - oldNum
        if (subscrId not in srvObj._subscrQueueDic):
            raise Exception('Cannot find the file queue associated with subscriber %s' % subscrId)
        quChunks = srvObj._subscrQueueDic[subscrId]

        for tid in range(int(num_threads)):
            args = (srvObj, srvObj.getSubscriberDic()[subscrId], quChunks, srvObj._subscrFileCountDic, srvObj._subscrFileCountDic_Sem, None)
            thrdName = NGAMS_DELIVERY_THR + subscrId + str(oldNum + tid)
            deliveryThrRef = threading.Thread(None, ngamsSubscriptionThread._deliveryThread, thrdName, args)
            deliveryThrRef.setDaemon(0)
            deliveryThreadRefDic[thrdName] = 1
            deliveryThrRef.start()
            deliveryThreadList.append(deliveryThrRef)


def handleCmd(srvObj,
              reqPropsObj,
              httpRef):
    """
    Handle the update subscriber (USUBSCRIBE) Command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    errMsg = ''
    err = 0
    if (not reqPropsObj.hasHttpPar("subscr_id")):
        httpRef.send_status("USUBSCRIBE command failed: 'subscr_id' is not specified",
                            status=NGAMS_FAILURE, code=NGAMS_HTTP_SUCCESS)
        return

    subscrId = reqPropsObj.getHttpPar("subscr_id")
    if (subscrId not in srvObj.getSubscriberDic()):
        httpRef.send_status("USUBSCRIBE command failed: Cannot find subscriber '%s'" % subscrId,
                            status=NGAMS_FAILURE, code=NGAMS_HTTP_SUCCESS)
        return

    if (reqPropsObj.hasHttpPar("suspend")):
        suspend = int(reqPropsObj.getHttpPar("suspend"))
        suspend_processed = 0
        # could use locks, but the race condition should not really matter here if only one request of suspend is issued at a time (by a system admin?)
        if (suspend == 1 and srvObj._subscrSuspendDic[subscrId].is_set()): # suspend condition met
            srvObj._subscrSuspendDic[subscrId].clear()
            suspend_processed = 1
            action = 'SUSPENDED'
        elif (suspend == 0 and (not srvObj._subscrSuspendDic[subscrId].is_set())): # resume condition met
            srvObj._subscrSuspendDic[subscrId].set()
            suspend_processed = 1
            action = 'RESUMED'
        if (suspend_processed):
            httpRef.send_status("Successfully %s for the subscriber %s" % (action, subscrId))
        else:
            reMsg = "No suspend/resume action is taken for the subscriber %s" % subscrId
            httpRef.send_status(reMsg, status=NGAMS_FAILURE, code=NGAMS_HTTP_SUCCESS)
        return

    subscriber = srvObj.getSubscriberDic()[subscrId]

    if (reqPropsObj.hasHttpPar("priority")):
        priority = int(reqPropsObj.getHttpPar("priority"))
        subscriber.setPriority(priority)

    if (reqPropsObj.hasHttpPar("url")):
        url = reqPropsObj.getHttpPar("url")
        ngamsSubscriber.validate_url(url)
        subscriber.setUrl(url)

    if (reqPropsObj.hasHttpPar("start_date")):
        startDate = reqPropsObj.getHttpPar("start_date").strip()
        if (startDate):
            subscriber.setStartDate(fromiso8601(startDate))
            lastIngDate = subscriber.getLastFileIngDate()
            if lastIngDate is not None: # either re-check past files or skip unchecked files
                subscriber.setLastFileIngDate(None)
            if (subscrId in srvObj._subscrScheduledStatus):
                #if (startDate < srvObj._subscrScheduledStatus[subscrId] and srvObj._subscrScheduledStatus[subscrId]): # enables trigger re-delivering files that have been previously delivered
                del srvObj._subscrScheduledStatus[subscrId]
            if (subscrId in srvObj._subscrCheckedStatus):
                del srvObj._subscrCheckedStatus[subscrId]
                #if (srvObj._subscrScheduledStatus[subscrId]):# either re-check past files or skip unchecked files
                    #del srvObj._subscrScheduledStatus[subscrId]
                    #srvObj._subscrScheduledStatus[subscrId] = None

    if (reqPropsObj.hasHttpPar("filter_plug_in")):
        filterPi = reqPropsObj.getHttpPar("filter_plug_in")
        subscriber.setFilterPi(filterPi)

    if (reqPropsObj.hasHttpPar("plug_in_pars")):
        pipars = reqPropsObj.getHttpPar("plug_in_pars")
        subscriber.setFilterPiPars(pipars)

    if (reqPropsObj.hasHttpPar("concurrent_threads")):
        ccthrds = int(reqPropsObj.getHttpPar("concurrent_threads"))
        origthrds = int(subscriber.getConcurrentThreads())
        if (ccthrds != origthrds):
            subscriber.setConcurrentThreads(ccthrds)
            try:
                changeNumThreads(srvObj, subscrId, origthrds, ccthrds)
            except Exception as e:
                msg = " Exception updating subscriber's concurrent threads: %s." % str(e)
                logger.warning(msg)
                err += 1
                errMsg += msg
    try:
        srvObj.getDb().updateSubscriberEntry(subscriber)
        srvObj.addSubscriptionInfo([], [subscriber]).triggerSubscriptionThread()
    except Exception as e:
        msg = " Update subscriber in DB exception: %s." % str(e)
        logger.warning(msg)
        err += 1
        errMsg += msg
    if (err):
        httpRef.send_status("USUBSCRIBE command failed. Exception: %s" % errMsg, status=NGAMS_FAILURE, code=NGAMS_HTTP_SUCCESS)
    else:
        httpRef.send_status("USUBSCRIBE command succeeded")
