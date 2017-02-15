#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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

from ngamsLib.ngamsCore import error, info, time


def ngamsJanitorCheckOldRequestsinDBM(srvObj, stopEvt, checkStopJanitorThread):
    """
    Check and if needs be clean up old requests.

    Remove a Request Properties Object from the queue if
     1. The request handling is completed for more than 24 hours (86400s).
     2. The request status has not been updated for more than 24 hours (86400s).

   srvObj:            Reference to NG/AMS server class object (ngamsServer).

   Returns:           Void.
   """
    info(4, "Checking/cleaning up Request DB ...")
    # reqTimeOut = 10
    reqTimeOut = 86400
    try:
        reqIds = srvObj.getRequestIds()
        for reqId in reqIds:
            reqPropsObj = srvObj.getRequest(reqId)
            checkStopJanitorThread(stopEvt)

            # Remove a Request Properties Object from the queue if
            #
            # 1. The request handling is completed for more than
            #    24 hours (86400s).
            # 2. The request status has not been updated for more
            #    than 24 hours (86400s).
            timeNow = time.time()
            if (reqPropsObj.getCompletionTime() != None):
                complTime = reqPropsObj.getCompletionTime()
                if ((timeNow - complTime) >= reqTimeOut):
                    info(4, "Removing request with ID from " + \
                         "Request DBM: %s" % str(reqId))
                    srvObj.delRequest(reqId)
                    continue
            if (reqPropsObj.getLastRequestStatUpdate() != None):
                lastReq = reqPropsObj.getLastRequestStatUpdate()
                if ((timeNow - lastReq) >= reqTimeOut):
                    info(4, "Removing request with ID from " + \
                         "Request DBM: %s" % str(reqId))
                    srvObj.delRequest(reqId)
                    continue
            time.sleep(0.020)

    except Exception, e:
        error("Exception encountered: %s" % str(e))
    info(4, "Request DB checked/cleaned up")
