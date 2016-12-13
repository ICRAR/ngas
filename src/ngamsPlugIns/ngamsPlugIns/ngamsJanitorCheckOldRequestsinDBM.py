def Check_Old_RequestsinDBM(srvObj, stopEvt, checkStopJanitorThread):
    """
    Check and if needs be clean up old requests.

    Remove a Request Properties Object from the queue if
     1. The request handling is completed for more than 24 hours (86400s).
     2. The request status has not been updated for more than 24 hours (86400s).

   srvObj:            Reference to NG/AMS server class object (ngamsServer).

   Returns:           Void.
   """
    from ngamsLib.ngamsCore import error, info, time

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
