def Check_to_Suspend_NGAS_Host(srvObj, stopEvt):
    """
	Check if the conditions for suspending this NGAS Host are met.

   srvObj:            Reference to NG/AMS server class object (ngamsServer).

   Returns:           Void.
   """

    from ngamsLib.ngamsCore import error, info, logFlush, NGAMS_NOTIF_ERROR, loadPlugInEntryPoint
    import time
    from ngamsLib import ngamsNotification


    hostId = srvObj.getHostId()
    srvDataChecking = srvObj.getDb().getSrvDataChecking(hostId)
    if ((not srvDataChecking) and
            (srvObj.getCfg().getIdleSuspension()) and
            (not srvObj.getHandlingCmd())):
        timeNow = time.time()
        # Conditions are that the time since the last request was
        # handled exceeds the time for suspension defined.
        if ((timeNow - srvObj.getLastReqEndTime()) >=
                srvObj.getCfg().getIdleSuspensionTime()):
            # Conditions are met for suspending this NGAS host.
            info(2, "NG/AMS Server: %s suspending itself ..." % \
                 hostId)

            # If Data Checking is on, we request a wake-up call.
            if (srvObj.getCfg().getDataCheckActive()):
                wakeUpSrv = srvObj.getCfg().getWakeUpServerHost()
                nextDataCheck = srvObj.getNextDataCheckTime()
                srvObj.reqWakeUpCall(wakeUpSrv, nextDataCheck)

            # Now, suspend this host.
            srvObj.getDb().markHostSuspended(srvObj.getHostId())
            suspPi = srvObj.getCfg().getSuspensionPlugIn()
            info(3, "Invoking Suspension Plug-In: " + suspPi + " to " + \
                 "suspend NG/AMS Server: " + hostId + " ...")
            logFlush()
            try:
                plugInMethod = loadPlugInEntryPoint(suspPi)
                plugInMethod(srvObj)
            except Exception, e:
                errMsg = "Error suspending NG/AMS Server: " + \
                         hostId + " using Suspension Plug-In: " + \
                         suspPi + ". Error: " + str(e)
                error(errMsg)
                ngamsNotification.notify(srvObj.getHostId(), srvObj.getCfg(),
                                         NGAMS_NOTIF_ERROR,
                                         "ERROR INVOKING SUSPENSION " + \
                                         "PLUG-IN", errMsg)