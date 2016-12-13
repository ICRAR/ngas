import ngamsServer

def Check_to_Wakeup_OtherNGAS_Host(srvObj, stopEvt):
    """
	Check if this NG/AMS Server is requested to wake up another/other NGAS Host(s).

   srvObj:            Reference to NG/AMS server class object (ngamsServer).

   Returns:           Void.
   """
    import time
    from ngamsLib.ngamsCore import info

    hostId = srvObj.getHostId()
    timeNow = time.time()
    for wakeUpReq in srvObj.getDb().getWakeUpRequests(srvObj.getHostId()):
        # Check if the individual host is 'ripe' for being woken up.
        suspHost = wakeUpReq[0]
        if (timeNow > wakeUpReq[1]):
            info(2, "Found suspended NG/AMS Server: " + suspHost + " " + \
                 "that should be woken up by this NG/AMS Server: " + \
                 hostId + " ...")
            ngamsServer.ngamsSrvUtils.wakeUpHost(srvObj, suspHost)