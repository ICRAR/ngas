def Check_Disk_Space(srvObj, stopEvt):
    """
	Check if there is enough disk space for the various
    directories defined.

   srvObj:            Reference to NG/AMS server class object (ngamsServer).

   Returns:           Void.
   """
    from ngamsLib.ngamsCore import NGAMS_OFFLINE_CMD, NGAMS_HTTP_INT_AUTH_USER, alert, getHostName
    import ngamsLib

    try:
        srvObj.checkDiskSpaceSat()
    except Exception, e:
        alert(str(e))
        alert("Bringing the system to Offline State ...")
        # We use a small trick here: We send an Offline Command to
        # the process itself.
        #
        # If authorization is on, fetch a key of a defined user.
        if (srvObj.getCfg().getAuthorize()):
            authHdrVal = srvObj.getCfg(). \
                getAuthHttpHdrVal(NGAMS_HTTP_INT_AUTH_USER)
        else:
            authHdrVal = ""
        ngamsLib.httpGet(getHostName(), srvObj.getCfg().getPortNo(),
                         NGAMS_OFFLINE_CMD, 0,
                         [["force", "1"], ["wait", "0"]],
                         "", 65536, 30, 0, authHdrVal)