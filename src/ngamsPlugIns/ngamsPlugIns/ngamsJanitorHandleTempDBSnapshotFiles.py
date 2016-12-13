import ngamsServer

def Handle_TempDB_SnapShot_Files(srvObj, stopEvt,updateDbSnapShots):
    """
    Check if there are any Temporary DB Snapshot Files to handle.

    srvObj:            Reference to NG/AMS server class object (ngamsServer).

    Returns:           Void.
    """
    from ngamsLib.ngamsCore import error, info, genLog

    try:
        updateDbSnapShots(srvObj, stopEvt)
    except Exception, e:
        error("Error encountered updating DB Snapshots: " + str(e))

    # => Check Back-Log Buffer (if appropriate).
    if (srvObj.getCfg().getAllowArchiveReq() and \
                srvObj.getCfg().getBackLogBuffering()):
        info(4, "Checking Back-Log Buffer ...")
        try:
            ngamsServer.ngamsArchiveUtils.checkBackLogBuffer(srvObj)
        except Exception, e:
            errMsg = genLog("NGAMS_ER_ARCH_BACK_LOG_BUF", [str(e)])
            error(errMsg)
