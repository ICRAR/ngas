def Check_Subscr_Backlog_n_Temp_Dir(srvObj, stopEvt,checkCleanDirs):
    """
	Checks/cleans up Subscription Back-Log Buffer and
    Checks/cleans up NG/AMS Temp Directory of any leftover files.

   srvObj:            Reference to NG/AMS server class object (ngamsServer).

   Returns:           Void.
   """
    from ngamsLib.ngamsCore import info, isoTime2Secs
    import os
    from ngamsLib.ngamsCore import NGAMS_SUBSCR_BACK_LOG_DIR
    from ngamsLib import ngamsHighLevelLib

    info(4, "Checking/cleaning up Subscription Back-Log Buffer ...")
    backLogDir = os.path. \
        normpath(srvObj.getCfg().getBackLogBufferDirectory() + \
                 "/" + NGAMS_SUBSCR_BACK_LOG_DIR)
    expTime = isoTime2Secs(srvObj.getCfg().getBackLogExpTime())
    checkCleanDirs(backLogDir, expTime, expTime, 0)
    info(4, "Subscription Back-Log Buffer checked/cleaned up")

    # => Check if there are left-over files in the NG/AMS Temp. Dir.
    info(4, "Checking/cleaning up NG/AMS Temp Directory ...")
    tmpDir = ngamsHighLevelLib.getTmpDir(srvObj.getCfg())
    expTime = (12 * 3600)
    checkCleanDirs(tmpDir, expTime, expTime, 1)
    info(4, "NG/AMS Temp Directory checked/cleaned up")