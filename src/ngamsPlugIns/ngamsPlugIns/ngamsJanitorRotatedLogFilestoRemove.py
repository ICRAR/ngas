def Rotated_Log_FilestoRemove(srvObj, stopEvt):
    """
	Check if there are expired or rotated Local Log Files to remove.

   srvObj:            Reference to NG/AMS server class object (ngamsServer).

   Returns:           Void.
   """
    from ngamsLib.ngamsCore import info, rmFile
    import os, glob

    info(4, "Check if there are rotated Local Log Files to remove ...")
    logFile = srvObj.getCfg().getLocalLogFile()
    logPath = os.path.dirname(logFile)

    rotLogFilePat = os.path.normpath(logPath + "/LOG-ROTATE-*.nglog")
    rotLogFileList = glob.glob(rotLogFilePat)
    delLogFiles = (len(rotLogFileList) - \
                   srvObj.getCfg().getLogRotateCache())
    if (delLogFiles > 0):
        rotLogFileList.sort()
        for n in range(delLogFiles):
            info(1, "Removing Rotated Local Log File: " + \
                 rotLogFileList[n])
            rmFile(rotLogFileList[n])
    info(4, "Checked for expired, rotated Local Log Files")