
import ngamsServer

def CheckUnsavedLogFile(srvObj, stopEvt):
    """
	Checks to see if we have an unsaved log file after a shutdown and
    archives them.

   srvObj:            Reference to NG/AMS server class object (ngamsServer).

   Returns:           Void.
   """
    import os, glob, shutil, sys
    from ngamsLib.ngamsCore import info

    info(4, "Checking if we have unsaved Log File ")
    logFile = srvObj.getCfg().getLocalLogFile()
    logPath = os.path.dirname(logFile)
    if (os.path.exists(srvObj.getCfg().getLocalLogFile())):
        unsavedLogFiles = glob.glob(logPath + '/*.unsaved')
        if (len(unsavedLogFiles) > 0):
            info(3, "Archiving unsaved log-files ...")
            for ulogFile in unsavedLogFiles:
                ologFile = '.'.join(ulogFile.split('.')[:-1])
                shutil.move(ulogFile, ologFile)
                ngamsServer.ngamsArchiveUtils.archiveFromFile(srvObj, ologFile, 0,
                                                  'ngas/nglog', None)