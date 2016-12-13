def Check_Processing_Directory(srvObj, stopEvt,checkCleanDirs):
    """
    Check and clean up Processing Directory

    srvObj:            Reference to NG/AMS server class object (ngamsServer).

    Returns:           Void.
    """
    import os
    from ngamsLib.ngamsCore import NGAMS_PROC_DIR
    from ngamsLib.ngamsCore import info

    info(4, "Checking/cleaning up Processing Directory ...")
    procDir = os.path.normpath(srvObj.getCfg(). \
                               getProcessingDirectory() + \
                               "/" + NGAMS_PROC_DIR)
    checkCleanDirs(procDir, 1800, 1800, 0)
    info(4, "Processing Directory checked/cleaned up")