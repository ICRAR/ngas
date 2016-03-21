#******************************************************************************
#
#
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      07/12/2012  Created
#
""" Contains a DDPI which stage the file from Tape to the disk (only if the file is offline). """

from ngamsLib import ngamsPlugInApi, ngamsDppiStatus
from ngamsLib.ngamsCore import NGAMS_PROC_FILE, alert, info


def ngamsMWACortexStageDppi(srvObj,
                           reqPropsObj,
                           filename):
    """
    This plugin stages the file from Tape to the disk, works ONLY in the iVEC Cortex environment.

    srvObj:        Reference to instance of the NG/AMS Server
                   class (ngamsServer).

    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).

    filename:      Name of file to process (string).

    Returns:       DPPI return status object (ngamsDppiStatus).
    """
    #mimeType = ngamsPlugInApi.determineMimeType(srvObj.getCfg(), filename)
    mimeType = "application/octet-stream" #hardcode this since our file extension is actually ".fits"
    resultObj = ngamsDppiStatus.ngamsDppiResult(NGAMS_PROC_FILE, mimeType,
                                                    filename, filename)
    statusObj = ngamsDppiStatus.ngamsDppiStatus().addResult(resultObj)

    #procFilename, procDir = ngamsPlugInApi.prepProcFile(srvObj.getCfg(), filename)
    cmd = "sls -D " + filename
    t = ngamsPlugInApi.execCmd(cmd, -1)
    exitCode = t[0]
    if (exitCode != 0 or len(t) != 2):
        errMsg = "Fail to query the online/offline status for file " + filename
        alert(errMsg)
        return statusObj #the sls -D command failed to execute, but retrieval might still go on, so just simply return empty result

    offline = t[1].find('offline;')

    if (offline != -1): # the file is offline, i.e. it is on tape
        info(3, "File " + filename + " is currently on tapes, staging it for retrieval...")
        cmd = "stage -w " + filename
        t = ngamsPlugInApi.execCmd(cmd, -1) #stage it back to disk cache
        info(3, "File " + filename + " staging completed.")

    return statusObj
