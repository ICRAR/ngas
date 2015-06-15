#******************************************************************************
#
#
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      11/12/2012  Created
#
""" A python wrapper that interacts with tape libraries using command line """

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import alert


def isFileOffline(filename):
    """
    To check if the file is completely offline, thus no copy is online
    
    return 1 - on tape, 0 - not on tape, -1 - query error
    """
    return isFileOnTape(filename)

def isFileOnTape(filename):
    """
    return 1 - on tape, 0 - not on tape, -1 - query error
    """
    cmd = "sls -D " + filename
    t = ngamsPlugInApi.execCmd(cmd, -1)
    exitCode = t[0]
    if (exitCode != 0 or len(t) != 2):
        errMsg = "Fail to query the online/offline status for file " + filename
        alert(errMsg)
        #print errMsg
        return -1 #raise error
    
    offline = t[1].find('offline;') # Do not use "archdone" any more given this reason: http://www.mail-archive.com/sam-qfs-discuss@mail.opensolaris.org/msg00419.html
    
    if (offline != -1): # the file is offline, i.e. it is on tape
        return 1
    else:
        return 0

def stageFiles(filenameList):
    """
    Stage a list of files. 
    The system will sort files in the order that they are archived on the tape volumes for better performance
    """
    cmd1 = "stage -r"
    cmd2 = "stage -r -w"
    num_staged = 0
    for filename in filenameList:
        if (isFileOnTape(filename) == 1):
            cmd1 = cmd1 + " " + filename
            cmd2 = cmd2 + " " + filename
            num_staged = num_staged + 1
            
    t = ngamsPlugInApi.execCmd(cmd1, -1)
    exitCode = t[0]
    if (exitCode != 0):
        errMsg = "Staging problem: " + str(exitCode) + ", cmd: " + cmd1
        alert(errMsg)
        #print errMsg
        return -1
                
    t = ngamsPlugInApi.execCmd(cmd2, -1)
    exitCode = t[0]
    if (exitCode != 0):
        errMsg = "Staging problem: " + str(exitCode) + ", cmd: " + cmd2
        alert(errMsg)
        return -1        
    
    return num_staged

if __name__ == '__main__':
    import time
    
    filepath = '/home/chenwu/MWA_HSM/NGAS_MWA_PBSTORE/volume1/afa/2012-10-01/1033124952/1/'
    L = ['1033124952_20121001110935_gpubox02_01.fits', '1033124952_20121001111017_gpubox01_02.fits', '1033124952_20121001110853_gpubox02_00.fits']
    filenamelist = [filepath + fn for fn in L]
    
    print ("start staging files......\n")
    starttime = int(round(time.time()))
    no_files = stageFiles(filenamelist)
    stoptime = int(round(time.time()))
    tt = stoptime - starttime
    print ("Staging " + str(no_files) + " files completed. Total time: " + str(tt) + "\n")
    
    
    
