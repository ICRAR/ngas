#
#    ICRAR - International Centre for Radio Astronomy Research
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
#******************************************************************************
"""
NGAS Command Plug-In, implementing a Archive PULL Command using BBCP

This works by calling archiveFromFile, which in turn takes care of all the handling

Usgae example with wget:

wget -O BARCHIVE.xml "http://ngas.ddns.net:7777/BBCPARC?fileUri=/home/ngas/NGAS/log/LogFile.nglog"

Usage example with curl to send file from Pawsey to MIT:

(without checksum)
curl --connect-timeout 7200 eor-12.mit.edu:7777/BBCPARC?fileUri=ngas%40146.118.84.67%3A/mnt/mwa01fs/MWA/testfs/KNOPPIX_V7.2.0DVD-2013-06-16-EN.iso\&bport=7790\&bwinsize=%3D32m\&bnum_streams=12\&mimeType=application/octet-stream

(with checksum)
curl --connect-timeout 7200 eor-12.mit.edu:7777/BBCPARC?fileUri=ngas%40146.118.84.67%3A/mnt/mwa01fs/MWA/testfs/output_320M_001.dat\&bport=7790\&bwinsize=%3D32m\&bnum_streams=12\&mimeType=application/octet-stream\&bchecksum=-354041269
"""

from collections import namedtuple
import commands
import os
import time

from ngamsLib import ngamsHighLevelLib, ngamsPlugInApi
from ngamsLib.ngamsCore import info, checkCreatePath, genLog, alert, TRACE, \
    NGAMS_SUCCESS, NGAMS_HTTP_GET, NGAMS_ARCHIVE_CMD, NGAMS_HTTP_FILE_URL, \
    NGAMS_NOTIF_NO_DISKS, setLogCache, mvFile, notice, NGAMS_FAILURE, error, \
    NGAMS_PICKLE_FILE_EXT, rmFile, NGAMS_ONLINE_STATE, NGAMS_IDLE_SUBSTATE, \
    NGAMS_BUSY_SUBSTATE, getDiskSpaceAvail, NGAMS_HTTP_SUCCESS,\
    loadPlugInEntryPoint
from ngamsServer import ngamsArchiveUtils, ngamsCacheControlThread
from pccUt import PccUtTime


bbcp_param = namedtuple('bbcp_param', 'port, winsize, num_streams, checksum')

def bbcpFile(srcFilename, targFilename, bparam,
    keyfile='', ssh_prefix=''):
    """
    Use bbcp tp copy file <srcFilename> to <targFilename>

    NOTE: This requires remote access to the host as well as
         a bbcp installation on both the remote and local host.
    """
    info(3,"Copying file: " + srcFilename + " to filename: " + targFilename)
    try:
        # Make target file writable if existing.
        if (os.path.exists(targFilename)): os.chmod(targFilename, 420)
        checkCreatePath(os.path.dirname(targFilename))
        #fileSize = getFileSize(srcFilename)
        #checkAvailDiskSpace(targFilename, fileSize)
        timer = PccUtTime.Timer()
        if keyfile: keyfile = '-i {0}'.format(keyfile)

        if bparam.port:
            pt = '-Z %d' % bparam.port
        else:
            pt = '-Z 5678'

        if bparam.winsize:
            fw = '-w %s' % bparam.winsize
        else:
            fw = ''

        if (bparam.num_streams):
            ns = '-s %d' % bparam.num_streams
        else:
            ns = ''

        cmd = "bbcp -r -V -a %s %s -P 2 %s %s %s %s%s" %\
                (fw, ns, pt, keyfile, srcFilename, ssh_prefix, targFilename)
        info(3, "Executing external command: {0}".format(cmd))
        stat, out = commands.getstatusoutput(cmd)
        if (stat): raise Exception, "Error executing copy command: %s: %s"%\
                   (cmd, str(out))
        deltaTime = timer.stop()
        info(3, 'BBCP final message: ' + out.split('\n')[-1]) # e.g. "1 file copied at effectively 18.9 MB/s"
    except Exception, e:
        errMsg = genLog("NGAMS_AL_CP_FILE", [srcFilename, targFilename, str(e)])
        alert(errMsg)
        raise Exception, errMsg
    info(3,"File: %s copied to filename: %s" % (srcFilename, targFilename))
    return deltaTime


def archiveFromFile(srvObj,
                    filename,
                    bparam,
                    noReplication = 0,
                    mimeType = None,
                    reqPropsObj = None):
    """
    Archive a file directly from a file as source.

    srvObj:          Reference to NG/AMS Server Object (ngamsServer).

    filename:        Name of file to archive (string).

    bparam:          BBCP parameter (named tuple)

    noReplication:   Flag to enable/disable replication (integer).

    reqPropsObj:     Request Property object to keep track of actions done
                     during the request handling (ngamsReqProps).

    Returns:         Execution result object of DAPI
    """
    T = TRACE()

    info(2,"Archiving file: " + filename + " ...")
    if (reqPropsObj):
        info(3,"Request Properties Object given - using this")
        reqPropsObjLoc = reqPropsObj
    else:
        info(3,"No Request Properties Object given - creating one")
        reqPropsObjLoc = ngamsArchiveUtils.ngamsReqProps.ngamsReqProps()
    stagingFile = filename
    try:
        if (mimeType == None):
            mimeType = ngamsHighLevelLib.determineMimeType(srvObj.getCfg(),
                                                           filename)
        archiveTimer = PccUtTime.Timer()

        # Prepare dummy ngamsReqProps object (if an object was not given).
        if (not reqPropsObj):
            reqPropsObjLoc.setMimeType(mimeType)
            reqPropsObjLoc.setStagingFilename(filename)
            reqPropsObjLoc.setHttpMethod(NGAMS_HTTP_GET)
            reqPropsObjLoc.setCmd(NGAMS_ARCHIVE_CMD)
            reqPropsObjLoc.setSize(os.path.getsize(filename))
            reqPropsObjLoc.setFileUri(NGAMS_HTTP_FILE_URL + filename)
            reqPropsObjLoc.setNoReplication(noReplication)

        # If no target disk is defined, find one suitable disk.
        if (not reqPropsObjLoc.getTargDiskInfo()):
            try:
                trgDiskInfo = ngamsArchiveUtils.ngamsDiskUtils.\
                              findTargetDisk(srvObj.getHostId(),
                                             srvObj.getDb(), srvObj.getCfg(),
                                             mimeType, 0,
                                             reqSpace=reqPropsObjLoc.getSize())
                reqPropsObjLoc.setTargDiskInfo(trgDiskInfo)
                # copy the file to the staging area of the target disk
                stagingFile = trgDiskInfo.getMountPoint()+ '/staging/' + os.path.basename(filename)
                bbcpFile(filename, stagingFile, bparam)
                reqPropsObjLoc.setStagingFilename(stagingFile)
            except Exception, e:
                errMsg = str(e) + ". Attempting to bbcp archive file: " +\
                         filename
                ngamsPlugInApi.notify(srvObj,
                                         NGAMS_NOTIF_NO_DISKS,
                                         "NO DISKS AVAILABLE", errMsg)
                raise Exception, errMsg

        # Set the log cache to 1 during the handling of the file.
        setLogCache(1)
        plugIn = srvObj.getMimeTypeDic()[mimeType]
        info(2,"Invoking DAPI: " + plugIn + " to handle file: " + stagingFile)
        plugInMethod = loadPlugInEntryPoint(plugIn)
        resMain = plugInMethod(srvObj, reqPropsObjLoc)
        # Move the file to final destination.
        st = time.time()
        mvFile(reqPropsObjLoc.getStagingFilename(),
               resMain.getCompleteFilename())
        iorate = reqPropsObjLoc.getSize()/(time.time() - st)
        setLogCache(10)

        diskInfo = ngamsArchiveUtils.postFileRecepHandling(srvObj, reqPropsObjLoc, resMain)
        if (bparam.checksum):
            #info(3, "Checking the checksum")
            fileObj = diskInfo.getFileObj(0)
            cal_checksum = fileObj.getChecksum()
            if (cal_checksum != bparam.checksum):
                info(3, "Checksum inconsistency, removing the file from the archive")
                from ngamsServer import ngamsDiscardCmd
                work_dir = srvObj.getCfg().getRootDirectory() + '/tmp/'
                ngamsDiscardCmd._discardFile(srvObj, diskInfo.getDiskId(), fileObj.getFileId(),
                                             int(fileObj.getFileVersion()), execute = 1, tmpFilePat = work_dir)
                raise Exception("Check sum error: remote CRC: %s, local CRC: %s" % (bparam.checksum, cal_checksum))

    except Exception, e:
        # If another error occurs, than one qualifying for Back-Log
        # Buffering the file, we have to log an error.
        if (ngamsHighLevelLib.performBackLogBuffering(srvObj.getCfg(),
                                                      reqPropsObjLoc, e)):
            notice("Tried to archive local file: " + filename +\
                   ". Attempt failed with following error: " + str(e) +\
                   ". Keeping original file.")
            return [NGAMS_FAILURE, str(e), NGAMS_FAILURE]
        else:
            error("Tried to archive local file: " + filename +\
                  ". Attempt failed with following error: " + str(e) + ".")

            """
            # this will not work for bbcp files since they are often remote, cannot be moved like this
            notice("Moving local file: " +\
                   filename + " to Bad Files Directory -- cannot be handled.")
            ngamsHighLevelLib.moveFile2BadDir(srvObj.getCfg(), filename,
                                              filename)
            """
            # Remove pickle file if available.
            pickleObjFile = filename + "." + NGAMS_PICKLE_FILE_EXT
            if (os.path.exists(pickleObjFile)):
                info(2,"Removing Back-Log Buffer Pickle File: "+pickleObjFile)
                rmFile(pickleObjFile)
            return [NGAMS_FAILURE, str(e), NGAMS_FAILURE]

    # If the file was handled successfully, we remove it from the
    # Back-Log Buffer Directory unless the local file was a log-file
    # in which case we leave the cleanup to the Janitor-Thread.
    if stagingFile.find('LOG-ROTATE') > -1:
        info(2,"Successfully archived local file: " + filename)
    else:
        info(2,"Successfully archived local file: " + filename +\
         ". Removing original file.")
        rmFile(stagingFile)
        rmFile(stagingFile + "." + NGAMS_PICKLE_FILE_EXT)

    info(2,"Archived local file: " + filename + ". Time (s): " +\
         str(archiveTimer.stop()))
    return (resMain, trgDiskInfo, iorate)



def handleCmd(srvObj,
              reqPropsObj,
              httpRef):
    """
    Handle the Quick Archive (QARCHIVE) Command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        (fileId, filePath) tuple.
    """
    T = TRACE()

    # Check if the URI is correctly set.
    info(3, "Check if the URI is correctly set.")
    info(3,"ReqPropsObj status: {0}".format(reqPropsObj.getObjStatus()))
    parsDic = reqPropsObj.getHttpParsDic()
    if (not parsDic.has_key('fileUri') or parsDic['fileUri'] == ""):
        errMsg = genLog("NGAMS_ER_MISSING_URI")
        error(errMsg)
        raise Exception, errMsg
    else:
        reqPropsObj.setFileUri(parsDic['fileUri'])
        fileUri = reqPropsObj.getFileUri()
    if (not parsDic.has_key('mimeType') or parsDic['mimeType'] == ""):
        mimeType = ""
        reqPropsObj.setMimeType("")
    else:
        reqPropsObj.setMimeType(parsDic['mimeType'])
        mimeType = reqPropsObj.getMimeType()
    # Is this NG/AMS permitted to handle Archive Requests?
    info(3, "Is this NG/AMS permitted to handle Archive Requests?")
    if (not srvObj.getCfg().getAllowArchiveReq()):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Archive"])
        raise Exception, errMsg
    srvObj.checkSetState("Archive Request", [NGAMS_ONLINE_STATE],
                         [NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE],
                         NGAMS_ONLINE_STATE, NGAMS_BUSY_SUBSTATE,
                         updateDb=False)

    # Get mime-type (try to guess if not provided as an HTTP parameter).
    info(3, "Get mime-type (try to guess if not provided as an HTTP parameter).")
    if (reqPropsObj.getMimeType() == ""):
        mimeType = ngamsHighLevelLib.\
                   determineMimeType(srvObj.getCfg(), reqPropsObj.getFileUri())
        reqPropsObj.setMimeType(mimeType)
    else:
        mimeType = reqPropsObj.getMimeType()

    ioTime = 0
    reqPropsObj.incIoTime(ioTime)


    port = None
    winsize = None
    num_streams = None
    checksum = None
    if (parsDic.has_key('bport')):
        port = int(parsDic['bport'])
    if (parsDic.has_key('bwinsize')):
        winsize = parsDic['bwinsize']
    if (parsDic.has_key('bnum_streams')):
        num_streams = int(parsDic['bnum_streams'])
    if (parsDic.has_key('bchecksum')):
        checksum = parsDic['bchecksum']


    bparam = bbcp_param(port, winsize, num_streams, checksum)

    (resDapi, targDiskInfo, iorate) = archiveFromFile(srvObj, fileUri, bparam, 0, mimeType, reqPropsObj)
    if (resDapi == NGAMS_FAILURE):
        errMsg = targDiskInfo
        srvObj.httpReply(reqPropsObj, httpRef, 500, errMsg + '\n')
        return

    # Inform the caching service about the new file.
    info(3, "Inform the caching service about the new file.")
    if (srvObj.getCachingActive()):
        diskId      = resDapi.getDiskId()
        fileId      = resDapi.getFileId()
        fileVersion = 1
        filename    = resDapi.getRelFilename()
        ngamsCacheControlThread.addEntryNewFilesDbm(srvObj, diskId, fileId,
                                                   fileVersion, filename)

    # Update disk info in NGAS Disks.
    info(3, "Update disk info in NGAS Disks.")
    srvObj.getDb().updateDiskInfo(resDapi.getFileSize(), resDapi.getDiskId())

    # Check if the disk is completed.
    # We use an approximate estimate for the remaning disk space to avoid
    # to read the DB.
    info(3, "Check available space in disk")
    availSpace = getDiskSpaceAvail(targDiskInfo.getMountPoint(), smart=False)
    if (availSpace < srvObj.getCfg().getFreeSpaceDiskChangeMb()):
        complDate = PccUtTime.TimeStamp().getTimeStamp()
        targDiskInfo.setCompleted(1).setCompletionDate(complDate)
        targDiskInfo.write(srvObj.getDb())

    # Request after-math ...
    srvObj.setSubState(NGAMS_IDLE_SUBSTATE)
    msg = "Successfully handled Archive Pull Request for data file " +\
          "with URI: " + reqPropsObj.getSafeFileUri()
    info(1, msg)
    srvObj.ingestReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                       NGAMS_SUCCESS, msg, targDiskInfo)

    # Trigger Subscription Thread. This is a special version for MWA, in which we simply swapped MIRRARCHIVE and QARCHIVE
    # chen.wu@icrar.org
    msg = "triggering SubscriptionThread for file %s" % resDapi.getFileId()
    info(3, msg)
    srvObj.addSubscriptionInfo([(resDapi.getFileId(),
                                 resDapi.getFileVersion())], [])
    srvObj.triggerSubscriptionThread()


    return (resDapi.getFileId(), '%s/%s' % (targDiskInfo.getMountPoint(), resDapi.getRelFilename()),
            iorate)

# EOF
