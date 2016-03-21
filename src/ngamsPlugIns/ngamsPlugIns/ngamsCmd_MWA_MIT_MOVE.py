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
#
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      2014-10-31  Created
#
"""
Move a file (no version) from this host to a target NGAS (increasing the version id)
"""

import os, binascii

from ngamsLib.ngamsCore import info, warning, NGAMS_TEXT_MT, \
    NGAMS_SUCCESS, NGAMS_HTTP_SUCCESS, NGAMS_FAILURE
from ngamsLib import ngamsStatus, ngamsLib
from ngamsServer import ngamsDiscardCmd


MOVE_SUCCESS = 'MOVEALLOK'

"""
1. Check if the file on this host, and check if the target host has already got the file?
2. Calculate the CRC of this file, and compared it with the one inside the database
3. archive a copy of this file to the target NGAS (with the attached CRC code)(maximum retry = 3)
4. remove this file from the current host
"""

def isMWAVisFile(fileId):
    fileName, fileExtension = os.path.splitext(fileId)
    if ('.fits' != fileExtension.lower()):
        return False # only FITS file is considered

    if (fileName.find('_gpubox') == -1):
        return False

    ss = fileName.split('_')
    if (len(ss) != 4):
        return False

    return True

def fileOnHost(srvObj, fileId, hostId):
    """
    hostId:    string without port number, e.g. eor-01
    Return boolean
    """
    query = "SELECT COUNT(a.file_id) FROM ngas_files a, ngas_disks b WHERE a.file_id = '%s' AND b.host_id = '%s:7777' AND a.disk_id = b.disk_id" % (fileId, hostId)
    info(3, "Executing SQL query for checking file on target host: %s" % query)
    res = srvObj.getDb().query(query, maxRetries=1, retryWait=0)
    c = int(res[0][0][0])
    return (c > 0)

def getCRCFromFile(filename):
    block = "-"
    crc = 0
    blockSize = 1048576 # 1M block size
    fdIn = open(filename)
    while (block != ""):
        block = fdIn.read(blockSize)
        crc = binascii.crc32(block, crc)
    fdIn.close()
    return str(crc)

def getCRCFromDB(srvObj, fileId, fileVersion, diskId):
    """
    query = "SELECT checksum FROM ngas_files WHERE file_id = '%s' AND file_version = %d AND disk_id = '%s'" % (fileId, fileVersion, diskId)
    info(3, "Executing SQL query for file moving: %s" % query)
    res = srvObj.getDb().query(query, maxRetries=1, retryWait=0)
    reList = res[0]
    if (len(reList) < 1):
        return None
    else:
        return reList[0][0]
    """
    fileChecksum = None
    try:
        fileChecksum = srvObj.getDb().getFileChecksum(diskId, fileId, fileVersion)
    except Exception, eyy:
        warning('Fail to get file checksum for file %s: %s' % (fileId, str(eyy)))
    return fileChecksum

def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Compress the file based on file_path (file_id, disk_id, and file_version)

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    # target_host does not contain port
    required_params = ['file_path', 'file_id', 'file_version', 'disk_id', 'target_host', 'crc_db', 'debug']

    filename = reqPropsObj.getHttpPar('file_path')

    if (not os.path.exists(filename)):
        errMsg = 'File does not exist: %s' % filename
        warning(errMsg)
        srvObj.httpReply(reqPropsObj, httpRef, 501, errMsg, NGAMS_TEXT_MT)
        return

    parDic = reqPropsObj.getHttpParsDic()
    for rp in required_params:
        if (not parDic.has_key(rp)):
            errMsg = 'Parameter missing: %s' % rp
            srvObj.httpReply(reqPropsObj, httpRef, 502, errMsg, NGAMS_TEXT_MT)
            return
    info(3, 'Moving file %s' % filename)

    fileId = parDic['file_id']
    fileVersion = int(parDic['file_version'])
    diskId = parDic['disk_id']
    tgtHost = parDic['target_host']
    crcDb = parDic['crc_db']
    debug = int(parDic['debug'])

    if (not isMWAVisFile(fileId)):
        errMsg = 'Not MWA visibilty file: %' % (fileId)
        warning(errMsg)
        srvObj.httpReply(reqPropsObj, httpRef, 503, errMsg, NGAMS_TEXT_MT)
        return
    """
    if (fileOnHost(srvObj, fileId, tgtHost)):
        errMsg = "File %s already on host %s" % (fileId, tgtHost)
        warning(errMsg)
        srvObj.httpReply(reqPropsObj, httpRef, 200, errMsg, NGAMS_TEXT_MT) # make it correct
        return
    """
    if (debug):
        info(3, 'Only for file movment debugging, return now')
        srvObj.httpReply(reqPropsObj, httpRef, 200, 'DEBUG ONLY', NGAMS_TEXT_MT)
        return

    fileCRC = getCRCFromFile(filename)
    if (fileCRC != crcDb):
        errMsg = 'File %s on source host %s already corrupted, moving request rejected' % (filename, srvObj.getHostId().replace(':', '-'))
        warning(errMsg)
        srvObj.httpReply(reqPropsObj, httpRef, 504, errMsg, NGAMS_TEXT_MT)
        return

    sendUrl = 'http://%s:7777/QARCHIVE' % tgtHost
    info(3, "Moving to %s" % sendUrl)
    fileMimeType = 'application/octet-stream'
    baseName = os.path.basename(filename)
    contDisp = "attachment; filename=\"" + baseName + "\""
    contDisp += "; no_versioning=1"
    deliver_success = False
    last_deliv_err = ''
    for i in range(3): # total trials - 3 times
        stat = ngamsStatus.ngamsStatus()
        try:
            reply, msg, hdrs, data = ngamsLib.httpPostUrl(sendUrl, fileMimeType,
                                                            contDisp, filename, "FILE",
                                                            blockSize=\
                                                            srvObj.getCfg().getBlockSize(),
                                                            checkSum = fileCRC)
            if (data.strip() != ""):
                stat.clear().unpackXmlDoc(data)
            else:
                stat.clear().setStatus(NGAMS_SUCCESS)
            if (reply != NGAMS_HTTP_SUCCESS or stat.getStatus() == NGAMS_FAILURE):
                warning("Attempt %d failed: %s" % (i, stat.getMessage()))
                last_deliv_err = stat.getMessage()
                continue
            else:
                deliver_success = True
                break
        except Exception, hexp:
            warning("Attempt %d failed: %s" % (i, str(hexp)))
            last_deliv_err = str(hexp).replace('\n', '--')
            continue

    if (not deliver_success):
        errMsg = 'File %s failed to be moved to %s: %s' % (fileId, tgtHost, last_deliv_err)
        warning(errMsg)
        srvObj.httpReply(reqPropsObj, httpRef, 505, errMsg, NGAMS_TEXT_MT)
        return

    try:
        ngamsDiscardCmd._discardFile(srvObj, diskId, fileId, fileVersion, execute = 1,
                                     tmpFilePat = srvObj.getCfg().getRootDirectory() + '/tmp/')
    except Exception, e1:
        warning('Fail to remove file %s: %s' % (filename, str(e1)))
        srvObj.httpReply(reqPropsObj, httpRef, 200, 'Remove error: %s' % str(e1).replace('\n', '--'), NGAMS_TEXT_MT)
        return


    srvObj.httpReply(reqPropsObj, httpRef, 200, MOVE_SUCCESS, NGAMS_TEXT_MT)


