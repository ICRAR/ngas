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
# cwu      2014-08-08  Created
#
"""
Reinstall NGAS on eor-02
    fab --host=eor-02.mit.edu -u ngas --set standalone=1 -f machine-setup/deploy.py user_deploy

Deployt uvcompress on eor-02

Deploy this command on all EOR machines

This command wil be invoked by a central processing client
"""

import os, commands, binascii

from ngamsLib.ngamsCore import error, info, warning, NGAMS_TEXT_MT, NGAMS_HTTP_SUCCESS, getFileSize


debug = 0
uvcompress = '/home/ngas/ngas_rt/bin/uvcompress'
work_dir = '/tmp'
sf = 4 # scaling factor
th = 1E-5 # threshold
bins = 0
remove_uc = 0
timeout = 600 # each command should not run more than 10 min, otherwise something is wrong

def execCmd(cmd, failonerror = False, okErr = []):
    re = commands.getstatusoutput(cmd)
    if (re[0] != 0 and not (re[0] in okErr)):
        errMsg = 'Fail to execute command: "%s". Exception: %s' % (cmd, re[1])
        if (failonerror):
            raise Exception(errMsg)
            
    return re
    """
    info(3, 'Executing command: %s' % cmd)
    try:
        ret = ngamsPlugInApi.execCmd(cmd, timeout)
    except Exception, ex:
        if (str(ex).find('timed out') != -1):
            return (-1, 'Timed out (%d seconds): %s' % (timeout, cmd))
        else:
            return (-1, str(ex))
    if (ret):
        return ret
    else:
        return (-1, 'Unknown error')
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

def hasCompressed(filename):
    cmd = 'head -c %d %s' % (1024 * 3, filename)    
    try:
        #re = ngamsPlugInApi.execCmd(cmd, 60)
        re = commands.getstatusoutput(cmd)
    except Exception, ex:
        if (str(ex).find('timed out') != -1):
            error('Timed out when checking FITS header %s' % cmd)
        else:
            error('Exception when checking FITS header %s: %s' % (cmd, str(ex)))
        return 1
    
    if (0 == re[0]):
        a = re[1].find("XTENSION= 'BINTABLE'")
        if (a > -1):
            return 1 # if the file is already compressed, do not add again
        else:
            info(3, "File %s added to be compressed" % filename)
            return 0 
    else:
        warning('Fail to check header for file %s: %s' % (filename, re[1]))
        return 1

def getFileCRC(filename):
    block = "-"
    crc = 0
    blockSize = 1048576 # 1M block size
    fdIn = open(filename)
    while (block != ""):
        block = fdIn.read(blockSize)
        crc = binascii.crc32(block, crc)
    fdIn.close()
    return crc

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
    required_params = ['file_path', 'file_id', 'file_version', 'disk_id']
    
    parDic = reqPropsObj.getHttpParsDic()
    for rp in required_params:
        if (not parDic.has_key(rp)):
            errMsg = 'Parameter missing: %s' % rp
            srvObj.httpReply(reqPropsObj, httpRef, 500, errMsg, NGAMS_TEXT_MT)
            return
        
    filename = reqPropsObj.getHttpPar('file_path')
    info(3, 'Compressing file %s' % filename)
    if (not os.path.exists(filename)):
        errMsg = 'File Not found: %s' % filename
        error(errMsg)
        srvObj.httpReply(reqPropsObj, httpRef, 404, errMsg, NGAMS_TEXT_MT)
        return
        
    fileId = parDic['file_id']
    fileVersion = int(parDic['file_version'])
    diskId = parDic['disk_id']
    
    if (not isMWAVisFile(fileId)):
        errMsg = 'Not MWA visibilty file: %' % (fileId)
        warning(errMsg)
        srvObj.httpReply(reqPropsObj, httpRef, 500, errMsg, NGAMS_TEXT_MT)
        return
    
    if (hasCompressed(filename)):
        errMsg = 'OK'
        info(3, 'File compressed %s already, return now' % (filename))
        srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, errMsg, NGAMS_TEXT_MT)
        return
        
    if (parDic.has_key('scale_factor')):
        sf = int(parDic['scale_factor'])
    else:
        sf = 4
        
    if (parDic.has_key('threshold')):
        th = float(parDic['threshold']) # currently not used
    
    if (parDic.has_key('bins')):
        bins = int(parDic['bins'])
    else:
        bins = 0
        
    if (parDic.has_key('remove_uc')):
        remove_uc = int(parDic['remove_uc']) # currently not used
        
    if (parDic.has_key('timeout')):
        timeout = int(parDic['timeout'])
        if (timeout <= 0):
            timeout = 600
    
    if (parDic.has_key('debug')):
        debug = int(parDic['debug'])
    else:
        debug = 0
    
    if (bins):
        binstr = '-h %d' % bins
    else:
        binstr = ''
    bname = os.path.basename(filename)
    newfn = '%s/%s' % (work_dir, bname)
    fndir = os.path.dirname(filename)
    old_fs = getFileSize(filename)
    
    # do compression
    cmd = "%s -d %d %s %s %s" % (uvcompress, sf, binstr, filename, newfn)
    info(3, 'Compression: %s' % cmd)
    if (debug):
        pass
    else:
        ret = execCmd(cmd)
        if (ret[0] != 0):
            errMsg = 'Failed to compress %s' % ret[1]
            error(errMsg)
            srvObj.httpReply(reqPropsObj, httpRef, 500, errMsg, NGAMS_TEXT_MT)
            return               
        
    # calculate the CRC code
    info(3, 'Calculating CRC for file %s' % newfn)
    if (debug):
        crc = 1234567
    else:
        try:
            crc = getFileCRC(newfn)
        except Exception, exp:
            errMsg = 'Failed to calculate the CRC for file %s: %s' % (newfn, str(exp))
            error(errMsg)
            srvObj.httpReply(reqPropsObj, httpRef, 500, errMsg, NGAMS_TEXT_MT)
            # remove the temp compressed file 
            cmd1 = 'rm %s' % newfn
            execCmd(cmd1)
            return
    
    # rename the uncompressed file (under the same directory)
    cmd1 = 'mv %s %s/%s_uncompressed' % (filename, fndir, bname)
    info(3, 'Renaming the uncompressed file: %s' % cmd1)
    if (debug):
        pass
    else:
        ret = execCmd(cmd1)
        if (ret[0] != 0):
            errMsg = 'Failed to move/rename uncompressed file %s: %s' % (filename, ret[1])
            error(errMsg)
            srvObj.httpReply(reqPropsObj, httpRef, 500, errMsg, NGAMS_TEXT_MT)
            # remove the temp compressed file 
            cmd1 = 'rm %s' % newfn
            execCmd(cmd1)
            return
    
    # move the compressed file to replace the uncompressed file
    cmd2 = 'mv %s %s' % (newfn, filename)
    info(3, 'Moving the compressed file to NGAS volume: %s' % cmd2)
    if (debug):
        pass
    else:
        ret = execCmd(cmd2)
        if (ret[0] != 0):
            errMsg = 'Failed to move the compressed file to NGAS volume: %s' % ret[1]
            error(errMsg)
            srvObj.httpReply(reqPropsObj, httpRef, 500, errMsg, NGAMS_TEXT_MT)
            # remove the temp compressed file 
            cmd2 = 'rm %s' % newfn
            execCmd(cmd2)
            # move the uncompressed file back
            cmd2 = 'mv %s/%s_uncompressed %s' % (fndir, bname, filename)
            ret = execCmd(cmd2)
            if (ret[0] != 0):
                error('Fail to recover from a failed compression: %s' % cmd2)
            return
    
    # change the CRC in the database
    new_fs = getFileSize(filename)
    query = "UPDATE ngas_files SET checksum = '%d', file_size = %d, compression = 'RICE', format = 'application/fits' WHERE file_id = '%s' AND disk_id = '%s' AND file_version = %d" % (crc, new_fs, fileId, diskId, fileVersion)
    info(3, "Updating CRC SQL: %s" % str(query))
    if (debug):
        pass
    else:
        try:
            res = srvObj.getDb().query(query, maxRetries = 1, retryWait = 0)
        except Exception, ex:
            errMsg = 'Fail to update crc for file %s/%d/%s: %s' % (fileId, fileVersion, diskId, str(ex))
            error(errMsg)
            srvObj.httpReply(reqPropsObj, httpRef, 500, errMsg, NGAMS_TEXT_MT)
            # remove the compressed file that have been copied in 
            cmd2 = 'rm %s' % filename
            execCmd(cmd2)
            # recover the uncompressed back
            cmd2 = 'mv %s/%s_uncompressed %s' % (fndir, bname, filename)
            ret = execCmd(cmd2)
            if (ret[0] != 0):
                error('Fail to recover from a failed compression: %s' % cmd2)
            return
    
    # remove the uncompressed file
    errMsg = 'Compression completed %s.' % (filename)
    if (not debug):
        #new_fs = getFileSize(filename)
        cmd3 = 'rm %s/%s_uncompressed' % (fndir, bname)
        ret = execCmd(cmd3)
        if (ret[0] != 0):
            errMsg += '. But fail to remove the uncompressed file %s/%s: %s' % (fndir, bname, ret[1])
            warning(errMsg)
        info(3, errMsg + '\. Result: %d - %d = %d, compress ratio: %.2f' % (old_fs, new_fs, (old_fs - new_fs), new_fs * 1.0 / old_fs))
    else:
        info(3, errMsg)
    srvObj.httpReply(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, errMsg + '\n', NGAMS_TEXT_MT)
        