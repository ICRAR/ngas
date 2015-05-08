#
#    (c) University of Western Australia
#    International Centre of Radio Astronomy Research
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
#    Copyright by UWA,
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
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      23/Sep/2014  Created

"""
Decompression job plugin that will be called
by the SubscriptionThread._deliveryThread
"""

import commands, os
from glob import glob

from ngams import *
import ngamsPlugInApi

# decoded job uri: 
#     ngasjob://ngamsGLEAM_Decompress_JobPlugin?redo_on_fail=0

# originally encoded joburi (during subscribe command)
#     url=ngasjob://ngamsGLEAM_Decompress_JobPlugin%3Fredo_on_fail%3D0


debug = 0
work_dir = '/mnt/gleam2/tmp'
#uvcompress = '/home/ngas/processing/compression/uvcompress'
#uvcompress = '/Users/chen/processing/compression/uvcompress'

archive_client = '/home/ngas/ngas_rt/bin/ngamsCClient'
#archive_client = '/Users/chen/proj/ngas_buildout/bin/ngamsCClient'

#archive_host = getHostId().split(':')[0] # archive host must be on the same machine as the  data mover or job runner
archive_host = getIpAddress()

def execCmd(cmd, timeout):
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

def ngamsGLEAM_Decompress_JobPlugin(srvObj,
                          plugInPars,
                          filename,
                          fileId,
                          fileVersion,
                          diskId):
    """
    srvObj:        Reference to NG/AMS Server Object (ngamsServer).

    plugInPars:    Parameters to take into account for the plug-in
                   execution (string).(e.g. scale_factor=4,threshold=1E-5)
   
    fileId:        File ID for file to test (string).

    filename:      Filename of (complete) (string).

    fileVersion:   Version of file to test (integer).
 
    Returns:       the return code of the compression plugin (integer).
    """
    pars = ""
    remove_uc = 0
    timeout = 600 # each command should not run more than 10 min, otherwise something is wrong
    if ((plugInPars != "") and (plugInPars != None)):
        pars = plugInPars
    
    parDic = ngamsPlugInApi.parseRawPlugInPars(pars)     
    
    if (parDic.has_key('remove_uc')):
        remove_uc = int(parDic['remove_uc'])
        
    if (parDic.has_key('timeout')):
        timeout = int(parDic['timeout'])
        if (timeout <= 0):
            timeout = 600
    
    #cmd1 = "%s -host %s -port 7777 -fileUri %s -cmd QARCHIVE -mimeType application/octet-stream " % (archive_client, archive_host, newfn)
    cmd2 = "curl http://%s:7777/DISCARD?file_id=%s\\&file_version=%d\\&disk_id=%s\\&execute=1" % (archive_host, fileId, fileVersion, diskId)
    
    
    """
    if (debug):
        info(3, '*******************************************')
        info(3, cmd)
        info(3, cmd1)
        if (remove_uc):
            info(3, cmd2)
        info(3, cmd3)
        info(3, '*******************************************')
        return (0, 'Compressed OK')
    else:
    """
        #re = commands.getstatusoutput(cmd)
    cmd = "tar xf %s -C %s" % (filename, work_dir)
    info(3, "Extracting %s to %s" % (filename, work_dir))
    re = execCmd(cmd, timeout)
    if (0 == re[0]):
        # archive it back
        obsId = fileId.split('_')[0]
        imglist = glob('%s/%s/*.fits' % (work_dir, obsId))
        errNo = 0
        lasterrMsg = ''
        for imgfile in imglist:
            url = 'http://%s:7777/LARCHIVE?fileUri=%s\&mimeType=application/octet-stream\&file_version=%d\&no_versioning=1\&versioning=0' % (archive_host, imgfile, fileVersion)
            cmd1 = 'curl --connect-timeout %d %s' % (timeout, url)
            info(3, 'Local archiving %s' % cmd1)
            re = commands.getstatusoutput(cmd1)
            if (0 == re[0] and (re[1].count('Successfully handled Archive Pull Request') > 0)):
                info(3, 'Successfully re-archived the untarred FITS file %s' % imgfile)
            else:
                error('Fail to re-archive the untarred FITS file  %s: %s' % (imgfile, re[1]))
                errNo += 1
                lasterrMsg = re[1]
                #return (re[0], re[1])
        
        if (remove_uc and errNo == 0):
            # remove the original file if necessary
            re = execCmd(cmd2, timeout)
            info(3, 'Removing the tar file %s' % filename)
            if (0 == re[0]):
                info(3, 'Successfully DISCARDED the tar file %s' % filename)
            else:
                warning('Fail to DISCARD the tar file %s' % filename)
        
        # remove the temp file
        cmd3 = "rm -rf %s/%s" % (work_dir, obsId)
        info(3, "Removing the temp directory %s/%s" % (work_dir, obsId))
        re = execCmd(cmd3, timeout)
        if (0 != re[0]):
            warning('Fail to remove the temp untarred directory %s/%s' % (work_dir, obsId))
        
        if (errNo == 0):
            return (0, 'Done')
        else:
            return (errNo, lasterrMsg.replace("'", ""))
    else:
        error('Fail to untar file %s: %s' % (filename, re[1]))
        return (re[0], re[1])
    
    
    