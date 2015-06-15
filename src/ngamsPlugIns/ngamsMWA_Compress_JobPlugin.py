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
# cwu      29/05/2014  Created
"""
Compression job plugin that will be called
by the SubscriptionThread._deliveryThread
"""
# decoded job uri: 
#     ngasjob://ngamsMWA_Compress_JobPlugin?redo_on_fail=0&plugin_params=scale_factor=4,threshold=1E-5,bins=30,remove_uc=1
# originally encoded joburi (during subscribe command)
#     url=ngasjob://ngamsMWA_Compress_JobPlugin%3Fredo_on_fail%3D0%26plugin_params%3Dscale_factor%3D4%2Cthreshold%3D1E-5%2Cbins%3D30%2Cremove_uc%3D1

import commands, os

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import getHostId, info, error, warning


debug = 1
work_dir = '/tmp'
uvcompress = '/home/ngas/processing/compression/uvcompress'
archive_client = '/home/ngas/ngas_rt/bin/ngamsCClient'
archive_host = getHostId().split(':')[0] # archive host must be on the same machine as the  data mover or job runner

def execCmd(cmd, failonerror = True, okErr = []):
    re = commands.getstatusoutput(cmd)
    if (re[0] != 0 and not (re[0] in okErr)):
        errMsg = 'Fail to execute command: "%s". Exception: %s' % (cmd, re[1])
        if (failonerror):
            raise Exception(errMsg)
        else:
            print errMsg
    return re

def ngamsMWA_Compress_JobPlugin(srvObj,
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
    sf = 4 # scaling factor
    th = 1E-5 # threshold
    bins = 0
    remove_uc = 0
    if ((plugInPars != "") and (plugInPars != None)):
        pars = plugInPars
    
    parDic = ngamsPlugInApi.parseRawPlugInPars(pars)
            
    if (parDic.has_key('scale_factor')):
        sf = int(parDic['scale_factor'])
    
    if (parDic.has_key('threshold')):
        th = float(parDic['threshold'])
    
    if (parDic.has_key('bins')):
        bins = int(parDic['bins'])
        
    if (parDic.has_key('remove_uc')):
        remove_uc = int(parDic['remove_uc'])
    
    if (bins):
        binstr = '-h %d' % bins
    else:
        binstr = ''
    
    newfn = '%s/%s' % (work_dir, os.path.basename(filename))
    
    cmd = "%s -d %d %s %s %s" % (uvcompress, sf, binstr, filename, newfn)
    cmd1 = "%s -host %s -port 7777 -fileUri %s -cmd QARCHIVE -mimeType application/octet-stream " % (archive_client, archive_host, newfn)
    cmd2 = "curl http://%s:7777/DISCARD?file_id=%s\\&file_version=%d\\&disk_id=%s\\&execute=1" % (archive_host, fileId, fileVersion, diskId)
    cmd3 = "rm %s" % newfn
    
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
        re = commands.getstatusoutput(cmd)
        if (0 == re[0]):
            if (not bins):
                retstr = re[1].split('\n')[-1] # just get the elapsed time
            else:
                retstr = re[1].split('------ Histogram ------\n')[1]
            
            # archive it back
            # TODO - enable time out!!
            re = commands.getstatusoutput(cmd1)
            if (0 == re[0]):
                info(3, 'Successfully re-archived the compressed file %s' % newfn)
            else:
                error('Fail to re-archive compressed file %s: %s' % (newfn, re[1]))
                return (re[0], re[1])
            
            if (remove_uc):
                # remove the uncompressed file if necessary
                re = commands.getstatusoutput(cmd2)
                if (0 == re[0]):
                    info(3, 'Successfully DISCARDED the uncompressed file %s' % filename)
                else:
                    warning('Fail to DISCARD the uncompressed file %s' % filename)
            
            # remove the temp file
            re = commands.getstatusoutput(cmd3)
            if (0 != re[0]):
                warning('Fail to remove the temp compressed file %s' % newfn)
            
            return (0, retstr)
            
        else:
            error('Fail to compress file %s: %s' % (filename, re[1]))
            return (re[0], re[1])
    
    
    