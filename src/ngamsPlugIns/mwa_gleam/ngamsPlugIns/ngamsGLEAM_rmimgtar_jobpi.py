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
# cwu      13/April/2015  Created

"""
1. Check if the imgtar file has been untarred successfully (this is now in filter)
If so,
2. Transfer image tar from ICRAR to mwa-process01.ivec.org
3. remove the imgtar file from ICRAR archive

"""
import os, commands
from ngams import *
import ngamsDiscardCmd

ngas_path = "/home/ngas/ngas_rt"
archive_client = '/home/ngas/ngas_rt/bin/ngamsCClient'
archive_host = "mwa-process01.ivec.org"

def ngamsGLEAM_rmimgtar_jobpi(srvObj,
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
    archive_cmd = "%s -host %s -port 7777 -fileUri %s -cmd QARCHIVE -mimeType application/octet-stream " % (archive_client, archive_host, filename)
    re = commands.getstatusoutput(archive_cmd)
    #if (os.WIFEXITED(re[0])):
    if (0 == re[0]):
        info(3, 'Successfully archived file to mwa-process01: %s' % filename)
        work_dir = srvObj.getCfg().getRootDirectory() + '/tmp/'
        try:
            #ngamsDiscardCmd._discardFile(srvObj, diskId, fileId, fileVersion,
            #                             execute=1, tmpFilePat=work_dir)
            cmd2 = "curl --connect-timeout 5 http://store04.icrar.org:7777/DISCARD?file_id=%s\\&file_version=%d\\&disk_id=%s\\&execute=1" % (fileId, fileVersion, diskId)
            re2 = commands.getstatusoutput(cmd2)
            if (0 == re2[0]):
                return(0, 'OK')
            else:
                raise Exception(re2[1].split('\n')[-1])
        except Exception, e1:
            warning("\nFail to discard untar file: {0}/{1}: {2}\n".format(fileId, fileVersion, str(e1)))
            return (500, str(e1))
    else:
        error('Fail to archive file to mwa-process01 %s: %s' % (filename, re[1]))
        return (re[0], re[1])