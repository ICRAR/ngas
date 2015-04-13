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
# cwu      30/March/2015  Created

"""
Update GLEAM image values (due to wrong scales used)
"""

import os, commands, binascii
from ngams import *

host_name = os.uname()[1].split('.')[0]
python_path = {"store02":"/usr/local/share/gleamenv/bin/python",
"store04":"/home/ngas/pyws/bin/python"}
py = python_path[host_name]
ngas_path = {"store02":"/home/ngas/ngas_gleam",
"store04":"/home/ngas/ngas_rt"}
py_exec = "{0}/src/ngamsPlugIns/mwa_gleam/update_img_val.py".format(ngas_path[host_name])
tmp_path = "/tmp"

archive_host = getIpAddress()
sql_crc = "UPDATE ngas_files SET checksum = '{0}' WHERE file_id = '{1}' AND file_version = {2} AND disk_id = '{3}'"

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

def execCmd(cmd, failonerror = True):
    re = commands.getstatusoutput(cmd)
    if (failonerror and (not os.WIFEXITED(re[0]))):
        errMsg = 'Fail to execute command: "%s". Exception: %s' % (cmd, re[1])
        raise Exception(errMsg)
    return re

def ngamsGLEAM_uimg_jobpi(srvObj,
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
    """
    1. run the update_img_val.py to produce output at /tmp/out.fits
    2. update CRC and replace the current file
    3. clean up both outfilename
    """
    # 1. execute the update function
    outfilename = "{0}/{2}_{1}".format(tmp_path, fileId, fileVersion)
    cmd = "{0} {1} {2} {3}".format(py, py_exec, filename, outfilename)
    info(3, "IMUPDATE - Executing: " + cmd)
    re = execCmd(cmd)

    # 2. update CRC and replace the current file
    progress = 0
    try:
        info(3, 'IMUPDATE - Executing: Getting crc from {0}'.format(outfilename))
        crc = getFileCRC(outfilename)
        query = sql_crc.format(crc, fileId, fileVersion, diskId)
        info(3, "IMUPDATE - Executing: " + query)
        srvObj.getDb().query(query, maxRetries=1, retryWait=0)
        cmd_cp = "cp {0} {1}".format(outfilename, filename)
        info(3, "IMUPDATE - Executing: " + cmd_cp)
        execCmd(cmd_cp)
        return (0, 'OK')
    except Exception, ex:
        msg = "Updating CRC and replacing file {0} failed: {1}".format(outfilename, ex)
        error(msg)
        return (500, msg)
    else:
        pass
    finally:
        # 3 clean up
        if (os.path.exists(outfilename)):
            os.remove(outfilename)