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
# cwu      02/Dec/2014  Created
"""
This job plugin rename all phase 2 measurementset files
The system must already have pigz installed
"""

import commands
import logging
import os
import re
import shutil


logger = logging.getLogger(__name__)

phase2_line = "solutions.bin"
phase2_label = "phase2"
#mount_point = '/home/ngas/NGAS/volume1' #store04
mount_point = '/mnt/gleam/NGAS/volume1' #store02

def execCmd(cmd, failonerror = True):
    re = commands.getstatusoutput(cmd)
    if (failonerror and (not os.WIFEXITED(re[0]))):
        errMsg = 'Fail to execute command: "%s". Exception: %s' % (cmd, re[1])
        raise Exception(errMsg)
    return re

def ngamsGLEAM_Rename_JobPI(srvObj,
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
    idx = filename.find(mount_point)
    if (idx == -1):
        raise Exception('filename %s is not part of the mount_point %s' % (filename, mount_point))

    cmd = "tar -tf %s --use-compress-program=pigz" % filename
    logger.debug(cmd)
    ret = execCmd(cmd)
    obsId = fileId.split('.')[0]
    #sline = '%s/%s' % (obsId, phase2_line)
    sline = r"%s/[\S]*%s" % (obsId, phase2_line) # Sometimes Natasha has to calibrate an observation using another set of solutions
    lines = ret[1].split('\n')
    """
    if (not (sline in lines)):
        return (0, 'No need') # not phase 2 MS file
    """
    found = False
    for line in lines:
        m = re.match(sline, line)
        if (m is not None):
            found = True
            break

    if (not found):
        return (0, 'No need') # not phase 2 MS file

    base_dir = os.path.dirname(filename)
    new_fileId = "%s_%s.tar.gz" % (obsId, phase2_label)

    # change filename on the file system
    shutil.move(filename, "%s/%s" % (base_dir, new_fileId))
    # change DB
    # first, calculate the partial file name
    partial_path = filename[(len(mount_point) + 1):]
    partial_path = partial_path[0:partial_path.find(fileId)] # e.g. afa/2014-03-03/1/

    # then, update table
    sqlquery = "UPDATE ngas_files SET file_id = {0}, file_name = {1}" % (new_fileId, partial_path, new_fileId) +\
               " WHERE file_id = {2} AND file_version = {3} AND disk_id = {4}" % (fileId, fileVersion, diskId)
    srvObj.getDb().query2(sqlquery, args=(new_fileId, "%s%s" % (partial_path, new_fileId), fileId, fileVersion, diskId))
    return (0, 'Done')



