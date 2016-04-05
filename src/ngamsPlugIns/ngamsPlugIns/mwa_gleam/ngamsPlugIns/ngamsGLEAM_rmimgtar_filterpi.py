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
1 Check if it is imgtar (e.g. 1077914808_images.tar)
2 Check if the imgtar file has been untarred successfully
"""

from ngamsLib.ngamsCore import info


sql_query = "SELECT status FROM ngas_subscr_queue WHERE"\
            + " subscr_id = 'STORE04_UNTAR_IMAGE' AND file_id = {0}"\
            + " AND file_version = {1} AND disk_id = 'b66b9398e32632132b298311f838f752'"

def ngamsGLEAM_rmimgtar_filterpi(srvObj,
                          plugInPars,
                          filename,
                          fileId,
                          fileVersion = -1,
                          reqPropsObj = None):

    """
    srvObj:        Reference to NG/AMS Server Object (ngamsServer).

    plugInPars:    Parameters to take into account for the plug-in
                   execution (string).

    fileId:        File ID for file to test (string).

    filename:      Filename of (complete) (string).

    fileVersion:   Version of file to test (integer).

    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).

    Returns:       0 if the file does not match, 1 if it matches the
                   conditions (integer/0|1).
    """
    parts = fileId.split("_")
    if (2 != len(parts)):
        return 0
    if (len(parts[0]) == 10 and "images.tar" == parts[1]):
        info(3, "RMIMGTAR - Executing: " + sql_query)
        res = srvObj.getDb().query2(sql_query, args=(fileId, fileVersion))
        if not res:
            return 0 # not even in the queue, do not remove it
        status = res[0][0] # the first colummn at the first record
        if (0 == int(status)):
            #info("RMIMGTAR - Filter returns true {0}/{1}".format(fileId, fileVersion))
            return 1 # untar is done successfully already
        else:
            #info("RMIMGTAR - Filter returns false {0}/{1}".format(fileId, fileVersion))
            return 0 # untar is not done yet or done but with exceptions
    else:
        return 0

