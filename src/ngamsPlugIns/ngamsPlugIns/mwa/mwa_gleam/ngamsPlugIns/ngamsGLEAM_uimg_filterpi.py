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

import re

from ngamsLib.ngamsCore import warning


done_list = ['2:1061472160_072-080MHz_XX_r-1.0_v1.0.fits',
'2:1061472160_072-080MHz_XX_r0.0_v1.0.fits',
'2:1061472160_072-080MHz_YY_r-1.0_v1.0.fits']

def isGLEAMImage(fileId):
    """
    This is brittle, but works for now
    """
    return (fileId.lower().endswith('.fits') and
            (len(fileId.split('_')) == 5) and
            (fileId.find('mosaic') == -1))

def ngamsGLEAM_uimg_filterpi(srvObj,
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
    if (not isGLEAMImage(fileId)):
        return 0
    obs_id = None
    try:
        obs_id = int(re.split("_", fileId)[0])
    except ValueError, ve:
        warning("file {} is not a proper image".format(fileId))
        return 0

    if (obs_id is None):
        return 0

    if (not (1061471500 < obs_id < 1061481000)):
        return 0

    if ("{0}:{1}".format(fileVersion, obs_id) in done_list):
        return 0

    """
    if not srvObj.getDb().isLastVersion(fileId, fileVersion):
        return 0
    """
    return 1

