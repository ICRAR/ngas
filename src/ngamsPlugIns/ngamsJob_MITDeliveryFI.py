
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
# cwu      07/Aug/2014  Created

"""
This filter will keep  all already-compressed FITS file from entering the job queue
"""

import os

obswanted = {}

#obswanted['1096147944'] = 1
#obswanted['1096147824'] = 1
#obswanted['1096147696'] = 1
#obswanted['1096147576'] = 1
obswanted['1099315640'] = 1
#obswanted['1096147336'] = 1
#obswanted['1096147216'] = 1
#obswanted['1096147088'] = 1
obswanted['1099315760'] = 1
obswanted['1099315888'] = 1
#obswanted['1096146848'] = 1 #2014-10-01T05:16:05.661    

def isWanted(fileId):
    #if (not fileId.endswith('.fits')):
    #if (not fileId.endswith('_flags.zip')):
    #    return 0    
    k = fileId.split('_')[0]
    #return obswanted.has_key(k)
    try:
        intk = int(k)
        return (intk >= 1099306200 and intk <= 1099339800)
    except:
        return False
    
    
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
    

def ngamsJob_MITDeliveryFI(srvObj,
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
    return isWanted(fileId)
    
    