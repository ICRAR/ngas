#
#    ALMA - Atacama Large Millimiter Array
#    (c) European Southern Observatory, 2002
#    Copyright by ESO (in the framework of the ALMA collaboration),
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
# "@(#) $Id: ngamsEsoArchDppi.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  08/01/2002  Created
#

"""
Contains a DDPI which is used by the ESO Archive Facility to perform the
processing in connection with a standard data request handling.
"""

from ngams import *
import ngamsPlugInApi, ngamsDppiStatus


def ngamsEsoArchDppi(srvObj,
                     reqPropsObj,
                     filename):
    """
    This DPPI performs the processing neccessary for the files
    requested from the ESO Archive (by the Data Requestor).

    srvObj:        Reference to instance of the NG/AMS Server
                   class (ngamsServer).
    
    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).
    
    filename:      Name of file to process (string).

    Returns:       DPPI return status object (ngamsDppiStatus).
    """
    statusObj = ngamsDppiStatus.ngamsDppiStatus()

    # Decompress the file if the last extension is "Z".
    if (filename.split(".")[-1] == "Z"):
        procFilename, procDir = ngamsPlugInApi.prepProcFile(srvObj.getCfg(),
                                                            filename)
        exitCode, stdOut, stdErr = ngamsPlugInApi.\
                                   execCmd("uncompress " + procFilename)
        if (exitCode != 0):
            errMsg = "ngamsEsoArchDppi: Problems during archiving! " +\
                     "Decompressing the file: " + filename + " failed. " +\
                     "Error message: " + str(stdErr)
            raise Exception, errMsg
        resFilename = procFilename[0:-2]
    else:
        resFilename = filename
        procDir = ""
    mimeType = ngamsPlugInApi.determineMimeType(srvObj.getCfg(), resFilename)
    resObj = ngamsDppiStatus.ngamsDppiResult(NGAMS_PROC_FILE, mimeType,
                                             resFilename, resFilename, procDir)
    statusObj.addResult(resObj)

    return statusObj


# EOF
