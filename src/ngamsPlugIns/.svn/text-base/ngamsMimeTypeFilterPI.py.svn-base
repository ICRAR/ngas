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
# "@(#) $Id: ngamsMimeTypeFilterPI.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  21/11/2002  Created
#

"""
Contains a Filter Plug-In used to filter on the mime-type of the data.
"""

from ngams import *
import ngamsPlugInApi


def ngamsMimeTypeFilterPI(srvObj,
                          plugInPars,
                          filename,
                          fileId,
                          fileVersion = -1,
                          reqPropsObj = None):
    """
    Example Filter Plug-In used to filter on a given mime-type. In case the
    file referenced has the mime-type as specified in the plug-in parameters,
    the file being tested is selected.

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
    match = 0

    # Parse plug-in parameters.
    parDic = []
    pars = ""
    if ((plugInPars != "") and (plugInPars != None)):
        pars = plugInPars
    elif (reqPropsObj != None):
        if (reqPropsObj.hasHttpPar("plug_in_pars")):
            pars = reqPropsObj.getHttpPar("plug_in_pars")
    parDic = ngamsPlugInApi.parseRawPlugInPars(pars)
    if (not parDic.has_key("mime_types")):
        errMsg = "ngamsMimeTypeFilterPI: Missing Plug-In Parameter: " +\
                 "mime_types"
        raise Exception, errMsg
    
    # Perform the matching.
    refMimeTypes = parDic["mime_types"].split("|")
    actMimeType = ngamsPlugInApi.determineMimeType(srvObj.getCfg(), filename)
    for mt in refMimeTypes:
        if (actMimeType == mt.strip()): match = 1

    return match


# EOF
