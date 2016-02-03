#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
#    Copyright by UWA (in the framework of the ICRAR)
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
# "@(#) $Id: ngamsTestDppi1.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  09-06-2005  Created
#
"""
Test DPPI to test the functioning of the RETRIEVE Command used in conjuction
with DPPI processing.
"""

import commands

from ngamsLib import ngamsDppiStatus, ngamsPlugInApi
from ngamsLib.ngamsCore import info, NGAMS_PROC_FILE, NGAMS_PROC_DATA


def ngamsTestDppi1(srvObj,
                   reqPropsObj,
                   filename):
    """
    This test DPPI extracts the main header from a FITS file requested from the
    ESO Archive.

    Depending on the the DPPI Plug-In Parameters it stores the result in a file
    or in a buffer in memory.

    This is made to work on small FITS files.

    srvObj:        Reference to instance of the NG/AMS Server
                   class (ngamsServer).
    
    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).
    
    filename:      Name of file to process (string).

    Returns:       DPPI return status object (ngamsDppiStatus).
    """
    statusObj = ngamsDppiStatus.ngamsDppiStatus()

    # Uncompress the file.
    procFile, procDir = ngamsPlugInApi.prepProcFile(srvObj.getCfg(), filename)
    stat, out = commands.getstatusoutput("gunzip %s" % procFile)
    if stat:
        raise Exception('Problem while uncompressing %s: %s' % (procFile, out))
    procFile = procFile[0:procFile.rfind(".")]

    # Process the output file.
    stat, out = commands.getstatusoutput("fold %s" % procFile)
    if stat:
        raise Exception('Problem while folding %s: %s' % (procFile, out))
    head = ""
    for line in out.split("\n"):
        if (line.strip() == ""): continue
        head += line + "\n"
        if (line.strip().find("END") == 0): break
    mimeType = "TEST-MIME-TYPE"
    rawPiPars = srvObj.getCfg().getDppiPars("ngamsTest.ngamsTestDppi1")
    cfgParDic = ngamsPlugInApi.parseRawPlugInPars(rawPiPars)

    head += "\n\nConfiguration Parameters:\n"
    parList = cfgParDic.keys()
    parList.sort()
    for par in parList: head += "%s=%s\n" % (par, cfgParDic[par])

    head += "\nParameters Transferred:\n"
    httpParsDic = reqPropsObj.getHttpParsDic()
    httpPars = httpParsDic.keys()
    httpPars.sort()
    for httpPar in httpPars: head += "%s=%s\n" % (httpPar,httpParsDic[httpPar])
    head += "\nEOF\n"

    # Generate status.
    if (cfgParDic["TARGET"] == "FILE"):
        outFile = procFile + "_ngamsTestDppi1"
        fo = open(outFile, "a")
        fo.write(head)
        fo.close()
        info(3,"Storing result data in file: %s" % outFile)
        resObj = ngamsDppiStatus.ngamsDppiResult(NGAMS_PROC_FILE, mimeType,
                                                 outFile, filename, procDir)
    else:
        info(3,"Keeping result data in buffer")
        resObj = ngamsDppiStatus.ngamsDppiResult(NGAMS_PROC_DATA, mimeType,
                                                 head, filename)
    statusObj.addResult(resObj)
    return statusObj

# EOF
