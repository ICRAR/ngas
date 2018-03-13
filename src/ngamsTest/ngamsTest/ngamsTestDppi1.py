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

import subprocess

from ngamsLib import ngamsDppiStatus, ngamsPlugInApi
from ngamsLib.ngamsCore import NGAMS_PROC_FILE, NGAMS_PROC_DATA


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
    subprocess.check_call(['gunzip', procFile])
    procFile = procFile[0:procFile.rfind(".")]

    # dpallot: fold fails miserably on Mac when dealing with binary files
    #
    # Process the output file.
    #stat, out = commands.getstatusoutput("fold %s" % procFile)
    #if stat:
    #    raise Exception('Problem while folding %s: %s' % (procFile, out))

    head = []
    with open(procFile, 'rb') as f:
        while True:
            line = f.read(80)
            assert(line and len(line) == 80)
            head.append(line)
            head.append(b'\n')
            if b'END' in line:
                break

    mimeType = "TEST-MIME-TYPE"
    rawPiPars = srvObj.getCfg().dppi_plugins["ngamsTest.ngamsTestDppi1"].pars
    cfgParDic = ngamsPlugInApi.parseRawPlugInPars(rawPiPars)

    head.append(b"\n\nConfiguration Parameters:\n")
    parList = list(cfgParDic)
    parList.sort()
    for par in parList:
        head.append("%s=%s\n" % (par, cfgParDic[par]))

    head.append(b"\nParameters Transferred:\n")
    httpParsDic = reqPropsObj.getHttpParsDic()
    httpPars = list(httpParsDic)
    httpPars.sort()
    for httpPar in httpPars:
        head.append("%s=%s\n" % (httpPar,httpParsDic[httpPar]))
    head.append(b"\nEOF\n")

    buf = b''.join(head)
    # Generate status.
    if (cfgParDic["TARGET"] == "FILE"):
        outFile = procFile + "_ngamsTestDppi1"
        with open(outFile, "ab") as fo:
            fo.write(buf)
        resObj = ngamsDppiStatus.ngamsDppiResult(NGAMS_PROC_FILE, mimeType,
                                                 outFile, filename, procDir)
    else:
        resObj = ngamsDppiStatus.ngamsDppiResult(NGAMS_PROC_DATA, mimeType,
                                                 buf, filename)
    statusObj.addResult(resObj)
    return statusObj

# EOF
