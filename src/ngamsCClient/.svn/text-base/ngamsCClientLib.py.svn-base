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
# "@(#) $Id: ngamsCClientLib.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  11/06/2001  Created


# Small jacket module to make it possible to view the header file for
# the NG/AMS C-API Library.

from ngams import *


def _ngamsGenDocPage(cSrcFile):
    """
    Parse in a C source file code and generate an ASCII man-page from
    comments in the doc++ format.

    cSrcFile:     C source file.

    Returns:      Man-page string buffer.
    """
    fo = open(libSrcFile)

    srcFileLines = fo.readlines()

    docDic   = {}
    docCount = 0
    lineIdx  = 0
    while lineIdx < len(srcFileLines):
        line = srcFileLines[lineIdx]
        if (line.find("/**") != -1):
            # Get the documentation text. Read until */ encountered.
            lineIdx += 1
            docText = ""
            while lineIdx < len(srcFileLines):
                line = srcFileLines[lineIdx]
                if (line.find("*/") != -1):
                    break
                else:
                    docText += line
                lineIdx += 1
            docDic[docCount] = docText
            docCount += 1   
        lineIdx += 1

    docPage = ""
    for docIdx in range(len(docDic.keys())):
        docPage += docDic[docIdx] + "\n\n"

    return docPage

libSrcFile = ngamsGetSrcDir() + "/ngamsCClient/ngamsCClientLib.c"
__doc__ = _ngamsGenDocPage(libSrcFile)


# EOF
