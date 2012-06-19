

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
# "@(#) $Id: ngasCheckMissingFiles.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  10/08/2001  Created (from script from AWI).
#

"""
Tool to check for missing WFI frames archived in the NGAS Archive compared
to the WFI frame registered in the OLAS DB.

NOTE: This tool is obsolete. This only takes WFI files from 2001-07-15
      into account. Probably this tool can be suppressed.
"""

import sys
import pcc, PccUtString
from   ngams import *
import ngamsLib, ngamsDb


def correctUsage():
    """
    Print correct tool usage on stdout.

    Returns:   Void.
    """
    print "\n% ngasCheckMissingFiles <exempt file list>\n"


def loadExemptFileList(fileName):
    """
    Load the file with File IDs to be ignored.

    Returns:  List with File IDs to be ignored (list/string).
    """
    fo = open(fileName)
    lines = fo.readlines()
    exemptList = []
    for line in lines:
        line = PccUtString.trimString(line, " \n\t")
        if (line != ""):
            exemptList.append(line)
    return exemptList


if __name__ == '__main__':
    """
    Main program invoking the install() function.
    """
    if (len(sys.argv) != 2):
        correctUsage()
        sys.exit()
    exemptList = loadExemptFileList(sys.argv[1])

    db = ngamsDb.ngamsDb("ESOECF", "ngas", "ngas", "bmdhc19wdw==", 0)

    msg = "\nFILES MISSING IN NGAS DB:"
    msg = msg + "\n(message automatically generated)\n"

    # First frame.
    query = "SELECT MIN(file_id) FROM ngas_files WHERE file_id LIKE 'WFI%'"
    res = db.query(query)
    msg = msg + "\nFirst frame: " + res[0][0][0]

    # Last frame.
    query = "SELECT MAX(file_id) FROM ngas_files WHERE file_id LIKE 'WFI%'"
    res = db.query(query)
    msg = msg + "\nLast frame:  " + res[0][0][0]

    # Find the missing frames.
    minFileIdQuery = "SELECT MIN(file_id) FROM ngas_files WHERE " +\
                     "file_id > 'WFI.2001-06%'"
    res = db.query(minFileIdQuery)
    minFileId = res[0][0][0]
    misFrameQuery = "SELECT dp_id FROM observations..data_products dp " +\
                    "WHERE  NOT EXISTS (SELECT n.file_id FROM ngas_files n " +\
                    "WHERE file_id LIKE 'WFI.%' and n.file_id = dp.dp_id) " +\
                    "AND dp.dp_id > 'WFI.2001-06%' AND dp.dp_id > '" +\
                    minFileId + "'"
    res = db.query(misFrameQuery)

    # Check for missing files.
    msg = msg + "\n\nMissing files:"
    count = 0
    for el in res[0]:
        file = el[0]
        if (exemptList.count(file) == 0):
            msg = msg + "\n" + file
            count = count + 1
    if (not count): msg = msg + "\nNone"

    # Send an email if files are missing.
    if (count > 0):
        ngamsLib.sendEmail("smtphost.hq.eso.org",
                           "NGAS DB: Files Missing (GENERAL)",
                           ["jknudstr@eso.org"], "ngast@eso.org", msg)

# EOF
