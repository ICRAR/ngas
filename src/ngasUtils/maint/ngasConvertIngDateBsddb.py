

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
# "@(#) $Id: ngasConvertIngDateBsddb.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  31/08/2004  Created
#

"""
The script is used to remove an entry from a BSDDB.
"""

import sys, bsddb, cPickle
import Sybase


def correctUsage():
    """
    Print out correct usage of the tool on stdout.

    Returns:   Void.
    """    
    print "\nCorrect usage is: "
    print "\n  % ngasConvertIngDateBsddb.py.py <Snapshot Filename>\n\n"
    

def converIngDateBsddb(dbSnapshotName):
    """
    Convert ingestion date from time stamp to ISO-8601.

    dbSnapshotName:     Name of DB Snapshot (string).

    Returns:            Void.
    """
    db = bsddb.hashopen(dbSnapshotName, "w")
    dbKeys = db.keys()

    # First get mapping.
    nm2IdDic = {}
    id2NmDic = {}
    for key in dbKeys:
        if (key.find("___NM2ID___") == 0):
            nm2IdDic[key.split("___")[-1]] = cPickle.loads(db[key])
        elif (key.find("___ID2NM___") == 0):
            id2NmDic[int(key.split("___")[-1])] = cPickle.loads(db[key])
    print nm2IdDic

    # Now convert the Ingestion Date entries.
    ingDateIdx = nm2IdDic["ingestion_date"]
    creDateIdx = nm2IdDic["creation_date"]
    print ingDateIdx
    for key in dbKeys:
        if (key.find("___") == 0): continue
        fileInfoDic = cPickle.loads(db[key])
        ingDateStr = str(fileInfoDic[ingDateIdx])
        ingDateStr = ingDateStr[0:10] + "T" + ingDateStr[11:]
        fileInfoDic[ingDateIdx] = ingDateStr
        if (fileInfoDic[creDateIdx] == None):
            fileInfoDic[creDateIdx] = ingDateStr
        db[key] = cPickle.dumps(fileInfoDic, 1)

    db.close()


if __name__ == '__main__':
    """
    Main function invoking the function to convert the ingestion date.
    """  
    if (len(sys.argv) != 2):
        correctUsage()
        sys.exit(1)
    converIngDateBsddb(sys.argv[1])


# EOF
