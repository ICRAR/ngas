

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
# "@(#) $Id: ngasChangeDiskIdDbSnapshot.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
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
    print "\n  % ngasChangeDiskIdDbSnapshot.py <Snapshot Filename> " +\
          "<New Disk ID>\n\n"
    

def changeDiskIdDbSnapshot(dbSnapshotName,
                           diskId):
    """
    Update Disk ID in the given DB Snapshot.

    dbSnapshotName:     Name of DB Snapshot (string).

    diskId:             New Disk ID (string).

    Returns:            Void.
    """
    db = bsddb.hashopen(dbSnapshotName, "w")

    # Get index for Disk ID.
    diskIdIdx = cPickle.loads(db["___NM2ID___disk_id"])

    # Loop over contents of DB Snapshot and change the Disk ID.
    dbKeys = db.keys()
    for key in dbKeys:
        if (key.find("___") == 0): continue
        fileInfoDic = cPickle.loads(db[key])
        fileInfoDic[diskIdIdx] = diskId
        db[key] = cPickle.dumps(fileInfoDic, 1)
    db.close()


if __name__ == '__main__':
    """
    Main function invoking the function to convert the ingestion date.
    """  
    if (len(sys.argv) != 3):
        correctUsage()
        sys.exit(1)
    changeDiskIdDbSnapshot(sys.argv[1], sys.argv[2])


# EOF
