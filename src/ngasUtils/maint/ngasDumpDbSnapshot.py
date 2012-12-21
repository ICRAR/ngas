

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
# "@(#) $Id: ngasDumpDbSnapshot.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/10/2003  Created
#

"""
The script is used to dump the contents of an NG/AMS DB Snapshot File
in a human readible (ASCII) format.
"""

import sys, bsddb, cPickle

try:
    from ngams import *
except:
    pass
try:
    import Sybase
except:
    pass

def correctUsage():
    """
    Print out correct usage of the tool on stdout.

    Returns:   Void.
    """    
    print "\nCorrect usage is: "
    print "\n  % ngasDumpDbSnapshot.py <Snapshot Filename>\n\n"
    

def dumpDbSnapshot(dbSnapshotName,
                   details = 0):
    """
    Dump the contents of a DB Snapshot in a raw format.

    dbSnapshotName:     Name of DB Snapshot (string).

    details:            Dump details (integer/0|1).

    Returns:            Void.
    """
    db = bsddb.hashopen(sys.argv[1], "r")
    print "\nDumping contents of NG/AMS DB Snapshot: " + sys.argv[1]

    try:
        key, pickleVal = db.first()
    except:
        key = None
    print ""
    while (key):
        try:
            val = cPickle.loads(pickleVal)
        except Exception, e:
            tstStr = "No module named"
            if (str(e).find(tstStr) != -1):
                modName = str(e).split(tstStr)[-1].strip()
                exec "import %s" % modName
                val = cPickle.loads(pickleVal)
            else:
                raise Exception, e
        # Try if the object has a dumpBuf() method.
        try:
            dumpBuf = val.dumpBuf()
        except:
            dumpBuf = None
        print "%s = %s" % (key, str(val))
        if (details and dumpBuf): print dumpBuf
        try:
            key, pickleVal = db.next()
        except Exception, e:
            break
    db.close()


if __name__ == '__main__':
    """
    Main function invoking the function to dump the DB Snapshot.
    """  
    if (len(sys.argv) != 2):
        correctUsage()
        sys.exit(1)
    dumpDbSnapshot(sys.argv[1], 1)
    

# EOF
