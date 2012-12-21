

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
# "@(#) $Id: ngasRemKeyBsddb.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  31/08/2004  Created
#

"""
The script is used to remove an entry from a BSDDB.
"""

import sys, bsddb, cPickle


def correctUsage():
    """
    Print out correct usage of the tool on stdout.

    Returns:   Void.
    """    
    print "\nCorrect usage is: "
    print "\n  % ngasRemKeyBsddb.py <Snapshot Filename> <Key>\n\n"
    

def remKeyBsddb(dbSnapshotName,
                key):
    """
    Remove an entry from a BSDDB referring to the name of a key in the DB.

    dbSnapshotName:     Name of DB Snapshot (string).

    key:                Name of key to remove from the DB Snapshot (string).

    Returns:            Void.
    """
    db = bsddb.hashopen(dbSnapshotName, "w")
    if (db.has_key(key)):
        try:
            del db[key]
            print "Removed key: %s from BSDDB: %s" % (dbSnapshotName, key)
        except Exception, e:
            errMsg = "Problem encountered removing key: %s from BSDDB: %s. " +\
                     "Error: %s" 
            print errMsg % (dbSnapshotName, key, str(e))
    else:
        print "Key: %s not found in BSDDB: %s" % (dbSnapshotName, key)
    db.close()


if __name__ == '__main__':
    """
    Main function invoking the function to dump the DB Snapshot.
    """  
    if (len(sys.argv) != 3):
        correctUsage()
        sys.exit(1)
    remKeyBsddb(sys.argv[1], sys.argv[2])
    

# EOF
