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
# "@(#) $Id: ngamsTileCompress.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/05/2006  Created
#
"""
Command line utility + function to tile compress a FITS file.
For now the 'imcopy' utility of the CFITSIO package is invoked for this.
"""

import sys, commands

from ngamsLib.ngamsCore import info, mvFile, rmFile, setLogCond

def ngamsTileCompress(filename):
    """
    Tile compress the referenced file.

    filename:   Filename (string).

    Returns:    Void.
    """
    tmpFilename = filename + ".tmp"
    try:
        comprCmd = "imcopy %s '%s[compress]'" % (filename, tmpFilename)
        info(3,"Command to tile compress file: %s" % comprCmd)
        stat, out = commands.getstatusoutput(comprCmd)
        if (stat != 0):
            msg = "Error compressing file: %s. Error: %s" %\
                  (filename, stat.replace("\n", "   "))
            raise Exception, msg
        mvFile(tmpFilename, filename)
        info(3,"Successfully tile compressed file: %s" % filename)
    except Exception, e:
        rmFile(tmpFilename)
        raise Exception, e
        

if __name__ == '__main__':
    """
    Main routine to calculate checksum for a file given as input parameter.
    """
    setLogCond(0, "", 0, "", 3)
    if (len(sys.argv) != 2):
        print "\nCorrect usage is:\n"
        print "% ngamsTileCompress.py <filename>\n"
        sys.exit(1)
    ngamsTileCompress(sys.argv[1])


# EOF
