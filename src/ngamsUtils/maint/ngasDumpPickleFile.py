

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
# "@(#) $Id: ngasDumpPickleFile.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/10/2003  Created
#

"""
The script is used to dump the contents of an NG/AMS object pickle file
on stdout in a human readible (ASCII) format.
"""

import sys, cPickle


def correctUsage():
    """
    Print out correct usage of the tool on stdout.

    Returns:   Void.
    """
    print "\nCorrect usage is: "
    print "\n  % ngasDumpPickleFile.py <Pickle Filename>\n\n"


if __name__ == '__main__':
    """
    Main program.
    """
    if (len(sys.argv) != 2):
        correctUsage()
        sys.exit(1)

    obj = cPickle.load(open(sys.argv[1]))

    print "Information about pickle file: " + sys.argv[1]
    objType = type(obj)
    if (str(objType).find("instance") != -1):
        print "\nObject Type: ", str(obj).split(" ")[0][1:]
    else:
        print "\nObject Type: ", objType
    print "\nContents:\n"
    try:
        print obj.dumpBuf()
    except:
        print str(obj)


# EOF
