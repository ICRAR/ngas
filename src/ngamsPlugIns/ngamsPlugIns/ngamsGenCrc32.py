
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
# "@(#) $Id: ngamsGenCrc32.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  23/01/2002  Created
#

"""
Checksum Plug-In to generate the checksum stored in the ngas_files tables
in connection with each file archived into NGAS.
"""

import binascii


def ngamsGenCrc32(srvObj,
                  filename,
                  priority = 0):
    """
    Plug-in to generate CRC-32 checksum for an archived data file.

    srvObj:       Reference to instance of NG/AMS Server class (ngamsServer).

    filename:     Name of file to generate checksum for (string).

    priority:     Is used by NG/AMS to make the plug-in consume less
                  CPU. A value of 0 means highest priority (integer/[0; oo]).

    Returns:      CRC-32 checksum for file (string).
    """

    blocksize = srvObj.getCfg().getBlockSize()

    crc = 0
    with open(filename, 'rb') as fo:
        while True:
            buf = fo.read(blocksize)
            if not buf:
                break
            crc = binascii.crc32(buf, crc)

    return str(crc)