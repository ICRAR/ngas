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
# "@(#) $Id: ngamsGenericPlugInLib.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  26/02/2007  Created.
#

"""
Contains various utilities for building NGAS Plug-Ins.
"""

import base64

from   ngams import *
import ngamsPlugInApi


NGAS_VOL_INFO_FILE      = ".ngas_volume_info"
NGAS_VOL_INFO_IGNORE    = "IGNORE"
NGAS_VOL_INFO_UNDEF     = "UNDEFINED"
NGAS_VOL_INFO_UNDEF_NO  = -1

# Parameters.
NGAS_VOL_INFO_ID        = "DiskId"
NGAS_VOL_INFO_TYPE      = "Type"
NGAS_VOL_INFO_MANUFACT  = "Manufacturer"


def writeVolInfoFile(volInfoFile,
                     dic):
    """
    Write the parameters in the dictionary into the given Volume Info File.

    Note: If a Volume Info File of that name exists, the existing one is first
    loaded and updated with the new parameters.

    volInfoFile:   Path of Volume Info File (string).

    dic:           Dictionary with parameters (dictionary).

    Returns:       Void.
    """
    T = TRACE()

    if (os.path.exists(volInfoFile)):
        newDic = loadVolInfoFile(volInfoFile)
    else:
        newDic = {}

    # Now merge the new parameters into the new dictionary.
    pars = dic.keys()
    for par in pars:
        newDic[par] = dic[par]

    # Build up the new Volume Info File.
    pars = newDic.keys()
    pars.sort()
    volInfoBuf = ""
    for par in pars:
        volInfoBuf += "%s = %s\n" % (par, newDic[par])

    # Encode the Volume Info File contents (to make it difficult/less tempting
    # for someone to hack ...).
    volInfoBuf = base64.encodestring(volInfoBuf)

    fo = open(volInfoFile, "w+")
    fo.write(volInfoBuf)
    fo.close()


def loadVolInfoFile(volInfoFile):
    """
    Load the contents of a NGAS Volume Info file and return a dictionary
    with the parameters.

    volInfoFile:   Path of Volume Info File (string).

    Returns:       Dictionary with the parameters of the Volume Info File
                   (dictionary).
    """
    T = TRACE()

    fo = open(volInfoFile, "r")
    volInfoBuf = base64.decodestring(fo.read())
    fo.close()
    dic = {}
    for line in volInfoBuf.split("\n"):
        line = line.strip()
        if ((line == "") or (line[0] == "#")): continue
        par, val = line.split("=")
        dic[par.strip()] = val.strip()
    
    return dic
    

if __name__ == '__main__':
    """
    Main function.
    """
    pass


# EOF
