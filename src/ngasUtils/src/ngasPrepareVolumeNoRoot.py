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
# "@(#) $Id: ngasPrepareVolume.py,v 1.4 2008/12/12 14:28:33 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  26/02/2007  Created
# cwu       08/05/2013  Ported
#

_doc =\
"""
The ngasPrepareVolume Tool is used to prepare a 'HW independent' NGAS
volume. The volume must be mounted already. The tool will generate a file,
NGAS Volume Info File (<Volume Path>/.ngas_volume_info), which contains the
relevant parameters for the volume.

The name of the path for the volume, should be of the type:

<NGAS Root>/<Volumes Directory>/<Volume Name>

- whereby <NGAS Root> is the value of the Server.RootDirectory from the
NGAS configuration, and the <Volumes Directory> the value of the
Server.VolumesDiretory.

The tool will only accept to be executed as user root. The resulting
NGAS Volume Info File will be owned by user root and will be read-only for
all.

%s

"""

import ngasPrepareVolume, sys

if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    try:
        #if (os.getuid() != 0):
            #raise Exception, "This tool must be executed as user root!"
        optDic = ngasPrepareVolume.parseCmdLine(sys.argv, ngasPrepareVolume.getOptDic())
    except Exception, e:
        print "\nProblem executing the tool:\n\n%s\n" % str(e)
        sys.exit(1)
    ngasPrepareVolume.setLogCond(0, "", 0, "", 1)
    ngasPrepareVolume.execute(optDic)

# EOF
