#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      1/July/2014  Created

"""
Install the crc32c module, also check the sse availability (using ctype)
if the platform does not support sse4.2 then bail out
"""

import os, inspect, sys
from ctypes import cdll

currFilePath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
libfile = '%s/libchecksse42.so' % currFilePath
if (not os.path.exists(libfile)):
    print "Cannot check SSE4.2 availability, library '%s' cannot be found" % libfile
    sys.exit(1)

lib = cdll.LoadLibrary(libfile)
if (not lib.crc32c_intel_probe()):
    print "Platform does not support SSE4.2 instruction set"
    sys.exit(1)

from distutils.core import setup, Extension

module1 = Extension('crc32c',
                    sources = ['crc32c.c'])

setup (name = 'PackageName',
       version = '1.0',
       description = 'This is a CRC32C package using Intel SSE4.2 instruction set',
       ext_modules = [module1])
