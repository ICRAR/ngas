#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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

import io
import itertools
import time
import sys

from ngamsServer import ngamsFileUtils

if len(sys.argv) > 1:
    fname = sys.argv[1]
    with open(fname, 'rb') as f:
        data = f.read()
        size_mb = len(data) / 1024. / 1024.
else:
    size_mb = 128
    data = ' ' * 1024 * 1024 * size_mb

for variant, bufsize_log2 in itertools.product(('crc32', 'crc32c'), range(9, 21)):

    f = io.BytesIO(data)

    bufsize = 2 ** bufsize_log2
    start = time.time()
    crc = ngamsFileUtils.get_checksum(bufsize, f, variant)
    end = time.time()

    if crc is None:
        print("Variant not supported: %s" % variant)
    else:
        print("%-6s %08x %-7d %-.3f [MB/s]" % (variant, crc & 0xffffffff, bufsize, size_mb / (end - start)))
