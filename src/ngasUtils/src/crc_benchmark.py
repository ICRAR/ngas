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

from ngamsServer import ngamsFileUtils


size_mb = 128
data = ' ' * 1024 * 1024 * size_mb

for variant, bufsize_log2 in itertools.product(('crc32', 'crc32c'), range(9, 17)):

    info = ngamsFileUtils.get_checksum_info(variant)
    if not info:
        print("Variant not supported: %s" % variant)
        continue

    crc = info.init
    crc_m = info.method
    f = io.BytesIO(data)

    bufsize = 2 ** bufsize_log2
    start = time.time()
    while True:
        buff = f.read(bufsize)
        if not buff:
            break
        crc = crc_m(buff, crc)
    end = time.time()

    print("%6s %-5d %.3f" % (variant, bufsize, size_mb / (end - start)))