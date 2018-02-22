#
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
# "@(#) $Id: ngamsCmd_HTTPFETCH.py,v 1.1 2012/11/22 21:48:22 amanning Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jagonzal  2010/17/01  Created
#
"""
NGAS Command Plug-In, implementing an Archive Command specific for Mirroring

This works in a similar way as the 'standard' ARCHIVE Command, but has been
simplified in a few ways:

  - No replication to a Replication Volume is carried out.
  - Target disks are selected randomly, disregarding the Streams/Storage Set
    mappings in the configuration. This means that 'volume load balancing' is
    provided.
  - Archive Proxy Mode is not supported.
  - No probing for storage availability is supported.
  - In general, less SQL queries are performed and the algorithm is more
    light-weight.
  - crc is computed from the incoming stream
  - ngas_files data is 'cloned' from the source file
"""

import contextlib
import logging
import os
import time

from ngamsLib.ngamsCore import checkCreatePath
from ngamsLib import ngamsHttpUtils
from ngamsServer import ngamsFileUtils
from . import ngamsFailedDownloadException


logger = logging.getLogger(__name__)


def saveToFile(srvObj,
                       ngamsCfgObj,
                       reqPropsObj,
                       trgFilename,
                       blockSize,
                       startByte):
    """
    Save the data available on an HTTP channel into the given file.

    ngamsCfgObj:     NG/AMS Configuration object (ngamsConfig).

    reqPropsObj:     NG/AMS Request Properties object (ngamsReqProps).

    trgFilename:     Target name for file where data will be
                     written (string).

    blockSize:       Block size (bytes) to apply when reading the data
                     from the HTTP channel (integer).

    mutexDiskAccess: Require mutual exclusion for disk access (integer).

    diskInfoObj:     Disk info object. Only needed if mutual exclusion
                     is required for disk access (ngamsDiskInfo).

    Returns:         Tuple. Element 0: Time in took to write
                     file (s) (tuple).
    """

    disk_id = reqPropsObj.fileinfo['diskId']
    source_host = reqPropsObj.fileinfo['sourceHost']
    host_id = reqPropsObj.fileinfo['hostId']
    file_version = reqPropsObj.fileinfo['fileVersion']
    file_id = reqPropsObj.fileinfo['fileId']

    host, port = source_host.split(":")
    pars = {
        'disk_id': disk_id,
        'host_id': host_id,
        'quick_location': '1',
        'file_version': file_version,
        'file_id': file_id
    }
    hdrs = {'Range': str(startByte) + '-'}

    rx_timeout = 30 * 60
    if srvObj.getCfg().getVal("Mirroring[1].rx_timeout"):
        rx_timeout = int(srvObj.getCfg().getVal("Mirroring[1].rx_timeout"))
    response = ngamsHttpUtils.httpGet(host, int(port), 'RETRIEVE', pars=pars, hdrs=hdrs, timeout=rx_timeout)

    # can we resume a previous download?
    downloadResumeSupported = 'bytes' in response.getheader("Accept-Ranges", '')

    logger.debug("Creating path: %s", trgFilename)
    checkCreatePath(os.path.dirname(trgFilename))

    crc_info = ngamsFileUtils.get_checksum_info('crc32')
    if startByte != 0:
        logger.info("resume requested")
    if startByte != 0 and downloadResumeSupported:
        logger.info("Resume requested and mirroring source supports resume. Appending data to previously started staging file")
        crc = ngamsFileUtils.get_checksum(65536, trgFilename, 'crc32')
        reqPropsObj.setBytesReceived(startByte)
        fdOut = open(trgFilename, "a")
    else:
        if (startByte > 0):
            logger.info("Resume of download requested but server does not support it. Starting from byte 0 again.")
        fdOut = open(trgFilename, "w")
        crc = crc_info.init

    start = time.time()

    # Distinguish between Archive Pull and Push Request. By Archive
    # Pull we may simply read the file descriptor until it returns "".
    logger.info("It is an HTTP Archive Pull Request: trying to get Content-length")
    hdrs = {h[0]: h[1] for h in response.getheaders()}
    if hdrs.has_key('content-length'):
        remSize = int(hdrs['content-length'])
    else:
        logger.warning("Non Content-Lenght header found, defaulting to 1e11")
        remSize = int(1e11)

    # Receive the data.
    buf = "-"
    rdSize = blockSize

    crc_m = crc_info.method
    with contextlib.closing(response), contextlib.closing(fdOut):
        while (remSize > 0):
            if (remSize < rdSize):
                rdSize = remSize
            buf = response.read(rdSize)
            sizeRead = len(buf)
            if sizeRead == 0:
                raise Exception("server is unreachable")
            else:
                crc = crc_m(buf, crc)
                remSize -= sizeRead
                reqPropsObj.setBytesReceived(reqPropsObj.getBytesReceived() +\
                                         sizeRead)
                fdOut.write(buf)
    crc = crc_info.final(crc)

    deltaTime = time.time() - start
    msg = "Saved data in file: %s. Bytes received: %d. Time: %.3f s. " +\
          "Rate: %.2f Bytes/s"
    logger.info(msg, trgFilename, int(reqPropsObj.getBytesReceived()),
                  deltaTime, (float(reqPropsObj.getBytesReceived()) /
                              deltaTime))

    # Raise exception if less byes were received as expected.
    if (remSize != 0):
        msg = "No all expected data arrived, %d bytes left to read" % (remSize,)
        raise ngamsFailedDownloadException.FailedDownloadException(msg)

    # now check the CRC value against what we expected
    sourceChecksum = reqPropsObj.checksum
    logger.info('source checksum: %s - current checksum: %d', str(sourceChecksum), crc)
    if (crc != int(sourceChecksum)):
        msg = "checksum mismatch: source=" + str(sourceChecksum) + ", received: " + str(crc)
        raise ngamsFailedDownloadException.FailedDownloadException(msg)

    return [deltaTime,crc]
