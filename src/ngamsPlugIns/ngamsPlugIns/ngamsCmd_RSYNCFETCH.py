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
# "@(#) $Id: ngamsCmd_RSYNCFETCH.py,v 1.1 2012/11/22 21:48:22 amanning Exp $"
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

from ngamsLib.ngamsCore import checkCreatePath, getHostName
from . import ngamsFailedDownloadException
from ngamsLib import ngamsLib, ngamsHttpUtils
from ngamsServer import ngamsFileUtils

logger = logging.getLogger(__name__)


def get_full_qualified_name(srvObj):
    """
    Get full qualified server name for the input NGAS server object

    INPUT:
        srvObj  ngamsServer, Reference to NG/AMS server class object

    RETURNS:
        fqdn    string, full qualified host name (host name + domain + port)
    """

    # Get hots_id, domain and port using ngamsLib functions
    hostName = getHostName()
    domain = ngamsLib.getDomain()
    # Concatenate all elements to construct full qualified name
    # Notice that host_id may contain port number
    fqdn = hostName + "." + domain

    # Return full qualified server name
    return fqdn


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

    source_host = reqPropsObj.fileinfo['sourceHost']
    file_version = reqPropsObj.fileinfo['fileVersion']
    file_id = reqPropsObj.fileinfo['fileId']

    logger.info("Creating path: %s", trgFilename)
    checkCreatePath(os.path.dirname(trgFilename))

    rx_timeout = 30 * 60
    if srvObj.getCfg().getVal("Mirroring[1].rx_timeout"):
        rx_timeout = int(srvObj.getCfg().getVal("Mirroring[1].rx_timeout"))

    host, port = source_host.split(":")
    pars = {
        'file_version': file_version,
        'targetHost': get_full_qualified_name(srvObj),
        'targetLocation': trgFilename,
        'file_id': file_id
    }

    start = time.time()
    response = ngamsHttpUtils.httpGet(host, int(port), 'RSYNC', pars=pars, timeout=rx_timeout)
    with contextlib.closing(response):
        data = response.read()
        if 'FAILURE' in data:
            raise Exception(data)
    deltaTime = time.time() - start

    msg = "Saved data in file: %s. Bytes received: %d. Time: %.3f s. " +\
          "Rate: %.2f Bytes/s"
    logger.info(msg, trgFilename, int(reqPropsObj.getBytesReceived()),
                  deltaTime, (float(reqPropsObj.getBytesReceived()) / deltaTime))

    # now check the CRC value against what we expected
    checksum = reqPropsObj.checksum
    crc_variant = reqPropsObj.checksum_plugin
    start = time.time()
    crc = ngamsFileUtils.get_checksum(65536, trgFilename, crc_variant)
    deltaTime = time.time() - start
    logger.info("crc computed in %f [s]", deltaTime)
    logger.info('source checksum: %s - current checksum: %d', checksum, crc)
    if crc != int(checksum):
        msg = "checksum mismatch: source={:s}, received={:d}".format(checksum, crc)
        raise ngamsFailedDownloadException.FailedDownloadException(msg)

    return [deltaTime, crc, crc_variant]
