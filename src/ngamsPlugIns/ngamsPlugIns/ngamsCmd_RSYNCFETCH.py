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
# *****************************************************************************
#
# "@(#) $Id: ngamsCmd_RSYNCFETCH.py,v 1.1 2012/11/22 21:48:22 amanning Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jagonzal  2010/17/01  Created
#
"""
NGAS Command Plug-In, implementing an Archive Command specific for Mirroring

This works in a similar way as the 'standard' ARCHIVE Command, but has been simplified in a few ways:

* No replication to a Replication Volume is carried out.
* Target disks are selected randomly, disregarding the Streams/Storage Set mappings in the configuration. This means
that 'volume load balancing' is provided.
* Archive Proxy Mode is not supported.
* No probing for storage availability is supported.
* In general, less SQL queries are performed and the algorithm is more light-weight.
* crc is computed from the incoming stream
* ngas_files data is 'cloned' from the source file
"""

import contextlib
import logging
import os
import time

from ngamsLib.ngamsCore import checkCreatePath, getHostName
from ngamsLib import ngamsLib, ngamsHttpUtils
from ngamsServer import ngamsFileUtils
from ngamsServer.ngamsArchiveUtils import archiving_results
from . import ngamsFailedDownloadException

logger = logging.getLogger(__name__)


def get_fully_qualified_name(ngams_server):
    """
    Get fully qualified server name for the input NGAS server object
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :return: string, fully qualified host name (host name + domain + port)
    """
    # Get hots_id, domain and port using ngamsLib functions
    host_name = getHostName()
    domain = ngamsLib.getDomain()
    # Concatenate all elements to construct full qualified name
    # Notice that host_id may contain port number
    fqdn = host_name + "." + domain
    return fqdn


def save_to_file(ngams_server, request_properties, target_filename):
    """
    Save the data available on an HTTP channel into the given file
    :param ngams_server: NG/AMS Configuration object (ngamsConfig)
    :param request_properties: NG/AMS Request Properties object (ngamsReqProps)
    :param target_filename: Target name for file where data will be written (string)
    :param block_size: Block size (bytes) to apply when reading the data from the HTTP channel (integer)
    :param start_byte: Start byte offset
    :return: Tuple. Element 0: Time in took to write file (s) (tuple)
    """
    source_host = request_properties.fileinfo['sourceHost']
    file_version = request_properties.fileinfo['fileVersion']
    file_id = request_properties.fileinfo['fileId']

    logger.info("Creating path: %s", target_filename)
    checkCreatePath(os.path.dirname(target_filename))

    rx_timeout = 30 * 60
    if ngams_server.getCfg().getVal("Mirroring[1].rx_timeout"):
        rx_timeout = int(ngams_server.getCfg().getVal("Mirroring[1].rx_timeout"))

    host, port = source_host.split(":")
    pars = {
        'file_version': file_version,
        'targetHost': get_fully_qualified_name(ngams_server),
        'targetLocation': target_filename,
        'file_id': file_id
    }

    fetch_start_time = time.time()
    response = ngamsHttpUtils.httpGet(host, int(port), 'RSYNC', pars=pars, timeout=rx_timeout)
    with contextlib.closing(response):
        data = response.read()
        if 'FAILURE' in data:
            raise Exception(data)
    fetch_duration = time.time() - fetch_start_time

    # Avoid divide by zeros later on, let's say it took us 1 [us] to do this
    if fetch_duration == 0.0:
        fetch_duration = 0.000001

    msg = "Saved data in file: %s. Bytes received: %d. Time: %.3f s. Rate: %.2f Bytes/s"
    logger.info(msg, target_filename, int(request_properties.getBytesReceived()), fetch_duration,
                (float(request_properties.getBytesReceived()) / fetch_duration))

    # Now check the CRC value against what we expected
    checksum = request_properties.checksum
    crc_variant = request_properties.checksum_plugin
    crc_start_time = time.time()
    crc = ngamsFileUtils.get_checksum(65536, target_filename, crc_variant)
    crc_duration = time.time() - crc_start_time
    logger.info("CRC computed in %f [s]", crc_duration)
    logger.info('Cource checksum: %s - current checksum: %d', checksum, crc)
    if crc != int(checksum):
        msg = "Checksum mismatch: source={:s}, received={:d}".format(checksum, crc)
        raise ngamsFailedDownloadException.FailedDownloadException(msg)

    # We half the total time for reading and writing because we do not have enough data for an accurate measurement
    read_duration = fetch_duration / 2.0
    write_duration = fetch_duration / 2.0
    read_total_bytes = request_properties.getBytesReceived()

    return archiving_results(read_total_bytes, read_duration, write_duration, crc_duration, fetch_duration, crc_variant, crc)
