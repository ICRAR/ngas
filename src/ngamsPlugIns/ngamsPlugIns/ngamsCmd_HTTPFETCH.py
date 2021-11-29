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
# "@(#) $Id: ngamsCmd_HTTPFETCH.py,v 1.1 2012/11/22 21:48:22 amanning Exp $"
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

from ngamsLib.ngamsCore import checkCreatePath, NGAMS_RETRIEVE_CMD
from ngamsLib import ngamsHttpUtils
from ngamsServer import ngamsFileUtils, ngamsSrvUtils
from ngamsServer.ngamsArchiveUtils import archiving_results
from . import ngamsFailedDownloadException

logger = logging.getLogger(__name__)


def save_to_file(ngams_server, request_properties, target_filename, block_size, start_byte):
    """
    Save the data available on an HTTP channel into the given file
    :param ngams_server: Reference to NG/AMS server class object (ngamsServer)
    :param request_properties: NG/AMS Request Properties object (ngamsReqProps)
    :param target_filename: Target name for file where data will be written (string)
    :param block_size: Block size (bytes) to apply when reading the data from the HTTP channel (integer)
    :param start_byte: Start byte offset
    :return: Tuple. Element 0: Time in took to write file (s) (tuple)
    """
    disk_id = request_properties.fileinfo['diskId']
    source_host = request_properties.fileinfo['sourceHost']
    host_id = request_properties.fileinfo['hostId']
    file_version = request_properties.fileinfo['fileVersion']
    file_id = request_properties.fileinfo['fileId']
    checksum = request_properties.checksum
    crc_variant = request_properties.checksum_plugin

    host, port = source_host.split(":")
    parameter_list = [
        ('disk_id', disk_id),
        ('host_id', host_id),
        ('quick_location', '1'),
        ('file_version', file_version),
        ('file_id', file_id)]
    header_dict = {'Range': "bytes={:d}-".format(start_byte)}

    rx_timeout = 30 * 60
    if ngams_server.getCfg().getVal("Mirroring[1].rx_timeout"):
        rx_timeout = int(ngams_server.getCfg().getVal("Mirroring[1].rx_timeout"))

    url = 'http://{0}:{1}/{2}'.format(host, port, NGAMS_RETRIEVE_CMD)
    authorization_header = ngamsSrvUtils.genIntAuthHdr(ngams_server)
    response = ngamsHttpUtils.httpGetUrl(url, parameter_list, header_dict, rx_timeout, authorization_header)

    # Can we resume a previous download?
    download_resume_supported = 'bytes' in response.getheader("Accept-Ranges", '')

    logger.debug("Creating path: %s", target_filename)
    checkCreatePath(os.path.dirname(target_filename))

    logger.info('Fetching file ID %s, checksum %s, checksum variant %s', file_id, checksum, crc_variant)
    crc_info = ngamsFileUtils.get_checksum_info(crc_variant)
    if start_byte != 0:
        logger.info("Resume requested from start byte %d", start_byte)

    if start_byte != 0 and download_resume_supported:
        logger.info("Resume requested and mirroring source supports resume. Appending data to previous staging file")
        crc = ngamsFileUtils.get_checksum(65536, target_filename, crc_variant)
        request_properties.setBytesReceived(start_byte)
        fd_out = open(target_filename, "ab")
    else:
        if start_byte > 0:
            logger.info("Resume of download requested but server does not support it. Starting from byte 0 again.")
        fd_out = open(target_filename, "wb")
        crc = crc_info.init

    fetch_start_time = time.time()

    # Distinguish between archive pull and push request
    # By archive pull we may simply read the file descriptor until it returns and empty string
    response_header_dict = {h[0]: h[1] for h in response.getheaders()}
    if 'content-length' in response_header_dict:
        # For some reason python 2 uses lower case 'content-length'
        remaining_size = int(response_header_dict['content-length'])
        logger.debug("Got Content-Length header value %d in response", remaining_size)
    elif 'Content-Length' in response_header_dict:
        # For some reason python 3 uses mixed case 'Content-Length'
        remaining_size = int(response_header_dict['Content-Length'])
        logger.debug("Got Content-Length header value %d in response", remaining_size)
    else:
        logger.warning("No Content-Length header found in response. Defaulting to 1e11")
        remaining_size = int(1e11)

    # Receive the data
    read_size = block_size

    crc_duration = 0
    read_duration = 0
    write_duration = 0
    read_total_bytes = 0

    crc_method = crc_info.method
    with contextlib.closing(response), contextlib.closing(fd_out):
        while remaining_size > 0:
            if remaining_size < read_size:
                read_size = remaining_size

            # Read the remote file
            read_start_time = time.time()
            data_buffer = response.read(read_size)
            read_duration += time.time() - read_start_time
            size_read = len(data_buffer)
            read_total_bytes += size_read

            if size_read == 0:
                raise ngamsFailedDownloadException.FailedDownloadException("server is unreachable")

            # CRC
            crc_start_time = time.time()
            crc = crc_method(data_buffer, crc)
            crc_duration += time.time() - crc_start_time

            remaining_size -= size_read
            request_properties.setBytesReceived(request_properties.getBytesReceived() + size_read)

            # Write the file onto disk
            write_start_time = time.time()
            fd_out.write(data_buffer)
            write_duration += time.time() - write_start_time

    crc = crc_info.final(crc)

    fetch_duration = time.time() - fetch_start_time
    # Avoid divide by zeros later on, let's say it took us 1 [us] to do this
    if fetch_duration == 0.0:
        fetch_duration = 0.000001

    msg = "Saved data in file: %s. Bytes received: %d. Time: %.3f s. Rate: %.2f Bytes/s"
    logger.info(msg, target_filename, int(request_properties.getBytesReceived()), fetch_duration,
                (float(request_properties.getBytesReceived()) / fetch_duration))

    # Raise exception if bytes received were less than expected
    if remaining_size != 0:
        msg = "No all expected data arrived, {:d} bytes left to read".format(remaining_size)
        raise ngamsFailedDownloadException.FailedDownloadException(msg)

    # Now check the freshly calculated CRC value against the stored CRC value
    logger.info('source checksum: %s - current checksum: %d', checksum, crc)
    if crc != int(checksum):
        msg = "checksum mismatch: source={:s}, received={:d}".format(checksum, crc)
        raise ngamsFailedDownloadException.FailedDownloadException(msg)

    return archiving_results(read_total_bytes, read_duration, write_duration, crc_duration, fetch_duration,
                             crc_variant, crc)
