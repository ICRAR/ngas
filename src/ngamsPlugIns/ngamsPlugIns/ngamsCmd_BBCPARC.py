#
#    ICRAR - International Centre for Radio Astronomy Research
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
"""
NGAS Command Plug-In, implementing a Archive Pull Command using BBCP
"""

from collections import namedtuple
import logging
import os
import subprocess
import time
from urlparse import urlparse

from ngamsLib.ngamsCore import checkCreatePath, getFileSize
from ngamsServer import ngamsArchiveUtils, ngamsFileUtils


logger = logging.getLogger(__name__)

bbcp_param = namedtuple('bbcp_param', 'port, winsize, num_streams, checksum')


def bbcpFile(srcFilename, targFilename, bparam, crc_name, skip_crc):
    """
    Use bbcp tp copy file <srcFilename> to <targFilename>

    NOTE: This requires remote access to the host as well as
         a bbcp installation on both the remote and local host.
    """
    logger.debug("Copying file: %s to filename: %s", srcFilename, targFilename)

    # Make target file writable if existing.
    if (os.path.exists(targFilename)):
        os.chmod(targFilename, 420)

    checkCreatePath(os.path.dirname(targFilename))

    if bparam.port:
        pt = ['-Z', str(bparam.port)]
    else:
        pt = ['-z']

    fw = []
    if bparam.winsize:
        fw = ['-w', str(bparam.winsize)]

    ns = []
    if (bparam.num_streams):
        ns = ['-s', str(bparam.num_streams)]

    # bypass password prompt with -oBatchMode=yes this implies you need keys
    ssh_src = ['-S', 'ssh -x -a -oBatchMode=yes -oFallBackToRsh=no %4 %I -l %U %H bbcp']

    # perform checksum on host and compare to target. If it's different bbcp will fail.
    if not skip_crc and crc_name is not None:
        cmd_checksum = ['-e', '-E']
        if crc_name in ('crc32', 'crc32z'):
            # c32z is the zip-flavor of CRC32
            # c32 is the POSIX flavour, which yields a different result
            cmd_checksum.append('c32z=/dev/stdout')
        elif crc_name == 'crc32c':
            cmd_checksum.append('c32c=/dev/stdout')
        else:
            raise Exception("Unsupported checksum method in BBCP: %s" % (crc_name,))

    cmd_list = ['bbcp', '-f', '-V'] + ssh_src + cmd_checksum + fw + ns + ['-P', '2'] + pt + [srcFilename, targFilename]

    logger.info("Executing external command: %s", subprocess.list2cmdline(cmd_list))

    p1 = subprocess.Popen(cmd_list, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    checksum_out, out = p1.communicate()

    if p1.returncode != 0:
        raise Exception, "bbcp returncode: %d error: %s" % (p1.returncode, out)

    # extract c32 zip variant checksum from output and convert to signed 32 bit integer
    crc_info = ngamsFileUtils.get_checksum_info(crc_name)
    bbcp_checksum = crc_info.from_bytes(checksum_out.split(' ')[2].decode('hex'))

    logger.info('BBCP final message: %s', out.split('\n')[-2]) # e.g. "1 file copied at effectively 18.9 MB/s"
    logger.info("File: %s copied to filename: %s", srcFilename, targFilename)

    return str(bbcp_checksum[0])


def get_params(request):

    # exclude pulling files from these locations
    invalid_paths = ('/dev', '/var', '/usr', '/opt', '/etc')
    uri = request.getFileUri()

    if uri.lower().startswith('ssh://'):
        uri = uri[6:]
    elif uri.lower().startswith('ssh:/'):
        uri = uri[5:]
    elif uri.lower().startswith('ssh:'):
        uri = uri[4:]

    uri = 'ssh://' + uri
    uri_parsed = urlparse(uri)
    if uri_parsed.path.lower().startswith(invalid_paths):
        raise Exception('Requested to pull file from exluded location')

    # Collect BBCP parameters
    port = None
    winsize = None
    num_streams = None
    checksum = None

    if 'bport' in request:
        port = int(request['bport'])

    if 'bwinsize' in request:
        winsize = request['bwinsize']

    if 'bnum_streams' in request:
        num_streams = int(request['bnum_streams'])

    if 'bchecksum' in request:
        checksum = request['bchecksum']

    return bbcp_param(port, winsize, num_streams, checksum)


def bbcp_transfer(request, out_fname, crc_name, skip_crc):

    bparam = get_params(request)

    # perform the bbcp transfer, we will always return the checksum
    start = time.time()
    checksum = bbcpFile(request.getFileUri(), out_fname, bparam, crc_name, skip_crc)
    size = getFileSize(out_fname)
    totaltime = time.time() - start

    # Feedback the file size into the request object now that we know it,
    # just in case we need it later
    request.setSize(size)

    # Artificially split the total time between read and write (+ 0 crc),
    # we don't actually know how much was spent on what
    half = totaltime / 2
    return ngamsArchiveUtils.archiving_results(size, half, half, 0, totaltime, crc_name, checksum)


def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Handle the BBCP Command.
    """

    mimeType = ngamsArchiveUtils.archiveInitHandling(srvObj, reqPropsObj, httpRef,
                                                     do_probe=False, try_to_proxy=False)

    if not mimeType:
        return

    ngamsArchiveUtils.dataHandler(srvObj, reqPropsObj, httpRef,
                                  volume_strategy=ngamsArchiveUtils.VOLUME_STRATEGY_RANDOM,
                                  pickle_request=False, sync_disk=False,
                                  do_replication=False,
                                  transfer=bbcp_transfer)
