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
"""
Function + code to handle the CRETRIEVE Command.
"""

import contextlib
import functools
import logging
import os
import tarfile

from ngamsLib.ngamsCore import genLog, getFileSize
from ngamsLib.ngamsCore import NGAMS_CONT_MT
from ngamsLib.ngamsCore import NGAMS_HOST_LOCAL, NGAMS_HOST_REMOTE, NGAMS_HOST_CLUSTER
from ngamsLib.ngamsCore import NGAMS_RETRIEVE_CMD, NGAMS_ONLINE_STATE, NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE
from ngamsLib import ngamsMIMEMultipart, ngamsHttpUtils
from ngamsServer import ngamsSrvUtils, ngamsFileUtils


logger = logging.getLogger(__name__)

def fopener(fname):
    return functools.partial(open, fname, 'rb')

def http_opener(host, port, file_id, file_version, srvObj):
    pars = [('file_id', file_id), ('file_version', file_version)]
    authHdr = ngamsSrvUtils.genIntAuthHdr(srvObj)
    return functools.partial(ngamsHttpUtils.httpGet, host, port,
                             NGAMS_RETRIEVE_CMD, pars=pars, timeout=30, auth=authHdr)

def finfo_from_database(fileInfo, srvObj, reqPropsObj):

    fileId = fileInfo.getFileId()
    fileVer = fileInfo.getFileVersion()

    # Locate the file best suiting the query and send it back if possible.
    location, _, ipAddress, port, mountPoint, filename, fileVersion, mimeType = \
       ngamsFileUtils.quickFileLocate(srvObj, reqPropsObj, fileId, fileVersion=fileVer)

    basename = os.path.basename(filename)

    if location == NGAMS_HOST_LOCAL:
        absname = os.path.normpath(os.path.join(mountPoint, filename))
        size = getFileSize(absname)
        opener = fopener(absname)
    elif location == NGAMS_HOST_CLUSTER or location == NGAMS_HOST_REMOTE:
        size = srvObj.getDb().getFileSize(fileId, fileVersion)
        opener = http_opener(ipAddress, port, fileId, fileVersion, srvObj)
    else:
        raise Exception("Unknown location type: %s" % (location,))

    return ngamsMIMEMultipart.file_info(mimeType, basename, size, opener)

def cinfo_from_database(cont, srvObj, reqPropsObj):
    finfos = [cinfo_from_database(c, srvObj, reqPropsObj) for c in cont.getContainers()] + \
             [finfo_from_database(f, srvObj, reqPropsObj) for f in cont.getFilesInfo()]
    return ngamsMIMEMultipart.container_info(cont.getContainerName(), finfos)

def round_up(size, mul):
    return ((size + mul - 1) // mul) * mul

def tarsize_cinfo(cinfo):
    """Calculate the size of the resulting tarball"""
    # The size of the tarball will be:
    #  * The size of each individual file, rounded up to a multiple of 512, plus
    #  * 512 bytes for each header (one per file or directory, plus one for the toplevel directory)
    #  * 512 bytes x 2 (two empty blocks at the end)
    return 512 + 1024 + _tarsize_cinfo(cinfo)

def _tarsize_cinfo(cinfo):
    size = 512 * len(cinfo.files)
    for finfo in cinfo.files:
        if isinstance(finfo, ngamsMIMEMultipart.container_info):
            size += _tarsize_cinfo(finfo)
        else:
            size += round_up(finfo.size, 512)
    return size

def send_toplevel_cinfo(cinfo, http_ref):
    tinfo = tarfile.TarInfo(name=cinfo.name)
    tinfo.type = tarfile.DIRTYPE
    tinfo.mode = 0o755
    http_ref.write_data(tinfo.tobuf())
    _send_cinfo(cinfo, http_ref, cinfo.name + '/')
    http_ref.write_data(b'\x00' * 1024)

def _send_finfo(finfo, http_ref):
    """Send a file through for tarballing"""

    if finfo.opener.func == ngamsHttpUtils.httpGet:
        with contextlib.closing(finfo.opener()) as fobj:
            http_ref.write_data(fobj)
    else:
        absfname = finfo.opener.args[0]
        http_ref.write_file_data(absfname, finfo.size)

    padding = b'\x00' * (round_up(finfo.size, 512) - finfo.size)
    http_ref.write_data(padding)


def _send_cinfo(cinfo, http_ref, dirname=''):
    """recursively send containers' files through http connection"""
    for finfo in cinfo.files:
        arcname = dirname + finfo.name
        tinfo = tarfile.TarInfo(name=arcname)
        if isinstance(finfo, ngamsMIMEMultipart.container_info):
            tinfo.type = tarfile.DIRTYPE
            tinfo.mode = 0o755
            http_ref.write_data(tinfo.tobuf())
            _send_cinfo(finfo, http_ref, arcname + '/')
        else:
            tinfo.type = tarfile.REGTYPE
            tinfo.mode = 0o644
            tinfo.size = finfo.size
            http_ref.write_data(tinfo.tobuf())
            _send_finfo(finfo, http_ref)

def _handleCmdCRetrieve(srvObj,
                       reqPropsObj,
                       httpRef):
    """
    Carry out the action of a CRETRIEVE command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of
                    actions done during the request handling
                    (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """

    # For data files, retrieval must be enabled otherwise the request is
    # rejected.
    if (not srvObj.getCfg().getAllowRetrieveReq()):
        errMsg = genLog("NGAMS_ER_ILL_REQ", ["Retrieve"])
        raise Exception(errMsg)

    # We don't allow processing yet
    if 'processing' in reqPropsObj:
        raise Exception('CRETRIEVE command does not allow processing (yet)')

    # At least container_id or container_name must be specified
    containerName = containerId = None
    if "container_id" in reqPropsObj:
        containerId = reqPropsObj["container_id"].strip()
    if not containerId and "container_name" in reqPropsObj:
        containerName = reqPropsObj["container_name"].strip()
    if not containerId and not containerName:
        raise Exception('Neither container_name nor container_id given, cannot retrieve container')

    # If container_name is specified, and maps to more than one container,
    # an error is issued
    if not containerId:
        containerId = srvObj.getDb().getContainerIdForUniqueName(containerName)

    # Users can request a tarball instead of the default MIME multipart message
    return_tar = 'format' in reqPropsObj and reqPropsObj['format'] == 'application/x-tar'

    logger.debug("Handling request for file with containerId: %s", containerId)

    # Build the container hierarchy and get all file references
    container = srvObj.getDb().readHierarchy(containerId, True)
    cinfo = cinfo_from_database(container, srvObj, reqPropsObj)

    # Send all the data back, either as a multipart message or as a tarball
    if return_tar:
        httpRef.send_file_headers(cinfo.name, 'application/x-tar', tarsize_cinfo(cinfo))
        send_toplevel_cinfo(cinfo, httpRef)
    else:
        reader = ngamsMIMEMultipart.ContainerReader(cinfo)
        httpRef.send_data(reader, NGAMS_CONT_MT)


def handleCmd(srvObj, reqPropsObj, httpRef):
    """
    Handle a RETRIEVE command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of
                    actions done during the request handling
                    (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """

    srvObj.checkSetState("Command CRETRIEVE", [NGAMS_ONLINE_STATE],
                         [NGAMS_IDLE_SUBSTATE, NGAMS_BUSY_SUBSTATE],
                         "", NGAMS_BUSY_SUBSTATE)

    try:
        _handleCmdCRetrieve(srvObj, reqPropsObj, httpRef)
    finally:
        srvObj.setSubState(NGAMS_IDLE_SUBSTATE)


# EOF
