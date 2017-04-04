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
# "@(#) $Id: ngamsPClient.py,v 1.7 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  08/05/2001  Created
#
"""
This module contains the class ngamsPClient that provides a command
interface to the NG/AMS Server.

The NG/AMS Python Client is implemented as a class, ngamsPClient, which
can be used to build up Python applications communicating with NG/AMS.
"""

import argparse
import base64
import contextlib
import functools
import logging
import os
import random
import socket
import sys
import time
from xml.dom import minidom

from ngamsLib import ngamsLib, ngamsFileInfo, ngamsStatus, ngamsMIMEMultipart
from ngamsLib.ngamsCore import NGAMS_EXIT_CMD, NGAMS_INIT_CMD,\
    NGAMS_HTTP_SUCCESS
from ngamsLib.ngamsCore import TRACE, NGAMS_ARCHIVE_CMD, NGAMS_REARCHIVE_CMD, NGAMS_HTTP_PAR_FILENAME, NGAMS_HTTP_HDR_FILE_INFO, NGAMS_HTTP_HDR_CONTENT_TYPE, \
    NGAMS_LABEL_CMD, NGAMS_ONLINE_CMD, NGAMS_OFFLINE_CMD, NGAMS_REMDISK_CMD, \
    NGAMS_REMFILE_CMD, NGAMS_REGISTER_CMD, NGAMS_RETRIEVE_CMD, NGAMS_STATUS_CMD, \
    NGAMS_FAILURE, NGAMS_SUBSCRIBE_CMD, NGAMS_UNSUBSCRIBE_CMD, NGAMS_ARCH_REQ_MT, \
    NGAMS_CACHEDEL_CMD, NGAMS_CLONE_CMD, \
    NGAMS_HTTP_REDIRECT, getNgamsVersion, NGAMS_SUCCESS, NGAMS_ONLINE_STATE, \
    NGAMS_IDLE_SUBSTATE, getNgamsLicense, toiso8601, NGAMS_CONT_MT


logger = logging.getLogger(__name__)

def is_known_pull_url(s):
    return s.startswith('file:') or \
           s.startswith('http:') or \
           s.startswith('https:') or \
           s.startswith('ftp:')

def _dummy_stat(host_id, status, msg):
    return ngamsStatus.ngamsStatus().\
           setDate(toiso8601()).\
           setVersion(getNgamsVersion()).setHostId(host_id).\
           setStatus(status).\
           setMessage(msg).\
           setState(NGAMS_ONLINE_STATE).\
           setSubState(NGAMS_IDLE_SUBSTATE)

def _dummy_success_stat(host_id):
    return _dummy_stat(host_id, NGAMS_SUCCESS, "Successfully handled request")

def _dummy_failure_stat(host_id, cmd):
    logger.debug("HTTP status != 200, creating dummy NGAS_FAILURE status")
    return _dummy_stat(host_id, NGAMS_FAILURE, "Failed to handle command %s" % (cmd,))

class ngamsPClient:
    """
    Class providing services for sending and receiving commands to/from
    the NG/AMS Server.
    """

    def __init__(self,
                 host=None, port=None, servers=None, timeout=None, auth=None,
                 reload_mod=False):
        """
        Constructor.

        Either a list of server addresses or a single server address can be specified.
        If none is given then localhost:7777 is assumed
        """

        if servers and (host or port):
            raise ValueError("Either host/port or servers must be specified")

        if servers is not None:
            if not servers:
                raise ValueError("Empty server list")
            self.servers = servers
        else:
            host = host or 'localhost'
            port = port or 7777
            self.servers = [(host, port)]

        self.timeout = timeout
        self.auth = auth
        self.reload_mod = reload_mod


    def archive(self,
                fileUri,
                mimeType = "",
                async = False,
                noVersioning = 0,
                pars = [],
                cmd = NGAMS_ARCHIVE_CMD):
        """
        Send an Archive Request to the associated NG/AMS Server asking to
        have the file specified by the URI archived. This can either
        be an Archive Push Request or an Archive Pull Request.

        fileUri:       URI of file to archive (string).

        mimeType:      Mime-type of the data. Must be given if it is not
                       possible to determine the mime-type of the data
                       file to be archived from its filename (string).

        async:     Whether the registration should be carried out asynchronously
                   or not.

        noVersioning:  If set to 1 no new version number is
                       generated (integer).

        pars:          Extra parameters to submit with the request. Must be
                       in the format:

                         [[<Par>, <Val>], [<Par>, <Val>], ...]    (list).

        Returns:       NG/AMS Status object (ngamsStatus).
        """

        logger.info("Archiving file with URI: %s", fileUri)

        pars = pars or []
        pars.append(('async', '1' if async else '0'))
        pars.append(("no_versioning", '1' if noVersioning else '0'))

        # Archive pulls (fileUri is a URL) are GETs
        if is_known_pull_url(fileUri):
            pars.append(('filename', fileUri))
            if mimeType:
                pars.append(("mime_type", mimeType))
            return self.get_status(cmd, pars=pars)

        # pushes are POSTs
        # fileUri is simply a file path
        mt = mimeType or NGAMS_ARCH_REQ_MT
        pars.append(("filename", os.path.basename(fileUri)))
        with open(fileUri, "rb") as f:
            return self.post(cmd, mt, f, pars=pars)

    def archive_data(self, data, filename, mimeType,
                     async=False, noVersioning=0,
                     pars=[], cmd=NGAMS_ARCHIVE_CMD):
        """
        Like `archive`, but the data to sent is in memory instead of in a file.
        Thus, a filename to be used for storing on the server side must be given.
        """
        pars.append(("filename", os.path.basename(filename)))
        return self.post(cmd, mimeType, data, pars=pars)

    def reArchive(self,
                  fileUri,
                  fileInfoXml,
                  pars = []):
        """
        Send a  Re-archive Request to the associated NG/AMS Server asking to
        have the file specified by the URI archived. This can either
        be a Re-archive Push Request or a Re-archive Pull Request.

        fileUri:       URI of file to archive (string).

        fileInfoXml:   NG/AMS XML File Information for the source file used
                       as reference (string/XML).

        pars:          Extra parameters to submit with the request. Must be
                       in the format:

                         [[<Par>, <Val>], [<Par>, <Val>], ...]    (list).

        Returns:       NG/AMS Status object (ngamsStatus).
        """
        baseName = os.path.basename(fileUri)
        logger.info("Re-archiving file with URI: %s", baseName)
        locPars = []
        for par in pars: locPars.append(par)
        if is_known_pull_url(fileUri):
            tmpFileInfo = ngamsFileInfo.ngamsFileInfo().\
                          unpackXmlDoc(fileInfoXml)
            encFileInfo = base64.b64encode(fileInfoXml)
            locPars.append([NGAMS_HTTP_PAR_FILENAME, fileUri])
            httpHdrs = {NGAMS_HTTP_HDR_FILE_INFO: encFileInfo,
                        NGAMS_HTTP_HDR_CONTENT_TYPE: tmpFileInfo.getFormat()}
            res = self.get_status(NGAMS_REARCHIVE_CMD, pars=locPars, hdrs=httpHdrs)
        else:
            msg = "Rearchive Push is not yet supported!"
            raise Exception(msg)
        return res


    def clone(self,
              fileId,
              diskId,
              fileVersion,
              targetDiskId = "",
              async = False):
        """
        Send an CLONE command to the NG/AMS Server associated to the object.

        fileId:        ID of file to clone (string).

        diskId:        ID of disk where file(s) to be cloned are
                       stored (string).

        fileVersion:   Version of file (integer).

        targetDiskId:  ID of disk where the files cloned should be stored
                       (string).

        async:     Whether the registration should be carried out asynchronously
                   or not.

        Returns:       NG/AMS Status object (ngamsStatus).
        """
        pars = [('async', '1' if async else '0')]
        if (fileId): pars.append(["file_id", fileId])
        if (diskId): pars.append(["disk_id", diskId])
        if (fileVersion > 0): pars.append(["file_version", fileVersion])
        if (targetDiskId): pars.append(["target_disk_id", targetDiskId])
        return self.get_status(NGAMS_CLONE_CMD, pars=pars)

    def carchive(self, dirname, files_mtype):
        """
        Sends a CARCHIVE command to the NG/AMS Server to archive
        a full hierarchy of files, effectively creating a hierarchy of
        containers with all the files within
        """

        # If the dataRef is a directory, scan the directory
        # and build up a list of files contained directly within
        # Start preparing a mutipart MIME message that will contain
        # all of them
        dirname = os.path.abspath(dirname)
        logger.debug('Archiving directory %s as a container', dirname)

        # Recursively collect all files
        cinfo = ngamsMIMEMultipart.cinfo_from_filesystem(dirname, files_mtype)
        stream = ngamsMIMEMultipart.ContainerReader(cinfo)

        return self.post('CARCHIVE', NGAMS_CONT_MT, stream)

    def cappend(self, fileId, fileIdList='', containerId=None, containerName=None, force=False, closeContainer=False):
        """
        Sends a CAPPEND command to the NG/AMS Server to append the
        given file/s to the container indicated either by containerId
        or containerName. If fileId is given it is used; otherwise
        filesIdList must be given
        """
        if not (bool(containerId) ^ bool(containerName)):
            raise Exception('Either containerId or containerName must be indicated for CAPPEND')
        if not (bool(fileId) ^ bool(fileIdList)):
            raise Exception('Either fileId or fileIdList must be indicated for CAPPEND')

        pars = []
        if containerId:
            pars.append(['container_id', containerId])
        if containerName:
            pars.append(['container_name', containerName])
        if force:
            pars.append(['force', 1])
        if closeContainer:
            pars.append(['close_container', 1])

        if fileId:
            pars.append(['file_id', fileId])
            return self.get_status('CAPPEND', pars=pars)
        else:
            # Convert the list of file IDs to an XML document
            doc = minidom.Document()
            fileListEl = doc.createElement('FileList')
            doc.appendChild(fileListEl)
            for fileId in fileIdList.split(':'):
                fileEl = doc.createElement('File')
                fileEl.setAttribute('FileId', fileId)
                fileListEl.appendChild(fileEl)
            fileListXml = doc.toxml(encoding='utf-8')

            # And send it out!
            return self.post('CAPPEND', 'text/xml', fileListXml, pars=pars)

    def ccreate(self, containerName, parentContainerId=None, containerHierarchy=None):
        """
        Sends a CCREATE command to the NG/AMS Server to create a container
        or a container hierarchy
        """
        if not (bool(containerName) ^ bool(containerHierarchy)):
            raise Exception('Either a container name or container hierarchy must be indicated to create container/s')

        pars = []
        if containerName:
            pars.append(['container_name', containerName])
            if parentContainerId:
                pars.append(['parent_container_id', parentContainerId])
            return self.get_status('CCREATE', pars=pars)
        else:
            contHierarchyXml = containerHierarchy
            return self.post('CCREATE', 'text/xml', contHierarchyXml, pars=pars)

    def cdestroy(self, containerName, containerId=None, recursive=False):
        """
        Sends a CDESTROY command to the NG/AMS Server to destroy a container
        or a container hierarchy
        """
        if not (bool(containerId) ^ bool(containerName)):
            raise Exception('Either containerId or containerName must be indicated for CAPPEND')

        pars = []
        if containerId:
            pars.append(['container_id', containerId])
        if containerName:
            pars.append(['container_name', containerName])
        if recursive:
            pars.append(['recursive', 1])

        return self.get_status('CDESTROY', pars=pars)

    def clist(self, containerName, containerId=None):
        """
        Sends a CLIST command to the NG/AMS Server to get information about
        a particular container and its recursive hierarchy
        """
        """
        Sends a CDESTROY command to the NG/AMS Server to destroy a container
        or a container hierarchy
        """
        if not (bool(containerId) ^ bool(containerName)):
            raise Exception('Either containerId or containerName must be indicated for CAPPEND')

        pars = []
        if containerId:
            pars.append(['container_id', containerId])
        if containerName:
            pars.append(['container_name', containerName])

        return self.get_status('CLIST', pars=pars)

    def cremove(self, fileId, fileIdList='', containerId=None, containerName=None):
        """
        Sends a CAPPEND command to the NG/AMS Server to append the
        given file/s to the container indicated either by containerId
        or containerName. If fileId is given it is used; otherwise
        filesIdList must be given
        """
        if not (bool(containerId) ^ bool(containerName)):
            raise Exception('Either containerId or containerName must be indicated for CREMOVE')
        if not (bool(fileId) ^ bool(fileIdList)):
            raise Exception('Either fileId or fileIdList must be indicated for CREMOVE')

        pars = []
        if containerId:
            pars.append(['container_id', containerId])
        if containerName:
            pars.append(['container_name', containerName])

        if fileId:
            pars.append(['file_id', fileId])
            return self.get_status('CREMOVE', pars=pars)
        else:
            # Convert the list of file IDs to an XML document
            doc = minidom.Document()
            fileListEl = doc.createElement('FileList')
            doc.appendChild(fileListEl)
            for fileId in fileIdList.split(':'):
                fileEl = doc.createElement('File')
                fileEl.setAttribute('FileId', fileId)
                fileListEl.appendChild(fileEl)
            fileListXml = doc.toxml(encoding='utf-8')

            # And send it out!
            return self.post('CREMOVE', 'text/xml', fileListXml, pars=pars)


    def cretrieve(self, containerName, containerId=None, targetDir='.'):
        """
        Sends a CRETRIEVE command to NG/AMS to retrieve the full contents of a
        container and dumps them into the file system.
        """
        if (not containerId and not containerName):
            msg = "Must specify parameter -containerId or -containerName for " +\
                  "a CRETRIEVE Command"
            raise Exception, msg
        if not targetDir:
            targetDir = '.'

        pars = []
        if containerId:
            pars.append(("container_id", containerId))
        if containerName:
            pars.append(("container_name", containerName))

        resp, host, port = self.get('CRETRIEVE', pars=pars)
        host_id = "%s:%d" % (host, port)
        with contextlib.closing(resp):
            if resp.status != NGAMS_HTTP_SUCCESS:
                return ngamsStatus.ngamsStatus().unpackXmlDoc(resp.read(), 1)

            size = int(resp.getheader('Content-Length'))
            handler = ngamsMIMEMultipart.FilesystemWriterHandler(1024, basePath=targetDir)
            parser = ngamsMIMEMultipart.MIMEMultipartParser(handler, resp, size, 65536)
            parser.parse()
            return _dummy_success_stat(host_id)

    def exit(self):
        """
        Send an EXIT command to the NG/AMS Server associated to the object.

        Returns:   NG/AMS Status object (ngamsStatus).
        """
        return self.get_status(NGAMS_EXIT_CMD)


    def init(self):
        """
        Send an INIT command to the NG/AMS Server associated to the object.

        Returns:   NG/AMS Status object (ngamsStatus).
        """
        return self.get_status(NGAMS_INIT_CMD)


    def label(self,
              slotId,
              hostId):
        """
        Send an LABEL command to the NG/AMS Server associated to the object.

        slotId:    Slot ID for which to generate the label (string).

        hostId:    Host ID where the disk is installed (string).

        Returns:   NG/AMS Status object (ngamsStatus).
        """
        pars = [("slot_id", slotId), ("host_id", hostId)]
        return self.get_status(NGAMS_LABEL_CMD, pars=pars)


    def online(self):
        """
        Send an ONLINE command to the NG/AMS Server associated to the object.

        Returns:   NG/AMS Status object (ngamsStatus).
        """
        return self.get_status(NGAMS_ONLINE_CMD)


    def offline(self, force=False):
        """
        Send an OFFLINE command to the NG/AMS Server associated to the object.

        force:     If set to 1 the NG/AMS Server will be forced to
                   go Offline (integer).

        Returns:   NG/AMS Status object (ngamsStatus).
        """
        pars = [('force', "1" if force else "0")]
        return self.get_status(NGAMS_OFFLINE_CMD, pars=pars)


    def remDisk(self,
                diskId,
                execute = 0):
        """
        Send an REMDISK command to the NG/AMS Server associated to the object.

        diskId:    ID of disk to remove from the system (string).

        execute:   If set to 1 the action will be carried out, otherwise
                   only a report will be generated indicating what has
                   has been selected for removal (integer/0|1).

        Returns:   NG/AMS Status object (ngamsStatus).
        """
        pars = [("disk_id", diskId), ("execute", execute)]
        return self.get_status(NGAMS_REMDISK_CMD, pars=pars)


    def remFile(self,
                diskId,
                fileId,
                fileVersion = -1,
                execute = False):
        """
        Send an REMFILE command to the NG/AMS Server associated to the object.

        diskId:       ID of disk hosting file(s) to remove from the
                      system (system).

        fileId:       ID of file(s) to be removed (string).

        fileVersion:  Version of file to consider. If -1 is specified
                      the latest version will be selected (integer).

        execute:      If set to 1 the action will be carried out, otherwise
                      only a report will be generated indicating what has
                      has been selected for removal (integer/0|1).

        Returns:      NG/AMS Status object (ngamsStatus).
        """
        pars = [("disk_id", diskId),
                ("file_id", fileId),
                ("execute", "1" if execute else "0")]
        if fileVersion != -1:
            pars.append(("file_version", fileVersion))
        return self.get_status(NGAMS_REMFILE_CMD, pars=pars)


    def register(self, path, async=False):
        """
        Send an REGISTER command to the NG/AMS Server associated to the object.

        path:      Path used as starting point for selecting files for
                   registration (string).

        async:     Whether the registration should be carried out asynchronously
                   or not.

        Returns:   NG/AMS Status object (ngamsStatus).
        """
        pars = [("path", path), ('async', '1' if async else '0')]
        return self.get_status(NGAMS_REGISTER_CMD, pars=pars)


    def retrieve2File(self,
                      fileId,
                      fileVersion = -1,
                      targetFile = "",
                      processing = "",
                      processingPars = "",
                      containerName = None,
                      containerId = None,
                      cmd = NGAMS_RETRIEVE_CMD):
        """
        Request a file from the NG/AMS Server associated to the object.
        The file will be stored under the name given by the 'targetFile'
        parameter, or in the current working directory with the name
        as received from NG/AMS.

        fileId:          NG/AMS File ID of file to retrieve (string).

        fileVersion:     Specific version of the file to retrieve (integer)

        targetFile:      Name of file or directory where to store the file
                         retrieved. If "" is specified, the file will be
                         stored in the current working directory under the
                         same name as on NG/AMS (string).

        processing:      Name of DPPI to be invoked by NG/AMS to process the
                         data being retrieved (strings).

        processingPars:  Optional parameters to hand over to the DDPI. The
                         format of these is usually:

                           par1=val1,par2=val2,...

                         - but it is up to the DPPI to interpret
                         these (string).
        """
        pars = [("file_id", fileId)]
        if fileVersion != -1:
            pars.append(("file_version", str(fileVersion)))
        if processing:
            pars.append(("processing", processing))
            if processingPars:
                pars.append(("processingPars", processingPars))

        targetFile = targetFile or '.'

        resp, host, port = self.get('RETRIEVE', pars)
        host_id = "%s:%d" % (host, port)
        with contextlib.closing(resp):

            if resp.status != NGAMS_HTTP_SUCCESS:
                return ngamsStatus.ngamsStatus().unpackXmlDoc(resp.read(), 1)

            # If the target path is a directory, take the filename
            # of the incoming data as the filename
            fname = targetFile
            if os.path.isdir(fname):
                cdisp = resp.getheader('Content-Disposition')
                parts = ngamsLib.parseHttpHdr(cdisp)
                if 'filename' not in parts:
                    msg = "Missing or invalid Content-Disposition header in HTTP response"
                    raise Exception(msg)
                fname = os.path.join(fname, os.path.basename(parts['filename']))

            # Dump the data into the target file
            readf = functools.partial(resp.read, 65536)
            with open(fname, 'wb') as f:
                for buf in iter(readf, ''):
                    f.write(buf)

            return _dummy_success_stat(host_id)


    def status(self):
        """
        Request a general status from the NG/AMS Server
        associated to the object.

        Returns:     NG/AMS Status object (ngamsStatus).
        """
        T = TRACE()

        return self.get_status(NGAMS_STATUS_CMD)


    def subscribe(self,
                  url,
                  priority = 10,
                  startDate = "",
                  filterPlugIn = "",
                  filterPlugInPars = ""):
        """
        Subscribe to data from a Data Provider.

        url:                Subscriber URL to where the Data Provider will
                            deliver (POST) the data (string).

        priority:           Priority of the Data Subscriber. A low number
                            means high priority (integer/[0; 10], (10)).

        startDate:          Lower limit in time for considering data files
                            for delivery (string/ISO 8601).

        filterPlugIn:       Optional Filter Plug-In to invoke on the
                            individual data file to determine whether to
                            deliver it to a Subscriber or not (string).

        filterPlugInPars:   Optional Filter Plug-In parameters. Should be
                            given on the format: '<par>=<val>,<par>=<val>,...'
                            (string).

        Returns:            NG/AMS Status object (ngamsStatus).
        """
        pars = [("url", url), ("priority", priority)]
        if startDate:
            pars.append(("start_date", startDate))
        if filterPlugIn:
            pars.append(("filter_plug_in", filterPlugIn))
        if filterPlugInPars:
            pars.append(("plug_in_pars", filterPlugInPars))
        return self.get_status(NGAMS_SUBSCRIBE_CMD, pars=pars)


    def unsubscribe(self,
                    url):
        """
        Unsubscribe a Data Subscription.

        url:                Subscriber URL used when subscribing to the
                            Data Provider (string).

        Returns:            NG/AMS Status object (ngamsStatus).
        """
        pars = [("url", url)]
        return self.get_status(NGAMS_UNSUBSCRIBE_CMD, pars=pars)


    def get_status(self, cmd, outputFile=None, pars=[], hdrs=[]):
        """
        Sends a GET command to the NGAS server, receives the reply
        and returns it as a ngamsStatus object.
        """

        resp, host, port = self.get(cmd, outputFile, pars, hdrs)
        host_id = "%s:%d" % (host, port)

        if resp.status != NGAMS_HTTP_SUCCESS:
            return _dummy_failure_stat(host_id, cmd)

        # If the reply is a ngamsStatus document read it and return it
        data = resp.read()
        if data and "<?xml" in data:
            logger.debug("Parsing incoming HTTP data as ngamsStatus")
            return ngamsStatus.ngamsStatus().unpackXmlDoc(data, 1)

        # Create a dummy success
        logger.debug("Creating dummy NGAS_SUCCESS status")
        stat = _dummy_success_stat(host_id)
        if data:
            stat.setData(data)
        return stat

    def get(self, cmd, pars=[], hdrs=[]):
        """
        Send a command to the NG/AMS Server and receives the reply.
        """

        if self.timeout:
            pars.append(["time_out", str(self.timeout)])
        if self.reload_mod:
            pars.append(["reload", "1"])

        # For now we try the serves in random order.
        serverList = list(self.servers)
        random.shuffle(serverList)
        logger.debug("Server list: %s", str(serverList))

        # Try, if necessary, contacting all servers from the server list
        for i,host_port in enumerate(serverList):
            host, port = host_port
            try:
                resp = self.do_get(host, port, cmd, pars, hdrs)
                break
            except socket.error:
                if i == len(serverList) - 1:
                    raise
                logger.info("Failed to contact server %s:%d, trying next one", host, port)
                pass

        # Handle redirects, with a maximum of 5
        redirects = 0
        while redirects < 5:

            # Last result was not a redirect, return
            if resp.status != NGAMS_HTTP_REDIRECT:
                return resp, host, port

            # Get the host + port of the alternative URL, and send
            # the same query again.
            location = resp.getheaders()['Location']
            host, port = location.split("/")[2].split(":")
            port = int(port)
            logger.info("Redirecting to NG/AMS running on %s:%d", host, port)

            redirects += 1
            resp = self.do_get(host, port, cmd, pars, hdrs)

        raise Exception("Too many redirections, aborting")


    def do_get(self, host, port, cmd, pars, hdrs):

        auth = None
        if self.auth is not None:
            auth = "Basic %s" % self.auth

        start = time.time()
        res = ngamsLib.httpGet(host, port, cmd, pars=pars, hdrs=hdrs,
                               timeout=self.timeout, auth=auth)
        delta = time.time() - start
        logger.debug("Command: %s to %s:%d handled in %.3f [s]", cmd, host, port, delta)
        return res


    def do_post(self, host, port, cmd, mimeType, data, pars):

        auth = None
        if self.auth is not None:
            auth = "Basic %s" % self.auth

        start = time.time()
        res = ngamsLib.httpPost(host, port, cmd, data, mimeType,
                                pars=pars, timeout=self.timeout, auth=auth)
        delta = time.time() - start
        logger.info("Successfully completed command %s in %.3f [s]", cmd, delta)
        return res


    def post(self, cmd, mime_type, data, pars=[]):

        if self.timeout:
            pars.append(["time_out", str(self.timeout)] )
        if self.reload_mod:
            pars.append(["reload", "1"])

        # For now we try the serves in random order.
        servers = self.servers
        random.shuffle(servers)

        for i,host_port in enumerate(servers):
            host, port = host_port
            try:
                _,_,_,data = self.do_post(host, port, cmd, mime_type, data, pars)
                return ngamsStatus.ngamsStatus().unpackXmlDoc(data, 1)
            except socket.error:
                if i == len(servers) - 1:
                    raise


def setup_logging(opts):

    logging.root.addHandler(logging.NullHandler())

    if opts.verbose:
        logging_levels = {
            0:logging.CRITICAL,
            1:logging.ERROR,
            2:logging.WARNING,
            3:logging.INFO,
            4:logging.DEBUG,
            5:logging.NOTSET
        }

        fmt = '%(asctime)-15s.%(msecs)03d [%(threadName)10.10s] [%(levelname)6.6s] %(name)s#%(funcName)s:%(lineno)s %(message)s'
        datefmt = '%Y-%m-%dT%H:%M:%S'
        formatter = logging.Formatter(fmt, datefmt=datefmt)
        formatter.converter = time.gmtime
        hnd = logging.StreamHandler(stream=sys.stdout)
        hnd.setFormatter(formatter)
        logging.root.addHandler(hnd)
        logging.root.setLevel(logging_levels[opts.verbose - 1])

def main():
    """
    Entry point for the ngamsPClient script
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('cmd', help='Command to issue')

    gparser = parser.add_argument_group('General options')
    gparser.add_argument('-L', '--license', help='Show the license information', action='store_true')
    gparser.add_argument('-V', '--version', help='Show the version and exit', action='store_true')
    gparser.add_argument('-v', '--verbose', help='Increase verbosity. More -v is more verbose', action='count', default=3)

    cparser = parser.add_argument_group('Connection options')
    cparser.add_argument('-H', '--host',    help='Host to connect to', default="127.0.0.1")
    cparser.add_argument('-p', '--port',    help='Port to connect to', default=7777, type=int)
    cparser.add_argument('-t', '--timeout', help='Connection timeout, in sec.', default=None, type=float)
    cparser.add_argument(      '--servers', help='A comma-separated list of host:server addresses')

    parser.add_argument('-P', '--param',        help='Additional HTTP parameters in the form of param=value, can be specified more than once', action='append', default=[])
    parser.add_argument('-r', '--reload',       help='Reload the module implementing the command', action='store_true')

    parser.add_argument('-m', '--mime-type',    help='The mime-type', default='application/octet-stream')
    parser.add_argument('-s', '--show-status',  help='Display the status of the command', action='store_true')
    parser.add_argument('-o', '--output',       help='File/directory where to store the retrieved data')

    parser.add_argument('-a', '--async',         help='Run command asynchronously', action='store_true')
    parser.add_argument('-A', '--auth',          help='BASIC authorization string')
    parser.add_argument('-F', '--force',         help='Force the action', action='store_true')
    parser.add_argument('-f', '--file-id',       help='Indicates a File ID')
    parser.add_argument(      '--file-version',  help='A file version', type=int)
    parser.add_argument(      '--file-id-list',  help='A list of File IDs')
    parser.add_argument(      '--file-uri',      help='A File URI')
    parser.add_argument(      '--file-info-xml', help='An XML File Info string')
    parser.add_argument('-n', '--no-versioning', help='Do not increase the file version', action='store_true')
    parser.add_argument('-d', '--disk-id',       help='Indicates a Disk ID')
    parser.add_argument('-e', '--execute',       help='Executes the action', action='store_true')
    parser.add_argument(      '--path',          help='File path')
    parser.add_argument(      '--slot-id',       help='The Slot ID for the label')
    parser.add_argument(      '--p-plugin',      help='Processing plug-in to apply before retrieving data')
    parser.add_argument(      '--p-plugin-pars', help='Parameters for the processing plug-in, can be specified more than once', action='append')

    sparser = parser.add_argument_group('Subscription options')
    sparser.add_argument('-u', '--url',           help='URL to subscribe/unsubscribe')
    sparser.add_argument(      '--priority',      help='Priority used for subscription')
    sparser.add_argument(      '--f-plugin',      help='Filtering plug-in to use for this subscription')
    sparser.add_argument(      '--f-plugin-pars', help='Parameters for the filtering plug-in, can be specified more than once', action='append')
    sparser.add_argument('-S', '--start-date',    help='Start date for subscription')

    cparser = parser.add_argument_group('Container options')
    cparser.add_argument('-c', '--container-id',        help='Specifies a Container ID')
    cparser.add_argument(      '--container-name',      help='Specifies a Container name')
    cparser.add_argument(      '--container-hierarchy', help='Specifies a Container Hierarchy as an XML document')
    cparser.add_argument(      '--parent-container-id', help='Specifies a Parent Container ID')
    cparser.add_argument('-C', '--close',               help='Closes the container after appending a file', action='store_true')
    cparser.add_argument(      '--recursive',           help='Recursively remove containers', action='store_true')

    opts = parser.parse_args()
    setup_logging(opts)

    if opts.version:
        print getNgamsVersion()
        return

    if opts.license:
        print getNgamsLicense()
        return

    if opts.servers:
        servers = [(host, int(port)) for s in opts.servers.split(',') for host,port in s.split(':')]
        client = ngamsPClient(servers=servers, timeout=opts.timeout,
                              auth=opts.auth, reload_mod=opts.reload)
    else:
        client = ngamsPClient(opts.host, opts.port, timeout=opts.timeout,
                              auth=opts.auth, reload_mod=opts.reload)

    # Invoke the proper operation.
    cmd = opts.cmd
    mtype = opts.mime_type
    pars = [(name, val) for p in opts.param for name,val in p.split('=')]
    if cmd in [NGAMS_ARCHIVE_CMD, 'QARCHIVE']:
        pars += [('file_version', opts.file_version)] if opts.file_version is not None else []
        stat = client.archive(opts.file_uri, mtype, opts.async, opts.no_versioning, cmd=cmd, pars=pars)
    elif cmd == "CARCHIVE":
        stat = client.carchive(opts.file_uri, mtype)
    elif cmd == "CAPPEND":
        stat = client.cappend(opts.file_id, opts.file_id_list, opts.container_id, opts.container_name, opts.force, opts.close)
    elif cmd == "CCREATE":
        stat = client.ccreate(opts.container_name, opts.parent_container_id, opts.container_hierarchy)
    elif cmd == "CDESTROY":
        stat = client.cdestroy(opts.container_name, opts.container_id, opts.recursive)
    elif cmd == "CLIST":
        stat = client.clist(opts.container_name, opts.container_id)
    elif cmd == "CREMOVE":
        stat = client.cremove(opts.file_id, opts.file_id_list, opts.container_id, opts.container_name)
    elif cmd == 'CRETRIEVE':
        stat = client.cretrieve(opts.container_name, opts.container_id, opts.output)
    elif (cmd == NGAMS_CACHEDEL_CMD):
        pars.append(("disk_id", opts.disk_id))
        pars.append(("file_id", opts.file_id))
        pars.append(("file_version", str(opts.file_version)))
        stat = client.get_status(cmd, pars=pars)
    elif (cmd == NGAMS_CLONE_CMD):
        stat = client.clone(opts.file_id, opts.disk_id, opts.file_version, opts.async)
    elif (cmd == NGAMS_EXIT_CMD):
        stat = client.exit()
    elif (cmd == NGAMS_INIT_CMD):
        stat = client.init()
    elif (cmd == NGAMS_LABEL_CMD):
        stat = client.label(opts.slot_id)
    elif (cmd == NGAMS_OFFLINE_CMD):
        stat = client.offline(opts.force)
    elif (cmd == NGAMS_ONLINE_CMD):
        stat = client.online()
    elif (cmd == NGAMS_REARCHIVE_CMD):
        if not opts.file_info_xml:
            msg = "Must specify parameter -fileInfoXml for a REARCHIVE Command"
            raise Exception(msg)
        stat = client.reArchive(opts.file_uri, opts.file_info_xml, pars)
    elif (cmd == NGAMS_REGISTER_CMD):
        stat = client.register(opts.path, opts.async)
    elif (cmd == NGAMS_REMDISK_CMD):
        stat = client.remDisk(opts.disk_id, opts.execute)
    elif (cmd == NGAMS_REMFILE_CMD):
        stat = client.remFile(opts.disk_id, opts.file_id, opts.file_version, opts.execute)
    elif (cmd == NGAMS_RETRIEVE_CMD):
        stat = client.retrieve2File(opts.file_id, opts.file_version, opts.output,
                                  opts.p_processing, opts.p_processing_pars,
                                  containerName=opts.container_name,
                                  containerId=opts.container_id, cmd=cmd)
    elif (cmd == NGAMS_STATUS_CMD):
        stat = client.status()
    elif (cmd == NGAMS_SUBSCRIBE_CMD):
        stat = client.subscribe(opts.url, opts.priority, opts.start_date, opts.f_plugin, opts.f_plugin_pars)
    elif (cmd == NGAMS_UNSUBSCRIBE_CMD):
        stat = client.unsubscribe(opts.url)
    else:
        stat = client.get_status(cmd, pars=pars)

    if opts.verbose > 3:
        printStatus(stat)

    if opts.show_status:
        print stat.genXml(0, 1, 1, 1).toprettyxml('  ', '\n')[0:-1]
        print stat.getStatus()

    if stat.getStatus() == NGAMS_FAILURE:
        sys.exit(1)

def printStatus(stat):
    """
    Pretty print the return status document

    Input:
       stat:   an ngamsStatus document
    """
    message = """
Status of request:
Request Time:   {0}
Host ID:        {1}
Message:        {2}
Status:         {3}
State:          {4}
Sub-State:      {5}
NG/AMS Version: {6}
    """
    req_time = ""
    if stat.getRequestTime() is not None:
        req_time = toiso8601(stat.getRequestTime())
    print message.format(
                         req_time,
                         stat.getHostId(),
                         stat.getMessage(),
                         stat.getStatus(),
                         stat.getState(),
                         stat.getSubState(),
                         stat.getVersion(),
                         )

if __name__ == '__main__':
    """
    Main function instantiating and invoking the command handler class
    and printing the result to stdout.
    """
    main()

# EOF