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
import logging
import os
import random
import socket
import sys
import time

from ngamsLib import ngamsLib, ngamsFileInfo, ngamsStatus
from ngamsLib.ngamsCore import TRACE, NGAMS_ARCHIVE_CMD, NGAMS_REARCHIVE_CMD, NGAMS_HTTP_PAR_FILENAME, NGAMS_HTTP_HDR_FILE_INFO, NGAMS_HTTP_HDR_CONTENT_TYPE,\
    NGAMS_LABEL_CMD, NGAMS_ONLINE_CMD, NGAMS_OFFLINE_CMD, NGAMS_REMDISK_CMD,\
    NGAMS_REMFILE_CMD, NGAMS_REGISTER_CMD, NGAMS_RETRIEVE_CMD, NGAMS_STATUS_CMD,\
    NGAMS_FAILURE, NGAMS_SUBSCRIBE_CMD, NGAMS_UNSUBSCRIBE_CMD, NGAMS_ARCH_REQ_MT,\
    NGAMS_CACHEDEL_CMD, NGAMS_CLONE_CMD,\
    NGAMS_HTTP_REDIRECT, getNgamsVersion, NGAMS_SUCCESS, NGAMS_ONLINE_STATE,\
    NGAMS_IDLE_SUBSTATE, getNgamsLicense, toiso8601
from ngamsLib.ngamsCore import NGAMS_EXIT_CMD, NGAMS_INIT_CMD
from xml.dom import minidom


logger = logging.getLogger(__name__)

logging_levels = {
    0: logging.CRITICAL,
    1: logging.ERROR,
    2: logging.WARNING,
    3: logging.INFO,
    4: logging.DEBUG,
    5: logging.NOTSET
}

class ngamsPClient:
    """
    Class providing services for sending and receiving commands to/from
    the NG/AMS Server.
    """

    def __init__(self, host=None, port=None, servers=None, timeout=None, auth=None):
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
        T = TRACE()
        logger.info("Archiving file with URI: %s", fileUri)
        pars.append(('async', '1' if async else '0'))
        if (ngamsLib.isArchivePull(fileUri)):
            pars += [["filename", fileUri],
                        ["no_versioning", str(noVersioning)]]
            if mimeType:
                pars.append(["mime_type", mimeType])
            return self.sendCmd(cmd, pars=pars)
        else:
            res = self.pushFile(fileUri, mimeType, noVersioning, pars, cmd=cmd)
        return res


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
        if (ngamsLib.isArchivePull(fileUri)):
            tmpFileInfo = ngamsFileInfo.ngamsFileInfo().\
                          unpackXmlDoc(fileInfoXml)
            encFileInfo = base64.b64encode(fileInfoXml)
            locPars.append([NGAMS_HTTP_PAR_FILENAME, fileUri])
            httpHdrs = [[NGAMS_HTTP_HDR_FILE_INFO, encFileInfo],
                        [NGAMS_HTTP_HDR_CONTENT_TYPE, tmpFileInfo.getFormat()]]
            res = self.sendCmd(NGAMS_REARCHIVE_CMD, pars=locPars, hdrs=httpHdrs)
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
        return self.sendCmd(NGAMS_CLONE_CMD, pars=pars)

    def carchive(self, fileUri, reloadMod=False):
        """
        Sends a CARCHIVE command to the NG/AMS Server to archive
        a full hierarchy of files, effectively creating a hierarchy of
        containers with all the files within
        """
        pars = []
        if reloadMod:
            pars.append(['reload', 1])
        return self.archive(fileUri, pars=pars, cmd="CARCHIVE")

    def cappend(self, fileId, fileIdList='', containerId=None, containerName=None, force=False, closeContainer=False, reloadMod=False):
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
        if reloadMod:
            pars.append(['reload', 1])
        if force:
            pars.append(['force', 1])
        if closeContainer:
            pars.append(['close_container', 1])

        if fileId:
            pars.append(['file_id', fileId])
            return self.sendCmd('CAPPEND', pars=pars)
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
            return self.post_data('CAPPEND', 'text/xml', pars, fileListXml)

    def ccreate(self, containerName, parentContainerId=None, containerHierarchy=None, reloadMod=False):
        """
        Sends a CCREATE command to the NG/AMS Server to create a container
        or a container hierarchy
        """
        if not (bool(containerName) ^ bool(containerHierarchy)):
            raise Exception('Either a container name or container hierarchy must be indicated to create container/s')

        pars = []
        if reloadMod:
            pars.append(['reload', reloadMod])

        if containerName:
            pars.append(['container_name', containerName])
            if parentContainerId:
                pars.append(['parent_container_id', parentContainerId])
            return self.sendCmd('CCREATE', pars=pars)
        else:
            contHierarchyXml = containerHierarchy
            return self.post_data('CCREATE', 'text/xml', pars, contHierarchyXml)

    def cdestroy(self, containerName, containerId=None, recursive=False, reloadMod=None):
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
        if reloadMod:
            pars.append(['reload', 1])
        if recursive:
            pars.append(['recursive', 1])

        return self.sendCmd('CDESTROY', pars=pars)

    def clist(self, containerName, containerId=None, reloadMod=False):
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
        if reloadMod:
            pars.append(['reload', 1])

        return self.sendCmd('CLIST', pars=pars)

    def cremove(self, fileId, fileIdList='', containerId=None, containerName=None, reloadMod=False):
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
        if reloadMod:
            pars.append(['reload', 1])

        if fileId:
            pars.append(['file_id', fileId])
            return self.sendCmd('CREMOVE', pars=pars)
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
            return self.post_data('CREMOVE', 'text/xml', pars, fileListXml)


    def cretrieve(self, containerName, containerId=None, targetDir='.', reloadMod=False):
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
        return self.retrieve2File(None, targetFile=targetDir, containerName=containerName, containerId=containerId, cmd="CRETRIEVE", reloadMod=reloadMod)

    def exit(self):
        """
        Send an EXIT command to the NG/AMS Server associated to the object.

        Returns:   NG/AMS Status object (ngamsStatus).
        """
        return self.sendCmd(NGAMS_EXIT_CMD)


    def init(self):
        """
        Send an INIT command to the NG/AMS Server associated to the object.

        Returns:   NG/AMS Status object (ngamsStatus).
        """
        return self.sendCmd(NGAMS_INIT_CMD)


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
        return self.sendCmd(NGAMS_LABEL_CMD, pars=pars)


    def online(self):
        """
        Send an ONLINE command to the NG/AMS Server associated to the object.

        Returns:   NG/AMS Status object (ngamsStatus).
        """
        return self.sendCmd(NGAMS_ONLINE_CMD)


    def offline(self, force=False):
        """
        Send an OFFLINE command to the NG/AMS Server associated to the object.

        force:     If set to 1 the NG/AMS Server will be forced to
                   go Offline (integer).

        Returns:   NG/AMS Status object (ngamsStatus).
        """
        pars = [('force', "1" if force else "0")]
        return self.sendCmd(NGAMS_OFFLINE_CMD, pars=pars)


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
        return self.sendCmd(NGAMS_REMDISK_CMD, pars=pars)


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
        return self.sendCmd(NGAMS_REMFILE_CMD, pars=pars)


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
        return self.sendCmd(NGAMS_REGISTER_CMD, pars=pars)


    def retrieve2File(self,
                      fileId,
                      fileVersion = -1,
                      targetFile = "",
                      processing = "",
                      processingPars = "",
                      containerName = None,
                      containerId = None,
                      cmd = NGAMS_RETRIEVE_CMD,
                      reloadMod = False):
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

        containerName:   Name of a container to retrieve
                         (string).

        cmd:             The actual command to send.
                         (string).

        Returns:         NG/AMS Status object (ngamsStatus).
        """
        # If the target file is not specified, we give the
        # current working directory as target.
        pars = []
        if (targetFile == ""): targetFile = os.path.abspath(os.curdir)

        logger.debug('Requesting data with cmd=%s, fileId=%s, containerId=%s, containerName=%s', cmd, fileId, containerId, containerName)
        if cmd == NGAMS_RETRIEVE_CMD:
            if fileId: pars.append(["file_id", fileId])
        elif cmd == 'CRETRIEVE':
            if containerId: pars.append(["container_id", containerId])
            if containerName: pars.append(["container_name", containerName])

        if (fileVersion != -1): pars.append(["file_version", str(fileVersion)])
        if (processing != ""):
            pars.append(["processing", processing])
            if (processingPars != ""):
                pars.append(["processingPars", processingPars])
        if reloadMod: pars.append(['reload', 1])

        return self.sendCmd(cmd, targetFile, pars)


    def status(self):
        """
        Request a general status from the NG/AMS Server
        associated to the object.

        Returns:     NG/AMS Status object (ngamsStatus).
        """
        T = TRACE()

        stat = self.sendCmd(NGAMS_STATUS_CMD)
        return stat


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
        return self.sendCmd(NGAMS_SUBSCRIBE_CMD, pars=pars)


    def unsubscribe(self,
                    url):
        """
        Unsubscribe a Data Subscription.

        url:                Subscriber URL used when subscribing to the
                            Data Provider (string).

        Returns:            NG/AMS Status object (ngamsStatus).
        """
        pars = [("url", url)]
        return self.sendCmd(NGAMS_UNSUBSCRIBE_CMD, pars=pars)


    def pushFile(self,
                 fileUri,
                 mimeType = "",
                 noVersioning = 0,
                 pars = [],
                 cmd = NGAMS_ARCHIVE_CMD):
        """
        Handle an Archive Push Request.

        fileUri:       URI of file to archive (string).

        mimeType:      Mime-type of the data. Must be given if it is not
                       possible to determine the mime-type of the data
                       file to be archived from its filename (string).

        noVersioning:  If 1 no new version number will be generated for
                       the file being archived (integer).

        pars:          Extra parameters to submit with the request. Must be
                       in the format:

                         [[<Par>, <Val>], [<Par>, <Val>], ...]    (list).

        cmd:           Command to issue with the request if different from
                       a normal ARCHIVE Command (string).

        Returns:       NG/AMS Status Object (ngamsStatus).
        """
        mt = mimeType or NGAMS_ARCH_REQ_MT
        pars += [["attachment; filename", os.path.basename(fileUri)]]
        pars += [["no_versioning", str(noVersioning)]]
        return self.post_file(cmd, mt, pars, fileUri)


    def sendCmd(self, cmd, outputFile=None, pars=[], additional_hdrs=[]):
        """
        Send a command to the NG/AMS Server and receives the reply.
        """

        if self.timeout:
            pars.append(["time_out", str(self.timeout)])

        # For now we try the serves in random order.
        serverList = list(self.servers)
        random.shuffle(serverList)
        logger.debug("Server list: %s", str(serverList))

        # Try, if necessary, contacting all servers from the server list
        for i,host_port in enumerate(serverList):
            host, port = host_port
            try:
                reply, msg, hdrs, data = self.do_get(host, port, cmd, pars, outputFile, additional_hdrs)
            except socket.error:
                if i == len(serverList) - 1:
                    raise
                logger.info("Failed to contact server %s:%d, trying next one", host, port)
                pass

        # Handle redirects, with a maximum of 5
        redirects = 0
        while redirects < 5:

            # Last result was not a redirect, return
            if reply != NGAMS_HTTP_REDIRECT:

                # Prepare the 
                if data and "<?xml" in data:
                    return ngamsStatus.ngamsStatus().unpackXmlDoc(data, 1)

                # Create a dummy reply. This can be removed when the server
                # sends back a multipart message which always contains the
                # NG/AMS Status apart from the data at a RETRIEVE Request.
                stat = ngamsStatus.ngamsStatus().\
                       setDate(toiso8601()).\
                       setVersion(getNgamsVersion()).setHostId(host).\
                       setStatus(NGAMS_SUCCESS).\
                       setMessage("Successfully handled request").\
                       setState(NGAMS_ONLINE_STATE).\
                       setSubState(NGAMS_IDLE_SUBSTATE)
                if data:
                    stat.setData(data)

                return stat

            # Get the host + port of the alternative URL, and send
            # the same query again.
            hdrDic = ngamsLib.httpMsgObj2Dic(hdrs)
            host, port = hdrDic["location"].split("/")[2].split(":")
            port = int(port)
            logger.info("Redirecting to NG/AMS running on %s:%d", host, port)

            redirects += 1
            reply, msg, hdrs, data = self.do_get(host, port, cmd, pars, outputFile, additional_hdrs)

        raise Exception("Too many redirections, aborting")


    def do_get(self, host, port, cmd, pars, output, additional_hdrs):

        auth = None
        if self.auth is not None:
            auth = "Basic %s" % self.auth

        start = time.time()
        res = ngamsLib.httpGet(host, port, cmd, pars, output, None, self.timeout, auth, additional_hdrs)
        delta = time.time() - start
        logger.debug("Command: %s/%s to %s:%d handled in %.3fs", cmd, str(pars), host, port, delta)
        return res


    def do_post(self, host, port, cmd, mimeType,
                dataRef = "",
                dataSource = "BUFFER",
                pars = [],
                dataTargFile = "",
                fileName = "",
                dataSize = -1):

        auth = None
        if self.auth is not None:
            auth = "Basic %s" % self.auth

        if self.timeout:
            pars.append(["time_out", str(self.timeout)] )

        start = time.time()
        res = ngamsLib.httpPost(host, port, cmd, mimeType,
                                dataRef, dataSource, pars,
                                dataTargFile, self.timeout, auth,
                                fileName, dataSize)
        delta = time.time() - start
        logger.info("Successfully completed command %s in %.3f [s]", cmd, delta)
        return res

    def post_file(self, cmd, mime_type, pars, fname):

        # For now we try the serves in random order.
        servers = self.servers
        random.shuffle(servers)

        for i,host_port in enumerate(servers):
            host, port = host_port
            try:
                _,_,_,data = self.do_post(host, port, cmd, mime_type,
                                          dataRef = fname,
                                          dataSource = 'FILE',
                                          pars = pars)
                return ngamsStatus.ngamsStatus().unpackXmlDoc(data, 1)
            except socket.error:
                if i == len(servers) - 1:
                    raise

    def post_data(self, cmd, mime_type, pars, data):

        # For now we try the serves in random order.
        servers = self.servers
        random.shuffle(servers)

        for i,host_port in enumerate(servers):
            host, port = host_port
            try:
                _,_,_,data = self.do_post(host, port, cmd, mime_type,
                                          dataRef = data,
                                          dataSource = 'BUFFER',
                                          pars = pars)
                return ngamsStatus.ngamsStatus().unpackXmlDoc(data, 1)
            except socket.error:
                if i == len(servers) - 1:
                    raise

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

    parser.add_argument('-m', '--mime-type',    help='The mime-type', default='application/octet-stream')
    parser.add_argument('-s', '--show-status',  help='Display the status of the command', action='store_true')
    parser.add_argument('-o', '--output',       help='File/directory where to store the retrieved data')

    parser.add_argument('-a', '--async',         help='Run command asynchronously', action='store_true')
    parser.add_argument('-A', '--auth',          help='BASIC authorization string')
    parser.add_argument('-F', '--force',         help='Force the action', action='store_true')
    parser.add_argument('-f', '--file-id',       help='Indicates a File ID')
    parser.add_argument(      '--file-version',  help='A file version')
    parser.add_argument(      '--file-id-list',  help='A list of File IDs')
    parser.add_argument(      '--file-uri',      help='A File URI')
    parser.add_argument(      '--file-info-xml', help='An XML File Info string')
    parser.add_argument('-n', '--no-versioning', help='Do not increase the file version', action='store_true')
    parser.add_argument('-d', '--disk-id',       help='Indicates a Disk ID')
    parser.add_argument('-e', '--execute',       help='Executes the action', action='store_true')
    parser.add_argument('-r', '--reload',        help='Reload the module implementing the command', action='store_true')
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

    logging.root.addHandler(logging.NullHandler())
    if opts.verbose:
        fmt = '%(asctime)-15s.%(msecs)03d [%(threadName)10.10s] [%(levelname)6.6s] %(name)s#%(funcName)s:%(lineno)s %(message)s'
        datefmt = '%Y-%m-%dT%H:%M:%S'
        formatter = logging.Formatter(fmt, datefmt=datefmt)
        formatter.converter = time.gmtime
        hnd = logging.StreamHandler(stream=sys.stdout)
        hnd.setFormatter(formatter)
        logging.root.addHandler(hnd)
        logging.root.setLevel(logging_levels[opts.verbose-1])

    if opts.version:
        print getNgamsVersion()
        return

    if opts.license:
        print getNgamsLicense()
        return

    if opts.servers:
        servers = [(host, int(port)) for s in opts.servers.split(',') for host,port in s.split(':')]
        client = ngamsPClient(servers=servers, timeout=opts.timeout, auth=opts.auth)
    else:
        client = ngamsPClient(opts.host, opts.port, timeout=opts.timeout, auth=opts.auth)

    # Invoke the proper operation.
    cmd = opts.cmd
    mtype = opts.mime_type
    reload_mod = opts.reload
    pars = [(name, val) for p in opts.param for name,val in p.split('=')]
    if cmd in [NGAMS_ARCHIVE_CMD, 'QARCHIVE']:
        stat = client.archive(opts.file_uri, mtype, opts.async, opts.no_versioning, cmd=cmd, pars=[['reload', reload_mod]])
    elif cmd == "CARCHIVE":
        stat = client.carchive(opts.file_uri, reload_mod)
    elif cmd == "CAPPEND":
        stat = client.cappend(opts.file_id, opts.file_id_list, opts.container_id, opts.container_name, opts.force, opts.close, reload_mod)
    elif cmd == "CCREATE":
        stat = client.ccreate(opts.container_name, opts.parent_container_id, opts.container_hierarchy, reload_mod)
    elif cmd == "CDESTROY":
        stat = client.cdestroy(opts.container_name, opts.container_id, opts.recursive, reload_mod)
    elif cmd == "CLIST":
        stat = client.clist(opts.container_name, opts.container_id, reload_mod)
    elif cmd == "CREMOVE":
        stat = client.cremove(opts.file_id, opts.file_id_list, opts.container_id, opts.container_name, reload_mod)
    elif cmd == 'CRETRIEVE':
        stat = client.cretrieve(opts.container_name, opts.container_id, opts.output, reload_mod)
    elif (cmd == NGAMS_CACHEDEL_CMD):
        pars.append(["disk_id", opts.disk_id])
        pars.append(["file_id", opts.file_id])
        pars.append(["file_version", str(opts.file_version)])
        stat = client.sendCmd(cmd, pars=pars)
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
        if (not opts.file_info_xml):
            msg = "Must specify parameter -fileInfoXml for " +\
                  "a REARCHIVE Command"
            raise Exception, msg
        stat = client.reArchive(opts.file_u, opts.file_info_xml, pars) # no parArray in noDebug()
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
        stat = client.sendCmd(cmd, pars=pars)

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