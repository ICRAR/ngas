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

import os, sys, random, time, traceback, base64
from pccUt import PccUtTime
from ngamsLib import ngamsLib, ngamsFileInfo, ngamsStatus
from ngamsLib.ngamsCore import TRACE, NGAMS_ARCHIVE_CMD, NGAMS_REARCHIVE_CMD, NGAMS_HTTP_PAR_FILENAME, NGAMS_HTTP_HDR_FILE_INFO, NGAMS_HTTP_HDR_CONTENT_TYPE,\
    NGAMS_LABEL_CMD, NGAMS_ONLINE_CMD, NGAMS_OFFLINE_CMD, NGAMS_REMDISK_CMD,\
    NGAMS_REMFILE_CMD, NGAMS_REGISTER_CMD, NGAMS_RETRIEVE_CMD, NGAMS_STATUS_CMD,\
    NGAMS_FAILURE, NGAMS_SUBSCRIBE_CMD, NGAMS_UNSUBSCRIBE_CMD, NGAMS_ARCH_REQ_MT,\
    setLogCond, NGAMS_CACHEDEL_CMD, NGAMS_CLONE_CMD,\
    NGAMS_HTTP_REDIRECT, getNgamsVersion, NGAMS_SUCCESS, NGAMS_ONLINE_STATE,\
    NGAMS_IDLE_SUBSTATE, getNgamsLicense
from ngamsLib.ngamsCore import NGAMS_EXIT_CMD, NGAMS_INIT_CMD
from ngamsLib.ngamsCore import info, notice
from xml.dom import minidom
import pkg_resources


manPage = pkg_resources.resource_string(__name__, 'doc/ngamsPClient.txt')  # @UndefinedVariable
__doc__ += "\n\n\nMan-Page for the NG/AMS Python Client Tool:\n\n"
__doc__ += "ngamsPClient " + manPage


class ngamsPClient:
    """
    Class providing services for sending and receiving commands to/from
    the NG/AMS Server.

    Invoke the utility without input parameters to get a man page.
    """

    def __init__(self,
                 host = "",
                 port = -1,
                 timeOut = None):
        """
        Constructor method.
        """
        if not host:
            host = 'localhost'
        self.setHost(host).setPort(port).setStatus(0).setTimeOut(timeOut)
        self.setAuthorization(None)
        self.__servers = []


    def setHost(self,
                host):
        """
        Set host name of object.

        host:    Name of host (string).

        Return:  Reference to object itself.
        """
        self.__host = host
        return self


    def getHost(self):
        """
        Return host name.

        Returns: Host name (string).
        """
        return self.__host


    def setPort(self,
                port):
        """
        Set the port number used by the object.

        port:     Port number (integer).

        Returns:  Reference to object itself.
        """
        self.__port = port
        return self


    def getPort(self):
        """
        Return port number.

        Returns:   Port number (integer).
        """
        return self.__port


    def setStatus(self,
                  status):
        """
        Set the Display Status Flag.

        status:     If 1, status will be displayed (on stdout) (integer).

        Returns:    Reference to object itself.
        """
        self.__status = status
        return self


    def getStatus(self):
        """
        Return Display Status Flag.

        Returns:   Display Status Flag (integer).
        """
        return self.__status


    def setTimeOut(self,
                   timeOut):
        """
        Set the timeout to apply when waiting for reply from the server.

        timeOut:    Timeout in seconds (double).

        Returns:    Reference to object itself.
        """
        if (timeOut):
            self.__timeOut = float(timeOut)
        else:
            self.__timeOut = None
        return self


    def getTimeOut(self):
        """
        Return the timeout.

        Returns:   Timeout in seconds (double).
        """
        return self.__timeOut


    def archive(self,
                fileUri,
                mimeType = "",
                wait = 1,
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

        wait:          If set to 0 there will not be waited for termination of
                       the command handling (integer).

        noVersioning:  If set to 1 no new version number is
                       generated (integer).

        pars:          Extra parameters to submit with the request. Must be
                       in the format:

                         [[<Par>, <Val>], [<Par>, <Val>], ...]    (list).

        Returns:       NG/AMS Status object (ngamsStatus).
        """
        T = TRACE()
        info(1,"Archiving file with URI: " + fileUri)
        locPars = []
        for par in pars: locPars.append(par)
        if (ngamsLib.isArchivePull(fileUri)):
            locPars += [["filename", fileUri],
                        ["no_versioning", str(noVersioning)]]
            if (mimeType != ""): locPars.append(["mime_type", mimeType])
            res = self.sendCmd(cmd, wait, "", locPars)
        else:
            res = self.pushFile(fileUri, mimeType, wait, noVersioning, locPars, cmd=cmd)
        info(1,"Archive request for file: " + fileUri + " issued.")
        return res


    def reArchive(self,
                  fileUri,
                  fileInfoXml,
                  wait = 1,
                  pars = []):
        """
        Send a  Re-archive Request to the associated NG/AMS Server asking to
        have the file specified by the URI archived. This can either
        be a Re-archive Push Request or a Re-archive Pull Request.

        fileUri:       URI of file to archive (string).

        fileInfoXml:   NG/AMS XML File Information for the source file used
                       as reference (string/XML).

        wait:          If set to 0 there will not be waited for termination of
                       the command handling (integer).

        pars:          Extra parameters to submit with the request. Must be
                       in the format:

                         [[<Par>, <Val>], [<Par>, <Val>], ...]    (list).

        Returns:       NG/AMS Status object (ngamsStatus).
        """
        baseName = os.path.basename(fileUri)
        info(1, "Re-archiving file with URI: " + baseName)
        locPars = []
        for par in pars: locPars.append(par)
        if (ngamsLib.isArchivePull(fileUri)):
            tmpFileInfo = ngamsFileInfo.ngamsFileInfo().\
                          unpackXmlDoc(fileInfoXml)
            encFileInfo = base64.b64encode(fileInfoXml)
            locPars.append([NGAMS_HTTP_PAR_FILENAME, fileUri])
            httpHdrs = [[NGAMS_HTTP_HDR_FILE_INFO, encFileInfo],
                        [NGAMS_HTTP_HDR_CONTENT_TYPE, tmpFileInfo.getFormat()]]
            res = self.sendCmdGen(NGAMS_REARCHIVE_CMD, wait, pars = locPars,
                                  additionalHdrs = httpHdrs)
        else:
            msg = "Rearchive Push is not yet supported!"
            notice(msg)
            raise Exception, msg
        info(1,"Re-archive request of file: " + baseName + " issued.")
        return res


    def clone(self,
              fileId,
              diskId,
              fileVersion,
              targetDiskId = "",
              wait = 1):
        """
        Send an CLONE command to the NG/AMS Server associated to the object.

        fileId:        ID of file to clone (string).

        diskId:        ID of disk where file(s) to be cloned are
                       stored (string).

        fileVersion:   Version of file (integer).

        targetDiskId:  ID of disk where the files cloned should be stored
                       (string).

        wait:          If set to 0 there will not be waited for termination of
                       the command handling (integer/0|1).

        Returns:       NG/AMS Status object (ngamsStatus).
        """
        pars = []
        if (fileId): pars.append(["file_id", fileId])
        if (diskId): pars.append(["disk_id", diskId])
        if (fileVersion > 0): pars.append(["file_version", fileVersion])
        if (targetDiskId): pars.append(["target_disk_id", targetDiskId])
        pars.append(["wait", str(wait)])
        return self.sendCmd(NGAMS_CLONE_CMD, 0, "", pars)

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
            response = self._httpGet(self.getHost(), self.getPort(), 'CAPPEND', pars=pars)
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
            response = self._httpPost(self.getHost(), self.getPort(), 'CAPPEND', 'text/xml', fileListXml, 'BUFFER', pars, dataSize=len(fileListXml))

        # response = [reply, msg, hdrs, data]
        return ngamsStatus.ngamsStatus().unpackXmlDoc(response[3], 1)

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
            response = self._httpGet(self.getHost(), self.getPort(), 'CCREATE', pars=pars)
        else:
            contHierarchyXml = containerHierarchy
            response = self._httpPost(self.getHost(), self.getPort(), 'CCREATE', 'text/xml', contHierarchyXml, 'BUFFER', pars, dataSize=len(contHierarchyXml))

        # response = [reply, msg, hdrs, data]
        return ngamsStatus.ngamsStatus().unpackXmlDoc(response[3], 1)

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

        # response = [reply, msg, hdrs, data]
        response = self._httpGet(self.getHost(), self.getPort(), 'CDESTROY', pars=pars)
        return ngamsStatus.ngamsStatus().unpackXmlDoc(response[3], 1)

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

        # response = [reply, msg, hdrs, data]
        response = self._httpGet(self.getHost(), self.getPort(), 'CLIST', pars=pars)
        return ngamsStatus.ngamsStatus().unpackXmlDoc(response[3], 1)

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
            response = self._httpGet(self.getHost(), self.getPort(), 'CREMOVE', pars=pars)
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
            response = self._httpPost(self.getHost(), self.getPort(), 'CREMOVE', 'text/xml', fileListXml, 'BUFFER', pars, dataSize=len(fileListXml))

        # response = [reply, msg, hdrs, data]
        return ngamsStatus.ngamsStatus().unpackXmlDoc(response[3], 1)

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
        return self.sendCmd(NGAMS_LABEL_CMD, 0, "", [["slot_id", slotId],
                                                     ["host_id", hostId]])


    def online(self,
               wait = 1):
        """
        Send an ONLINE command to the NG/AMS Server associated to the object.

        wait:      If set to 0 there will not be waited for termination of
                   the command handling (integer).

        Returns:   NG/AMS Status object (ngamsStatus).
        """
        return self.sendCmd(NGAMS_ONLINE_CMD, wait)


    def offline(self,
                force = 0,
                wait = 1):
        """
        Send an OFFLINE command to the NG/AMS Server associated to the object.

        force:     If set to 1 the NG/AMS Server will be forced to
                   go Offline (integer).

        wait:      If set to 0 there will not be waited for termination of
                   the command handling (integer).

        Returns:   NG/AMS Status object (ngamsStatus).
        """
        return self.sendCmd(NGAMS_OFFLINE_CMD, wait, "", [["force", ""]])


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
        return self.sendCmd(NGAMS_REMDISK_CMD, 1, "",
                            [["disk_id", diskId], ["execute", execute]])


    def remFile(self,
                diskId,
                fileId,
                fileVersion = -1,
                execute = 0):
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
        return self.sendCmd(NGAMS_REMFILE_CMD, 1, "",
                            [["disk_id", diskId], ["file_id", fileId],
                             ["file_version", fileVersion],
                             ["execute", execute]])


    def register(self,
                 path,
                 wait = 1):
        """
        Send an REGISTER command to the NG/AMS Server associated to the object.

        path:      Path used as starting point for selecting files for
                   registration (string).

        wait:      If set to 0 there will not be waited for termination of
                   the command handling (integer).

        Returns:   NG/AMS Status object (ngamsStatus).
        """
        return self.sendCmd(NGAMS_REGISTER_CMD, wait, "",
                            [["path", path], ["wait", str(wait)]])


    def retrieve2File(self,
                      fileId,
                      fileVersion = -1,
                      targetFile = "",
                      processing = "",
                      processingPars = "",
                      internal = 0,
                      hostId = "",
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

        internal:        Used to indicate name of an internal file to
                         retrieve (string).

        hostId:          Host ID of host where to pick up internal file
                         (string).

        containerName:   Name of a container to retrieve
                         (string).

        cmd:             The actual command to send.
                         (string).

        Returns:         NG/AMS Status object (ngamsStatus).
        """
        # If the target file is not specified, we give the
        # current working directory as target.
        if (targetFile == ""): targetFile = os.path.abspath(os.curdir)
        if (internal):
            pars = [["internal", internal]]
        elif (fileId == "--CFG--"):
            pars = [["cfg", ""]]
        elif (fileId == "--NG--LOG--"):
            pars = [["ng_log", ""]]
        else:
            info(4, 'Requesting data with cmd={0}, fileId={1}, containerId={2}, containerName={3}'.format(cmd, fileId, containerId, containerName))
            pars = []
            if cmd == NGAMS_RETRIEVE_CMD:
                if fileId: pars.append(["file_id", fileId])
            elif cmd == 'CRETRIEVE':
                if containerId: pars.append(["container_id", containerId])
                if containerName: pars.append(["container_name", containerName])

        if (hostId): pars.append(["host_id", hostId])
        if (fileVersion != -1): pars.append(["file_version", str(fileVersion)])
        if (processing != ""):
            pars.append(["processing", processing])
            if (processingPars != ""):
                pars.append(["processingPars", processingPars])
        if reloadMod: pars.append(['reload', 1])

        return self.sendCmd(cmd, 0, targetFile, pars)


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
        pars = [["url", url], ["priority", priority]]
        if (startDate != ""): pars += ["start_date", startDate]
        if (filterPlugIn != ""): pars += ["filter_plug_in", filterPlugIn]
        if (filterPlugInPars != ""): pars += ["plug_in_pars", filterPlugInPars]
        stat = self.sendCmd(NGAMS_SUBSCRIBE_CMD, 1, "", pars)
        if (stat.getStatus() == NGAMS_FAILURE): return stat

        # Act as HTTP daemon ready to receive data.
        # TODO: Implement!


    def unsubscribe(self,
                    url):
        """
        Unsubscribe a Data Subscription.

        url:                Subscriber URL used when subscribing to the
                            Data Provider (string).

        Returns:            NG/AMS Status object (ngamsStatus).
        """
        pars = [["url", url]]
        return self.sendCmd(NGAMS_UNSUBSCRIBE_CMD, 1, "", pars)


    def _httpPost(self,
                  host,
                  port,
                  cmd,
                  mimeType,
                  dataRef = "",
                  dataSource = "BUFFER",
                  pars = [],
                  dataTargFile = "",
                  timeOut = None,
                  authHdrVal = "",
                  fileName = "",
                  dataSize = -1):
        """
        Issue an HTTP POST Request.

        For a description of the parameters, see ngams.ngamsLib.httpPost().
        """
        T = TRACE()

        # If a list of servers has been specified, use this list.
        if (len(self.__servers) > 0):
            serverList = self.__servers
        else:
            serverList = [(host, port)]
        # For now we try the serves in random order.
        random.shuffle(serverList)

        # Very simple algorithm, should maybe be refined.
        success = 0
        errors = ""
        for tmpHost, tmpPort in serverList:
            try:
                startTime = time.time()
                reply, msg, hdrs, data =\
                       ngamsLib.httpPost(tmpHost, tmpPort, cmd, mimeType,
                                         dataRef, dataSource, pars,
                                         dataTargFile, timeOut, authHdrVal,
                                         fileName, dataSize)
                success = 1
                break
            except Exception, e:
                # Problem contacting server.
                deltaTime = (time.time() - startTime)
                errors += " - Error/%s/%d: %s. Timeout/time: %ss/%.3fs" %\
                          (tmpHost, tmpPort, str(e), str(timeOut), deltaTime)
                continue

        if (success):
            return (reply, msg, hdrs, data)
        else:
            msg = "Error communicating to specified server(s) " +\
                  "(HTTP Post request). Errors:%s" % errors
            raise Exception, msg


    def pushFile(self,
                 fileUri,
                 mimeType = "",
                 wait = 1,
                 noVersioning = 0,
                 pars = [],
                 cmd = NGAMS_ARCHIVE_CMD):
        """
        Handle an Archive Push Request.

        fileUri:       URI of file to archive (string).

        mimeType:      Mime-type of the data. Must be given if it is not
                       possible to determine the mime-type of the data
                       file to be archived from its filename (string).

        wait:          If 0 don't wait for request handling to finish
                       (integer).

        noVersioning:  If 1 no new version number will be generated for
                       the file being archived (integer).

        pars:          Extra parameters to submit with the request. Must be
                       in the format:

                         [[<Par>, <Val>], [<Par>, <Val>], ...]    (list).

        cmd:           Command to issue with the request if different from
                       a normal ARCHIVE Command (string).

        Returns:       NG/AMS Status Object (ngamsStatus).
        """

        if (mimeType):
            mt = mimeType
        else:
            mt = NGAMS_ARCH_REQ_MT
        if (self.getAuthorization()):
            authVal = "Basic %s" % self.getAuthorization()
        else:
            authVal = ""

        httpPars = []
        for par in pars:
            httpPars.append(par)
        httpPars += [["attachment; filename", os.path.basename(fileUri)],
                     ["wait", str(wait)], ["no_versioning", str(noVersioning)]]
        if (self.getTimeOut()):
            httpPars.append(["time_out", self.getTimeOut()])

        reply, msg, hdrs, data =\
                   self._httpPost(self.getHost(), self.getPort(), cmd, mt,
                                  fileUri, dataSource = "FILE",
                                  pars = httpPars,
                                  timeOut = self.getTimeOut(),
                                  authHdrVal = authVal)

        return ngamsStatus.ngamsStatus().unpackXmlDoc(data, 1)


    def setAuthorization(self,
                         accessCode):
        """
        Set the authorization user/password (encrypted) to use for
        accessing the remote NG/AMS Server.

        accessCode:     Encrypted access code (string).

        Returns:        Reference to object itself.
        """
        self.__authorization = accessCode
        return self


    def getAuthorization(self):
        """
        Get the authorization user/password (encrypted) to use for
        accessing the remote NG/AMS Server.

        Returns:        Encrypted access code (string).
        """
        return self.__authorization


    def parseSrvList(self,
                     servers):
        """
        Parse a comma separated list of server nodes and port numbers of
        the form:

            <Host 1>:<Port 1>,<Host 2>:<Port 2>,...

            The server/port pairs are kept in an internal registry to be
            used when communicating with the remote server(s).

        servers:     List of servers/ports (string).

        Returns:     Reference to object itself.
        """
        T = TRACE()

        self._servers = []
        try:
            for serverInfo in servers.split(","):
                host, port = serverInfo.split(":")
                host = host.strip()
                port = int(port.strip())
                self.__servers.append((host, port))
        except Exception, e:
            msg = "Illegal server list specified: %s. Error: %s"
            raise Exception, msg % (servers, str(e))
        return self


    def handleCmd(self,
                  argv):
        """
        Handle the command based on the command line parameters given.

        argv:     Tuple containing the command line parameters (tuple).

        Returns:  Void.
        """
        T = TRACE()
        # Command line parameters.
        cmd              = ""
        contHierarchy    = ''
        closeContainer   = False
        diskId           = ""
        execute          = 0
        fileId           = ""
        fileIdList       = ""
        fileInfoXml      = ""
        fileUri          = ""
        fileVersion      = -1
        filterPlugIn     = ""
        force            = 0
        host             = '127.0.0.1'
        hostId           = ""
        internal         = ""
        mimeType         = ""
        noVersioning     = 0
        plugInPars       = ""
        wait             = 1
        outputFile       = ""
        parentContId     = ''
        path             = ""
        port             = 7777
        priority         = 10
        processing       = ""
        processingPars   = ""
        recursive        = False
        reloadMod        = False
        servers          = ""
        slotId           = ""
        startDate        = ""
        verboseLevel     = 0
        url              = ""
        parArray         = []
        parArrayIdx      = -1
        containerId      = ""
        containerName    = ""

        # Control variables.
        parLen           = len(argv)
        idx              = 1
        while idx < parLen:
            par = argv[idx].lower()
            try:
                if (par == "-auth"):
                    idx = idx + 1
                    self.setAuthorization(argv[idx])
                elif (par == "-v"):
                    idx = idx + 1
                    verboseLevel = int(argv[idx])
                    setLogCond(0, "", 0, "", verboseLevel)
                elif (par == "-cfg"):
                    fileId = "--CFG--"
                elif (par == "-host"):
                    idx = idx + 1
                    host = argv[idx]
                elif (par == "-port"):
                    idx = idx + 1
                    port = int(argv[idx])
                elif (par == "-cmd"):
                    idx = idx + 1
                    cmd = argv[idx]
                elif (par == "-diskid"):
                    idx = idx + 1
                    diskId = argv[idx]
                elif (par == "-execute"):
                    execute = 1
                elif (par == "-fileid"):
                    idx = idx + 1
                    fileId = argv[idx]
                elif (par == "-fileidlist"):
                    idx = idx + 1
                    fileIdList = argv[idx]
                elif (par == '-closecontainer'):
                    closeContainer = True
                elif (par == '-containerhierarchy'):
                    idx = idx + 1
                    contHierarchy = argv[idx]
                elif (par == "-containerid"):
                    idx = idx + 1
                    containerId = argv[idx]
                elif (par == "-containername"):
                    idx = idx + 1
                    containerName = argv[idx]
                elif (par == "-fileinfoxml"):
                    idx = idx + 1
                    fileInfoXml = argv[idx]
                elif (par == "-fileuri"):
                    idx = idx + 1
                    fileUri = argv[idx]
                elif (par == "-fileversion"):
                    idx = idx + 1
                    fileVersion = int(argv[idx])
                elif (par == "-filterplugin"):
                    idx = idx + 1
                    filterPlugIn = argv[idx]
                elif (par == "-internal"):
                    idx = idx + 1
                    internal = argv[idx]
                elif (par == "-hostid"):
                    idx = idx + 1
                    hostId = argv[idx]
                elif (par == "-pluginpars"):
                    idx = idx + 1
                    plugInPars = argv[idx]
                elif (par == "-force"):
                    force = 1
                elif (par == "-license"):
                    print getNgamsLicense()
                    sys.exit(0)
                elif (par == "-mimetype"):
                    idx = idx + 1
                    mimeType = argv[idx]
                elif (par == "-nglog"):
                    fileId = "--NG--LOG--"
                elif (par == "-noversioning"):
                    noVersioning = 1
                elif (par == "-nowait"):
                    wait = 0
                elif (par == "-outputfile"):
                    idx = idx + 1
                    outputFile = argv[idx]
                elif (par == "-par"):
                    idx = idx + 1
                    parArrayIdx += 1
                    parArray.append([argv[idx], ""])
                elif (par == '-parentContainerId'):
                    idx = idx + 1
                    parentContId = argv[idx]
                elif (par == "-path"):
                    idx = idx + 1
                    path = argv[idx]
                elif (par == "-priority"):
                    idx = idx + 1
                    priority = int(argv[idx])
                elif (par == "-processing"):
                    idx = idx + 1
                    processing = argv[idx]
                elif (par == "-processingpars"):
                    idx = idx + 1
                    processingPars = argv[idx]
                elif (par == "-recursive"):
                    recursive = True
                elif (par == "-reloadmod"):
                    reloadMod = True
                elif (par == "-servers"):
                    idx = idx + 1
                    servers = argv[idx]
                elif (par == "-slotid"):
                    idx = idx + 1
                    slotId = argv[idx]
                elif (par == "-status"):
                    self.setStatus(1)
                elif (par == "-startdate"):
                    idx = idx + 1
                    startDate = argv[idx]
                elif (par == "-timeout"):
                    idx = idx + 1
                    self.setTimeOut(argv[idx])
                elif (par == "-val"):
                    idx = idx + 1
                    parArray[parArrayIdx][1] = argv[idx]
                elif (par == "-version"):
                    print getNgamsVersion()
                    sys.exit(0)
                elif (par == "-url"):
                    idx = idx + 1
                    url = argv[idx]
                else:
                    print self.correctUsageBuf()
                    sys.exit(1)
            except Exception:
                print self.correctUsageBuf()
                raise
            idx = idx + 1

        self.verbosity = verboseLevel

        # Check generic input parameters.
        self.setHost(host)
        self.setPort(port)

        if servers != "":
            self.parseSrvList(servers)
        reloadMod = 1 if reloadMod else 0

        if not parArray and not cmd:
            print "Error: Neither a command (-cmd) nor parameters (-par/-val) have been given"
            print self.correctUsageBuf()
            sys.exit(1)

        # Invoke the proper operation.
        if (parArray):
            return self.sendCmdGen(cmd, wait, outputFile, parArray)
        elif (cmd in [NGAMS_ARCHIVE_CMD, 'QARCHIVE']):
            return self.archive(fileUri, mimeType, wait, noVersioning, cmd=cmd, pars=[['reload', reloadMod]])
        elif cmd == "CARCHIVE":
            return self.carchive(fileUri, reloadMod)
        elif cmd == "CAPPEND":
            return self.cappend(fileId, fileIdList, containerId, containerName, force, closeContainer, reloadMod)
        elif cmd == "CCREATE":
            return self.ccreate(containerName, parentContId, contHierarchy, reloadMod)
        elif cmd == "CDESTROY":
            return self.cdestroy(containerName, containerId, recursive, reloadMod)
        elif cmd == "CLIST":
            return self.clist(containerName, containerId, reloadMod)
        elif cmd == "CREMOVE":
            return self.cremove(fileId, fileIdList, containerId, containerName, reloadMod)
        elif cmd == 'CRETRIEVE':
            return self.cretrieve(containerName, containerId, outputFile, reloadMod)
        elif (cmd == NGAMS_CACHEDEL_CMD):
            parArray.append(["disk_id", diskId])
            parArray.append(["file_id", fileId])
            parArray.append(["file_version", str(fileVersion)])
            return self.sendCmdGen(cmd, wait, "", parArray)
        elif (cmd == NGAMS_CLONE_CMD):
            return self.clone(fileId, diskId, fileVersion)
        elif (cmd == NGAMS_EXIT_CMD):
            return self.exit()
        elif (cmd == NGAMS_INIT_CMD):
            return self.init()
        elif (cmd == NGAMS_LABEL_CMD):
            return self.label(slotId)
        elif (cmd == NGAMS_OFFLINE_CMD):
            return self.offline(force, wait)
        elif (cmd == NGAMS_ONLINE_CMD):
            return self.online(wait)
        elif (cmd == NGAMS_REARCHIVE_CMD):
            if (not fileInfoXml):
                msg = "Must specify parameter -fileInfoXml for " +\
                      "a REARCHIVE Command"
                raise Exception, msg
            return self.reArchive(fileUri, fileInfoXml, wait, parArray) # no parArray in noDebug()
        elif (cmd == NGAMS_REGISTER_CMD):
            return self.register(path, wait)
        elif (cmd == NGAMS_REMDISK_CMD):
            return self.remDisk(diskId, execute)
        elif (cmd == NGAMS_REMFILE_CMD):
            return self.remFile(diskId, fileId, fileVersion, execute)
        elif (cmd == NGAMS_RETRIEVE_CMD):
            # return self.retrieve2File(fileId, outputFile, cmd=cmd) # noDebug() version
            return self.retrieve2File(fileId, fileVersion, outputFile,
                                      processing, processingPars,
                                      internal, hostId, containerName=containerName,
                                      containerId=containerId, cmd=cmd)
        elif (cmd == NGAMS_STATUS_CMD):
            return self.status()
        elif (cmd == NGAMS_SUBSCRIBE_CMD):
            return self.subscribe(url, priority, startDate, filterPlugIn, plugInPars)
        elif (cmd == NGAMS_UNSUBSCRIBE_CMD):
            return self.unsubscribe(url)
        else:
            raise Exception, 'Unknown command: ' + cmd


    def _httpGet(self,
                 host,
                 port,
                 cmd,
                 wait = 1,
                 pars = [],
                 dataTargFile = "",
                 blockSize = 65536,
                 timeOut = None,
                 returnFileObj = 0,
                 authHdrVal = "",
                 additionalHdrs = []):
        """
        Issue an HTTP GET Request.

        For a description of the parameters, see ngams.ngamsLib.httpGet().
        """
        T = TRACE()

        # If a list of servers has been specified, use this list.
        if (len(self.__servers) > 0):
            serverList = self.__servers
        else:
            serverList = [(host, port)]
        # For now we try the serves in random order.
        random.shuffle(serverList)
        info(5,"Server list: %s" % str(serverList))

        # Very simple algorithm, should maybe be refined.
        success = 0
        errors = ""
        for tmpHost, tmpPort in serverList:
            try:
                info(5,"Trying server: %s:%s ..." % (tmpHost, str(tmpPort)))
                startTime = time.time()
                reply, msg, hdrs, data =\
                       ngamsLib.httpGet(tmpHost, tmpPort, cmd, wait, pars,
                                        dataTargFile, blockSize, timeOut,
                                        returnFileObj, authHdrVal,
                                        additionalHdrs)
                info(5,"Server: %s:%s OK" % (tmpHost, str(tmpPort)))
                success = 1
                break
            except Exception, e:
                # Problem contacting server.
                deltaTime = (time.time() - startTime)
                errors += " - Error/%s/%d: %s. Timeout/time: %s/%.3f [s]" %\
                          (tmpHost, tmpPort, str(e), str(timeOut), deltaTime)
                continue

        if (success):
            return (reply, msg, hdrs, data)
        else:
            raise Exception, "Error communicating to specified server(s). " +\
                  "(HTTP Get Request) Errors: %s" % errors


    def sendCmdGen(self,
                   cmd,
                   wait = 1,
                   outputFile = "",
                   pars = [],
                   additionalHdrs = [],
                   host=None,
                   port=None):
        """
        Send a command to the NG/AMS Server and receive the reply.

        cmd:              NG/AMS command (string).

        wait:             If set to 0, the NG/AMS Server will generate an
                          immediate reply, i.e.m before possibly terminating
                          handling the request (integer).

        outputFile:       File in which to write data returned by HTTP
                          request (string).

        pars:             Tuple of parameter/values pairs:

                            [[<par>, <val>], [<par>, <val>], ...]

        additionalHdrs:   Additional HTTP headers to send with the request.
                          Must be formatted as:

                            [[<hdr>, <val>], ...]                      (list).

        Returns:          NG/AMS Status Object (ngamsStatus).
        """
        T = TRACE()

        if (self.getAuthorization()):
            authHdrVal = "Basic %s" % self.getAuthorization()
        else:
            authHdrVal = ""

        locPars = []
        for par in pars:
            locPars.append(par)
        if self.getTimeOut():
            locPars.append(["time_out", self.getTimeOut()])

        host = host or self.getHost()
        port = port or self.getPort()
        try:
            startTime = time.time()
            reply, msg, hdrs, data =\
                   self._httpGet(host, port, cmd, wait, locPars, outputFile,
                                 None, self.getTimeOut(), 0, authHdrVal,
                                 additionalHdrs)
            deltaTime = (time.time() - startTime)
            info(3,"Command: %s/%s to %s:%s handled in %.3fs" %\
                 (cmd, str(locPars), host, str(port), deltaTime))
        except Exception, e:
            deltaTime = (time.time() - startTime)
            msg = "Exception raised handling command %s/%s to %s:%s " +\
                  "after %.3fs. Timeout: %s. Error: %s"
            notice(msg % (cmd, str(locPars), host, str(port), deltaTime,
                          str(self.getTimeOut()), str(e)))
            raise

        # If we have received a redirection HTTP response, we
        # send the query again to the alternative location.
        if (reply == NGAMS_HTTP_REDIRECT):
            # Get the host + port of the alternative URL, and send
            # the same query again.
            hdrDic = ngamsLib.httpMsgObj2Dic(hdrs)
            host, port = hdrDic["location"].split("/")[2].split(":")
            info(4,"Redirect to NG/AMS running on host: " + host + " using "+\
                 "port: " + str(port) + " is carried out")
            return self.sendCmdGen(cmd, wait, outputFile, locPars, host=host, port=port)
        else:
            if ((data != "") and (data.find("<?xml") != -1)):
                ngamsStat = ngamsStatus.ngamsStatus().unpackXmlDoc(data, 1)
            else:
                # Create a dummy reply. This can be removed when the server
                # sends back a multipart message which always contains the
                # NG/AMS Status apart from the data at a RETRIEVE Request.
                ngamsStat = ngamsStatus.ngamsStatus().\
                            setDate(PccUtTime.TimeStamp().getTimeStamp()).\
                            setVersion(getNgamsVersion()).setHostId(host).\
                            setStatus(NGAMS_SUCCESS).\
                            setMessage("Successfully handled request").\
                            setState(NGAMS_ONLINE_STATE).\
                            setSubState(NGAMS_IDLE_SUBSTATE)
                if (data != ""): ngamsStat.setData(data)

            info(4, "Returning successfully from ngamsPClient.sendCmdGen()")
            return ngamsStat


    def sendCmd(self,
                cmd,
                wait = 1,
                outputFile = "",
                pars = []):
        """
        Send a command to the NG/AMS Server and receive the reply.

        cmd:         NG/AMS command (string).

        wait:        If set to 0, the NG/AMS Server will generate an
                     immediate reply, i.e.m before possibly terminating
                     handling the request (integer).

        outputFile:  File in which to write data returned by HTTP
                     request (string).

        pars:        Tuple of parameter/values pairs:

                       [[<par>, <val>], [<par>, <val>], ...]

                     These are send as 'Content-Disposition' in the HTTP
                     command (list).

        Returns:     NG/AMS Status Object (ngamsStatus).
        """
        T = TRACE()

        stat = self.sendCmdGen(cmd, wait, outputFile, pars)
        return stat


    def correctUsageBuf(self):
        """
        Generate man-page in string buffer and return this.

        Returns:  Man-page for tool (string).
        """
        global manPage
        buf = "\n"
        buf += "> ngamsPClient "
        buf += manPage
        return buf


def handleCmdLinePars(argv,
                      fo = sys.stdout):
    """
    Function to send off a command based on command line parameters
    contained in a list (as is the case with sys.argv).

    argv:     List with command line parameters (list).

    Returns:  Void.
    """
    ngamsClient = ngamsPClient()
    try:
        ngamsStat = ngamsClient.handleCmd(argv)
        if ngamsClient.verbosity > 0 :
            pprintStatus(ngamsClient, ngamsStat)
    except Exception:
        print(traceback.print_exc())
        sys.exit(1)
    if (ngamsClient.getStatus()):
        fo.write(ngamsStat.genXml(0, 1, 1, 1).toprettyxml('  ', '\n')[0:-1])
        print ngamsStat.getStatus()
    if (ngamsStat == None):
        sys.exit(1)
    elif (ngamsStat.getStatus() == NGAMS_FAILURE):
        sys.exit(1)
    else:
        sys.exit(0)

def pprintStatus(client, stat):
    """
    Pretty print the return status document

    Input:
       stat:   an ngamsStatus document
    """
    message = """
Status of request:
Host:           {0}
Port:           {1}
Status:         {2}

Request Time:   {3}
Host ID:        {4}
Message:        {5}
Status:         {6}
State:          {7}
Sub-State:      {8}
NG/AMS Version: {9}
    """
    print message.format(
                         client.getHost(),
                         client.getPort(),
                         client.getStatus(),
                         stat.getRequestTimeIso(),
                         stat.getHostId(),
                         stat.getMessage(),
                         stat.getStatus(),
                         stat.getState(),
                         stat.getSubState(),
                         stat.getVersion(),
                         )

def main():
    handleCmdLinePars(sys.argv)

if __name__ == '__main__':
    """
    Main function instantiating and invoking the command handler class
    and printing the result to stdout.
    """
    main()

# EOF