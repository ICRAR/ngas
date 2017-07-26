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
# "@(#) $Id: ngamsStatus.py,v 1.8 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  29/05/2001  Created
#
"""
Module that contains the ngamsStatus class used to handle
the NG/AMS Status Report.
"""

import xml.dom.minidom

from ngamsCore import ngamsGetChildNodes, NGAMS_XML_STATUS_ROOT_EL, \
    getAttribValue, TRACE, prFormat1, toiso8601, fromiso8601
import ngamsConfig, ngamsDiskInfo, ngamsFileList
import ngamsContainer


class ngamsStatus:
    """
    Class to handle the NG/AMS Status Report.
    """

    def __init__(self):
        """
        Constructor method.
        """
        self.clear()


    def clear(self):
        """
        Clear the object.

        Returns:   Reference to object itself.
        """
        # Status information (from Status Element in NgamsStatus.dtd).
        self.__date           = ""
        self.__version        = ""
        self.__hostId         = ""
        self.__status         = ""
        self.__message        = ""
        self.__state          = ""
        self.__subState       = ""
        self.__data           = None


        # Request handling status.
        self.__requestId             = None
        self.__requestTime           = None
        self.__completionPercent     = None
        self.__expectedCount         = None
        self.__actualCount           = None
        self.__estTotalTime          = None
        self.__remainingTime         = None
        self.__lastRequestStatUpdate = None
        self.__completionTime        = None

        # NG/AMS Configuration Status information.
        self.__ngamsCfg       = ngamsConfig.ngamsConfig()

        # Status about disks and files.
        self.__diskStatusList = []

        # File Lists.
        self.__fileListList   = []

        # Container
        self.__containers     = []

        return self


    def setDate(self,
                date):
        """
        Set the date for generating the status report.

        date:      ISO 8601 Date for generating status (string).

        Returns:   Reference to object itself.
        """
        self.__date = str(date)
        return self


    def getDate(self):
        """
        Get the date for generating the status report.

        Returns:   Date for generating status report (string).
        """
        return self.__date


    def setVersion(self,
                   version):
        """
        Set the version of the NG/AMS generating the status report.

        version:   Version of the NG/AMS generating the status (string).

        Returns:   Reference to object itself.
        """
        self.__version = str(version)
        return self


    def getVersion(self):
        """
        Get the version of the NG/AMS generating the status report.

        Returns:   Version of the NG/AMS generating the status (string).
        """
        return self.__version


    def setHostId(self,
                  id):
        """
        Set the host ID for the application generating the report.

        id:        Host ID (string).

        Returns:   Reference to object itself.
        """
        self.__hostId = str(id)
        return self


    def getHostId(self):
        """
        Get the host ID for the application generating the report.

        Returns:   Host ID (string).
        """
        return self.__hostId


    def setStatus(self,
                  status):
        """
        Set the status of the request to NG/AMS.

        Returns:   Reference to object itself.
        """
        self.__status = str(status)
        return self


    def getStatus(self):
        """
        Get the status of the request to NG/AMS.

        Returns:  Status of request to NG/AMS (string).
        """
        return self.__status


    def setMessage(self,
                   msg):
        """
        Set the message in connection with the status report.

        msg:       Message in connection with status report (string).

        Returns:   Reference to object itself.
        """
        self.__message = str(msg).strip()
        return self


    def getMessage(self):
        """
        Get the message in connection with the status report.

        Returns:   Message in connection with status report (string).
        """
        return self.__message


    def setState(self,
                 state):
        """
        Set the State of NG/AMS.

        state:     State of NG/AMS (string).

        Returns:   Reference to object itself.
        """
        self.__state = str(state)
        return self


    def getState(self):
        """
        Get the State of NG/AMS.

        Returns:   State of NG/AMS (string).
        """
        return self.__state


    def setSubState(self,
                    subState):
        """
        Set the Sub-State of NG/AMS.

        subState:    Sub-State of NG/AMS (string).

        Returns:     Reference to object itself.
        """
        self.__subState = str(subState)
        return self


    def getSubState(self):
        """
        Get the Sub-State of NG/AMS.

        Returns:   Sub-State of NG/AMS (string).
        """
        return self.__subState


    def setRequestId(self,
                     requestId):
        """
        Set the Request ID of the object.

        requestId:    Request ID allocated to the request (string).

        Returns:      Reference to object itself.
        """
        self.__requestId = requestId
        return self


    def getRequestId(self):
        """
        Return the Request ID allocated to the request.

        Returns:      Request ID (string).
        """
        return self.__requestId


    def setRequestTime(self,
                       reqTime):
        """
        Set the time for receiving the request (in seconds since epoch).

        reqTime:    Time for receiving request in seconds since epoch (float).

        Returns:    Reference to object itself.
        """
        self.__requestTime = reqTime
        return self


    def getRequestTime(self):
        """
        Return the time for receiving the request (in seconds since epoch).

        Returns:    Time for receiving request in seconds since epoch (float).
        """
        return self.__requestTime


    def setCompletionPercent(self,
                             complPercent):
        """
        Set the degree of completion of the request (in percen).

        complPercent:   Degree of completion in percent (float).

        Returns:        Reference to object itself.
        """
        if (complPercent):
            self.__completionPercent = float(complPercent)
        else:
            self.__completionPercent = None
        return self


    def getCompletionPercent(self):
        """
        Get the degree of completion of the request (in percen).

        Returns:        Degree of completion in percent (float).
        """
        return self.__completionPercent


    def setExpectedCount(self,
                         expCount):
        """
        Set the expected number of iterations to be carried out. Could
        e.g. be the number of files to handle.

        expCount:    Expected number of iterations (integer).

        Returns:     Reference to object itself.
        """
        if (expCount != None):
            self.__expectedCount = int(expCount)
        else:
            self.__expectedCount = None
        return self


    def getExpectedCount(self):
        """
        Return the expected number of iterations to be carried out. Could
        e.g. be the number of files to handle.

        Returns:  Expected number of iterations (integer).
        """
        return self.__expectedCount


    def setActualCount(self,
                       actCount):
        """
        Set the actual number of iterations completed.

        actCount:    Current number of iterations done (integer).

        Returns:     Reference to object itself.
        """
        if (actCount != None):
            self.__actualCount = int(actCount)
        else:
            self.__actualCount = None
        return self


    def getActualCount(self):
        """
        Return the number of iterations carried out.

        Returns:    Current number of iterations done (integer).
        """
        return self.__actualCount


    def setEstTotalTime(self,
                        estTime):
        """
        Set the estimated total time for handling the request.

        estTime:    Estimated time for completing the request in seconds
                    or as a time string (HH:MM:SS) (integer|string).

        Returns:    Reference to object itself.
        """
        self.__estTotalTime = estTime
        return self


    def getEstTotalTime(self):
        """
        Return the estimated total time for handling the request.

        Returns:    Estimated time for completing the request in seconds
                    (integer).
        """
        return self.__estTotalTime


    def setRemainingTime(self,
                         remainingTime):
        """
        Set the estimated remaining time for handling the request.

        remainingTime:    The remaining time in seconds (integer).

        Returns:          Current number of iterations done (integer).
        """
        self.__remainingTime = remainingTime
        return self


    def getRemainingTime(self):
        """
        Get the estimated remaining time for handling the request.

        Returns:     The remaining time in seconds (integer).
        """
        return self.__remainingTime


    def setLastRequestStatUpdate(self,
                                 lastUpdateTime):
        """
        Set the time for performing the last update of the request
        handling status.

        Returns:    Reference to object itself.
        """
        self.__lastRequestStatUpdate = lastUpdateTime
        return self


    def getLastRequestStatUpdate(self):
        """
        Get the time for performing the last update of the request
        handling status.

        Returns:   Last time for updating the request handling status.
                   Given in seconds since epoch (integer).
        """
        return self.__lastRequestStatUpdate


    def setCompletionTime(self,
                          completionTime):
        """
        Set the time for completing the request to the present time.

        completionTime:  Time in seconds since epoch for when the request
                         handling finished (float).

        Returns:         Reference to object itself.
        """
        self.__completionTime = completionTime
        return self


    def getCompletionTime(self):
        """
        Get the time for completing the request. Given in seconds since epoch.

        Returns:    Time in seconds since epoch for when the request handling
                    finished (float).
        """
        return self.__completionTime


    def setData(self,
                data):
        """
        Set the data member of the object.

        data:     Data to be registered (string).

        Returns:  Reference to object itself.
        """
        self.__data = data
        return self


    def getData(self):
        """
        Return reference to internal data buffer.

        Returns:  Reference to data buffer (string|None).
        """
        return self.__data


    def setReqStatFromReqPropsObj(self,
                                  reqPropsObj):
        """
        Set the request handling status from an NG/AMS Request
        Properties Object.

        reqPropsObj:    Request properties object (ngamsReqProps).

        Returns:        Reference to object itself.
        """
        lastReqUpdate = reqPropsObj.getLastRequestStatUpdate()
        self.\
               setRequestId(reqPropsObj.getRequestId()).\
               setRequestTime(reqPropsObj.getRequestTime()).\
               setCompletionPercent(reqPropsObj.getCompletionPercent()).\
               setExpectedCount(reqPropsObj.getExpectedCount()).\
               setActualCount(reqPropsObj.getActualCount()).\
               setEstTotalTime(reqPropsObj.getEstTotalTime()).\
               setRemainingTime(reqPropsObj.getRemainingTime()).\
               setLastRequestStatUpdate(lastReqUpdate).\
               setCompletionTime(reqPropsObj.getCompletionTime())
        return self


    def addDiskStatus(self,
                      diskStatusObj):
        """
        Add a disk info object in the status report.

        diskStatusObj:  Disk status object (ngamsDiskInfo).

        Returns:        Reference to object itself.
        """
        self.__diskStatusList.append(diskStatusObj)
        return self


    def getDiskStatusList(self):
        """
        Get tuple of disk info objects.

        Returns:  Tuple with disk info objects ([ngamsDiskInfo, ...]).
        """
        return self.__diskStatusList


    def addFileList(self,
                    fileListObj):
        """
        Add a File List Object in the status report.

        fileListObj:    File List object (ngamsFileList).

        Returns:        Reference to object itself.
        """
        self.__fileListList.append(fileListObj)
        return self


    def getFileListList(self):
        """
        Get tuple of File List Objects.

        Returns:  Tuple with File List Objects ([ngamsFileList, ...]).
        """
        return self.__fileListList


    def setNgamsCfgObj(self,
                       ngamsCfgObj):
        """
        Set the internal reference to an instance of the NG/AMS
        Configuration Class.

        ngamsCfgObj:  Reference to NG/AMS Configuration object (ngamsConfig).

        Returns:      Reference to object itself.
        """
        self.__ngamsCfg = ngamsCfgObj
        return self

    def addContainer(self, container):
        """
        Add a Container in the status report.

        container:   An NGAMS container (ngamsContainer)

        Returns:     Reference to object itself
        """
        self.__containers.append(container)
        return self

    def getContainerList(self):
        """
        Get tuple of Container objects

        Returns: Tuple with Container objects ([ngamsContainer, ...])
        """
        return self.__containers

    def load(self,
             filename,
             ignoreVarDiskPars = 0,
             getStatus = 0):
        """
        Load a status report stored in a file

        filename:           Filename for status report (string).

        ignoreVarDiskPars:  Ignore the variable part of the disk status:
                            Host ID, Slot ID, Mounted, Mount
                            Point (integer/0|1).

        getStatus:          Extract also the status information from the
                            XML document (0|1/integer).

        Returns:            Reference to object itself.
        """
        try:
            fd = open(filename)
            doc = fd.read()
            fd.close()
            self.unpackXmlDoc(doc, getStatus, ignoreVarDiskPars)
            return self
        except Exception as e:
            errMsg = "Error loading status XML document: " + filename +\
                     ". Error: " + str(e)
            raise Exception(errMsg)


    def unpackXmlDoc(self,
                     doc,
                     getStatus = 0,
                     ignoreVarDiskPars = 0):
        """
        Unpack a status report stored in an XML document and set the
        members of the class accordingly.

        doc:                Status report as XML document (string).

        getStatus:          Extract also the status information from the
                            XML document (0|1/integer).

        ignoreVarDiskPars:  Ignore the variable part of the disk status:
                            Host ID, Slot ID, Mounted, Mount
                            Point (integer/0|1).

        Returns:            Reference to object itself.
        """
        dom = xml.dom.minidom.parseString(doc)
        ngamsStatusEl = ngamsGetChildNodes(dom, NGAMS_XML_STATUS_ROOT_EL)[0]

        # Get the information from the Status Element.
        nodeList = dom.getElementsByTagName("Status")
        self.setDate(getAttribValue(nodeList[0], "Date"))
        self.setVersion(getAttribValue(nodeList[0], "Version"))
        self.setHostId(getAttribValue(nodeList[0], "HostId"))
        if (getStatus):
            self.setStatus(getAttribValue(nodeList[0], "Status"))
        self.setMessage(getAttribValue(nodeList[0], "Message"))
        if (getStatus):
            self.setState(getAttribValue(nodeList[0], "State"))
            self.setSubState(getAttribValue(nodeList[0], "SubState"))

        # Get the optional request handling status.
        requestId = getAttribValue(nodeList[0], "RequestId", 1)
        if (requestId): self.setRequestId(requestId)
        requestTime = getAttribValue(nodeList[0], "RequestTime", 1)
        if (requestTime): self.setRequestTime(fromiso8601(requestTime))
        completionPercent = getAttribValue(nodeList[0], "CompletionPercent", 1)
        if (completionPercent): self.setCompletionPercent(completionPercent)
        expectedCount = getAttribValue(nodeList[0], "ExpectedCount", 1)
        if (expectedCount): self.setExpectedCount(expectedCount)
        actualCount = getAttribValue(nodeList[0], "ActualCount", 1)
        if (actualCount): self.setActualCount(actualCount)
        estTotalTime = getAttribValue(nodeList[0], "EstTotalTime", 1)
        if (estTotalTime): self.setEstTotalTime(float(estTotalTime))
        remainingTime = getAttribValue(nodeList[0], "RemainingTime", 1)
        if (remainingTime): self.setRemainingTime(float(remainingTime))
        lastRequestStatUpdate = getAttribValue(nodeList[0],
                                               "LastRequestStatUpdate", 1)
        if (lastRequestStatUpdate):
            self.setLastRequestStatUpdate(fromiso8601(lastRequestStatUpdate))
        completionTime = getAttribValue(nodeList[0], "CompletionTime", 1)
        if (completionTime): self.setCompletionTime(fromiso8601(completionTime))

        # Unpack the NG/AMS Configuration information.
        ngamsCfgRootNode = dom.getElementsByTagName("NgamsCfg")
        if (len(ngamsCfgRootNode) > 0):
            tolerant = 1
            self.__ngamsCfg.unpackFromRootNode(ngamsCfgRootNode[0], tolerant)

        # Unpack Disk Status Elements and File Status Elements.
        diskNodes = dom.getElementsByTagName("DiskStatus")
        for diskNode in diskNodes:
            diskInfo = ngamsDiskInfo.ngamsDiskInfo().\
                       unpackFromDomNode(diskNode, ignoreVarDiskPars)
            self.addDiskStatus(diskInfo)

        # Unpack File Lists.
        fileListNodes = dom.getElementsByTagName("FileList")
        for fileListNode in fileListNodes:
            fileListObj = ngamsFileList.ngamsFileList()
            self.addFileList(fileListObj)
            fileListObj.unpackFromDomNode(fileListNode)

        # Unpack Containers. Loop over the root containers,
        # then let them deal with their own recursion
        rootContainerEls = [node for node in ngamsStatusEl.childNodes \
                            if node.nodeType == xml.dom.minidom.Node.ELEMENT_NODE\
                            and node.tagName == 'Container']
        for rootContainerEl in rootContainerEls:
            container = ngamsContainer.ngamsContainer()
            container.unpackFromDomNode(rootContainerEl)
            self.addContainer(container)

        dom.unlink()

        return self


    def saveAsXml(self,
                  filename,
                  genCfgStatus = 0,
                  genDiskStatus = 0,
                  genFileStatus = 0,
                  genStatesStatus = 1):
        """
        Save the information in the status report into an XML document.

        filename:  Target filename (string).

        Returns:   Void.
        """
        ngamsStatusDoc = self.genXml(genCfgStatus, genDiskStatus,
                                     genFileStatus, genStatesStatus)
        fd = open(filename, "w")
        ngamsStatusDoc.writexml(fd, '  ', '  ', '\n')
        fd.close()


    def genXmlDoc(self,
                  genCfgStatus = 0,
                  genDiskStatus = 0,
                  genFileStatus = 0,
                  genStatesStatus = 1,
                  genLimDiskStatus = 0):
        """
        Generates an XML document as string.

        For an explanantion of the input parameters read man-page for the
        function genXml().

        Returns:    XML document (string).
        """
        return self.genXml(0, 1, 1, 1, 0).toprettyxml('  ', '\n')[:-1]


    def genXml(self,
               genCfgStatus = 0,
               genDiskStatus = 0,
               genFileStatus = 0,
               genStatesStatus = 1,
               genLimDiskStatus = 0):
        """
        Generate an XML Node which contains the status report (root node).

        genCfgStatus:      1 = generate configuration status (integer).

        genDiskStatus:     1 = generate disk status (integer).

        genFileStatus:     1 = generate file status (integer).

        genStatesStatus:   1 = generate State/Sub-State (integer).

        genLimDiskStatus:  1 = generate only a limited (generic) set
                           of disk status (integer).

        Returns:           XML document (xml.dom.minidom.Document).
        """
        T = TRACE(5)

        doc = xml.dom.minidom.Document()
        ngamsStatusEl = doc.createElement(NGAMS_XML_STATUS_ROOT_EL)
        doc.appendChild(ngamsStatusEl)

        # Status Element.
        statusEl = xml.dom.minidom.Document().createElement("Status")
        statusEl.setAttribute("Date", self.getDate())
        statusEl.setAttribute("Version", self.getVersion())
        statusEl.setAttribute("HostId", self.getHostId())
        statusEl.setAttribute("Message", self.getMessage())
        if (genStatesStatus):
            statusEl.setAttribute("Status", self.getStatus())
            statusEl.setAttribute("State", self.getState())
            statusEl.setAttribute("SubState", self.getSubState())

        # Add the request handling status (if defined).
        if (self.getRequestId()):
            statusEl.setAttribute("RequestId", str(self.getRequestId()))
        if self.__requestTime is not None:
            statusEl.setAttribute("RequestTime", toiso8601(self.__requestTime))
        if (self.getCompletionPercent()):
            statusEl.setAttribute("CompletionPercent",
                                  "%.2f" % self.getCompletionPercent())
        if (self.getExpectedCount() != None):
            statusEl.setAttribute("ExpectedCount",str(self.getExpectedCount()))
        if (self.getActualCount() != None):
            statusEl.setAttribute("ActualCount", str(self.getActualCount()))
        if self.__estTotalTime is not None:
            statusEl.setAttribute("EstTotalTime", str(self.__estTotalTime))
        if self.__remainingTime is not None:
            statusEl.setAttribute("RemainingTime", str(self.__remainingTime))
        if self.__lastRequestStatUpdate is not None:
            statusEl.setAttribute("LastRequestStatUpdate", toiso8601(self.__lastRequestStatUpdate))
        if self.__completionTime is not None:
            statusEl.setAttribute("CompletionTime", toiso8601(self.__completionTime))

        ngamsStatusEl.appendChild(statusEl)

        # NgamsCfg Element.
        if (genCfgStatus):
            ngamsCfgEl = self.__ngamsCfg.genXml()
            ngamsStatusEl.appendChild(ngamsCfgEl)

        # DiskStatus Elements.
        if (genDiskStatus):
            for disk in self.getDiskStatusList():
                diskStatusEl = disk.genXml(genLimDiskStatus, genFileStatus)
                ngamsStatusEl.appendChild(diskStatusEl)

        # FileList Elements.
        if (len(self.getFileListList())):
            for fileListObj in self.getFileListList():
                fileListXmlNode = fileListObj.genXml()
                ngamsStatusEl.appendChild(fileListXmlNode)

        # Container Elements
        for container in self.getContainerList():
            ngamsStatusEl.appendChild(container.genXml())

        return doc


    def dumpBuf(self,
                dumpCfg = 0,
                dumpStates = 1,
                ignoreUndefFields = 0):
        """
        Dump information in object into a buffer in a (simple) ASCII
        format.

        dumpCfg:               If set to 1 the configuration is dumped
                               (integer/0|1).

        ignoreUndefFields:     Don't take fields, which have a length of 0
                               (integer/0|1).

        Returns:               Buffer with status information (string).
        """
        T = TRACE()

        format = prFormat1()
        buf = "Status:\n"
        buf += format % ("Date:", self.getDate())
        buf += format % ("Version:", self.getVersion())
        buf += format % ("HostId:", self.getHostId())
        if (dumpStates): buf += format % ("Status:", self.getStatus())
        buf += format % ("Message:", self.getMessage())
        if (dumpStates): buf += format % ("State:", self.getState())
        if (dumpStates): buf += format % ("SubState:", self.getSubState())
        if (self.getActualCount()):
            buf += format % ("ActualCount:", str(self.getActualCount()))
        if (self.getExpectedCount()):
            buf += format % ("ExpectedCount:", str(self.getExpectedCount()))

        # Dump NG/AMS Configuration.
        # TODO: Implement!

        # Dump Disk Status.
        for diskStatus in self.getDiskStatusList():
            buf += diskStatus.dumpBuf(1)

        # Dump File Lists.
        for fileList in self.getFileListList():
            buf += fileList.dumpBuf(ignoreUndefFields)

        return buf


# EOF
