#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccLogDef.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  11/10/2001  Created
#

"""
Contains the class PccLogDefEl, which is used to handle a Log Definition
Document.
"""

import sys, exceptions, string, re, types, xml.dom.minidom
import PccUtString

# Log types (from UNIX syslog)
PCC_LOG_EMERGENCY = "EMERGENCY"
PCC_LOG_ALERT     = "ALERT"
PCC_LOG_CRITICAL  = "CRITICAL"
PCC_LOG_ERROR     = "ERROR"
PCC_LOG_WARNING   = "WARNING"
PCC_LOG_NOTICE    = "NOTICE"
PCC_LOG_INFO      = "INFO"
PCC_LOG_DEBUG     = "DEBUG"


class PccLogDefEl:
    """
    Object to contain information about one Log Definition entry.
    """

    def __init__(self):
        """
        Constructor method.
        """
        self.__LogId       = ""
        self.__logNumber   = 0
        self.__logText     = ""
        self.__logType     = ""
        self.__description = ""

    def setLogId(self,
                 id):
        """
        Set Log ID.

        id:       Log ID (mnemonic) (string).

        Returns:  Reference to object itself (PccLogDefEl).
        """
        self.__LogId = id.strip()
        return self

    def getLogId(self):
        """
        Return Log Id.

        Returns:  Log Id (string).
        """
        return self.__LogId

    def setLogNumber(self,
                     number):
        """
        Set Log Number.

        number:   Log number (integer).

        Returns:  Reference to object itself (PccLogDefEl).
        """
        self.__logNumber = int(number)
        return self

    def getLogNumber(self):
        """
        Return Log Number.

        Returns:  Log Number (integer).
        """
        return self.__logNumber

    def setLogText(self,
                   text):
        """
        Set Log Text.

        text:     Log text (string).

        Returns:  Reference to object itself (PccLogDefEl).
        """
        self.__logText = text.strip()
        return self

    def getLogText(self):
        """
        Return Log Text.

        Returns:  Log Text (string).
        """
        return self.__logText

    def setLogType(self,
                   type):
        """
        Set Log Type. Valid values are:

            EMERGENCY, ALERT, CRITICAL, ERROR, WARNING, NOTICE, INFO, DEBUG.

        type:     Log Type (string).

        Returns:  Reference to object itself (PccLogDefEl).
        """
        self.__logType = type
        return self

    def getLogType(self):
        """
        Return Log Type.

        Returns:   Log Type (string).
        """
        return self.__logType

    def setDescription(self,
                       description):
        """
        Set Description.

        description:  Description of log entry (string).

        Returns:      Reference to object itself (PccLogDefEl).
        """
        self.__description = description
        return self

    def getDescription(self):
        """
        Return Description.

        Returns:   Description (string).
        """
        return self.__description

    def dumpBuf(self):
        """
        Dump contents of object in ASCII format in a string buffer.

        Returns:  String buffer with contents of object (string).
        """
        buf = ""
        buf = buf + "Log ID:          " + self.getLogId() + "\n"
        buf = buf + "Log Number:      " + str(self.getLogNumber()) + "\n"
        buf = buf + "Log Text:        " + self.getLogText() + "\n"
        buf = buf + "Log Type:        " + self.getLogType() + "\n"
        buf = buf + "Log Description: \n" + self.getDescription()
        return buf

        
    def dump(self):
        """
        Dump contents of object in ASCII format to stdout.

        Returns:   Reference to object itself (PccLogDefEl).
        """
        print self.dumpBuf()


def getAttribValue(node,
                   attributeName):
    """
    Return the value of an attribute for the node referenced. If the
    attribute is not available, "" is returned.

    node:           DOM Node object containing the information for the
                    element (Node).
    
    attributeName:  Name of attribute (string).

    Returns:        Value of attribute or "" if not found (string).
    """
    try:
        val = node._attrs[attributeName].nodeValue
        return str(val)
    except exceptions.Exception, e:    
        return ""


class PccLogDef:
    """
    Object to hold the information from an XML Log Definition File.
    It is possible to load, Log Definition Files, and to generate
    the logs.    
    """
   
    def __init__(self):
        """
        Constructor method.
        """
        self.clear()
        

    def clear(self):
        """
        Clear the object.

        Returns:      Reference to object itself (PccLogDef).
        """
        self.__logDefFile = ""
        self.__logDefs    = []
        self.__logIdDic   = {}
        self.__logNoDic   = {}
        return self


    def setLogDefFilename(self,
                          filename):
        """
        Set name of Log Definition File.

        filename:   Name of Log Definition File (string).

        Returns:    Reference to object itself (PccLogDef).
        """
        self.__logDefFile = filename
        return self


    def getLogDefFilename(self):
        """
        Return the name of the Log Definition File.

        Returns:   Name of Log Definition File (string).
        """
        return self.__logDefFile


    def load(self,
             filename):
        """
        Load the given Log Definition XML file into the object.

        filename:     Name of Log Definition File to load (string).
        
        Returns:      Reference to object itself (PccLogDef).
        """
        self.clear()
        fd = open(self.setLogDefFilename(filename).getLogDefFilename())
        doc = fd.read()
        fd.close()

        # The Expat parser does not like XSL declarations -- we
        # replace these. This can be removed if a parser is user later,
        # which conforms with the XML standards (we comment it out).
        doc = re.sub('<\?xml:stylesheet', '<!-- ?xml:stylesheet', doc)
        doc = re.sub('.xsl"\?>', '.xsl"? -->', doc)
        
        self.unpackXmlDoc(doc)
        self.check()
        return self


    def unpackXmlDoc(self,
                     doc):
        """
        Unpack the Log Definition XML Document and set the members
        of the class accordingly. The XML document must be loaded
        into a string buffer.

        doc:          Log File Defintion XML Document (string).

        Returns:      Reference to object itself (PccLogDef).
        """
        try:
            dom = xml.dom.minidom.parseString(doc)
        except exceptions.Exception, e:
            ex = str(e)
            exSplit = string.split(ex, ":")
            if (len(exSplit) > 1):
                lineNo = exSplit[1]
            else:
                lineNo = -1
            errMsg = "Error parsing Log Definition Document: " +\
                     self.getLogDefFilename() + ". " +\
                     "Probably around line number: " + str(lineNo) + ". " +\
                     "Exception: " + str(e)
            raise exceptions.Exception, errMsg  
        nodeList = dom.getElementsByTagName("LogDef")
        if (len(nodeList) == 0):
            errMsg = "Log Definition XML Document, does not have the " +\
                     "proper root element: LogDef!"
            raise exceptions.Exception, errMsg 

        # Unpack the document.

        # - IMPL.: We skip the XML header for the moment
        
        # - Get the Log Definition Elements
        logDefList = nodeList[0].getElementsByTagName("LogDefEl")
        if (len(logDefList) > 0):
            for node in logDefList:
                logDefEl = PccLogDefEl()
                logDefEl.setLogId(getAttribValue(node, "LogId"))
                logDefEl.setLogNumber(getAttribValue(node, "LogNumber"))

                # Get the Log Text
                tmpNodeList = node.getElementsByTagName("LogText")
                text = ""
                for nd in tmpNodeList[0].childNodes:
                    if (nd.nodeType == node.TEXT_NODE):
                        text = text + " " +\
                               PccUtString.trimString(nd.data, " \n")
                #logDefEl.setLogText(PccUtString.trimString(text, " \n"))
                # Remove newline characters and ensure that there is no
                # sequence of blanks.
                text = text.replace("\n", "")
                text = re.sub("\s *", " ", text)
                logDefEl.setLogText(text)
                 
                logDefEl.setLogType(getAttribValue(node, "LogType"))

                # Get the Log Description
                tmpNodeList = node.getElementsByTagName("Description")
                text = ""
                for nd in tmpNodeList[0].childNodes:
                    if (nd.nodeType == node.TEXT_NODE):
                        text = text +\
                               PccUtString.trimString(nd.data, " ")
                logDefEl.setDescription(PccUtString.trimString(text, " \n"))
                 
                logDefEl.setLogType(getAttribValue(node, "LogType"))
                
                self.addLogDefEl(logDefEl)
        
        return self


    def addLogDefEl(self,
                    logDefEl):
        """
        Add a Log Definition Element in the object.

        logDefEl:   Log Definition Object (PccLogDefEl).
        
        Returns:    Reference to object itself (PccLogDef).
        """
        self.__logDefs.append(logDefEl)
        self.__logIdDic[logDefEl.getLogId()] = logDefEl
        self.__logNoDic[logDefEl.getLogNumber()] = logDefEl
        return self
    

    def check(self):
        """
        Check the contents of the object.

        Returns:    Reference to object itself (PccLogDef).
        """
        return self


    def getLogDef(self,
                  logId):
        """
        Return a Log Definition Element referred to by its ID.

        logId:    Log ID (string).

        Returns:  Log Definition Element (PccLogDefEl)
        """
        try:
            return self.__logIdDic[logId]
        except exceptions.Exception, e:
            raise exceptions.Exception, "No Log Definition Element found "+\
                  "for Log ID given: " + logId


    def genLog(self,
               logId,
               parList = None):
        """
        Generate a log line by filling in the parameters giving in a list
        (if any). The resulting log line is returned.

        logId:     ID of the log (string).

        parList:   List with the parameters to fill into the log
                   format (list).

        Returns:   Resulting log line (string).
        """
        logEl = self.getLogDef(logId)
        if (parList): 
            parStr = ""
            for par in parList:
                if (isinstance(par, types.StringType)):
                    par = re.sub("'", "", par)
                    parStr = parStr + "'" + par + "',"
                else:
                    parStr = parStr + str(par) + ","
            parStr = parStr[0:-1]
            log = str(logEl.getLogText() % eval(parStr))
        else:
            log = str(logEl.getLogText())
        return log


    def genLogX(self,
                logId,
                parList = None):
        """
        Generate a log line by filling in the parameters giving in a list
        (if any). As a prefix to the log line, the following information
        is added. The format of the generated log line is as follows:

          <Log ID>:<Log Number>:<Log Type>: <Log Messge>

        logId:     ID of the log (string).

        parList:   List with the parameters to fill into the log
                   format (list).

        Returns:   Resulting log line (string).
        """
        logEl = self.getLogDef(logId)
        return logEl.getLogId() + ":" + str(logEl.getLogNumber()) + ":" +\
               logEl.getLogType() + ": " + self.genLog(logId, parList)


    def dumpBuf(self):
        """
        Dump contents of object in ASCII format in a string buffer.

        Returns:  String buffer with contents of object (string).
        """
        buf = "Contents of Log Definition File: " + self.getLogDefFilename() +\
              "\n\n"
        for el in self.__logDefs:
            buf = buf + el.dumpBuf() + "\n\n"
        return buf


    def dump(self):
        """
        Dump contents of buffer on stdout.

        Returns:   Reference to object itself (PccLogDef).   
        """
        print self.dumpBuf()
        return self


if __name__ == '__main__':
    """
    Main program that loads a Log Definition XML Document and prints out
    all the log entries in a readible format.
    """
    logDef = PccLogDef()
    if (len(sys.argv) > 1):
        logDef.load(sys.argv[1])    

    # Add an element
    logEl = PccLogDefEl().\
            setLogId("ERR_OPEN_FILE").setLogNumber(123456789).\
            setLogType(PCC_LOG_ERROR).\
            setLogText("Could not open file: %s due to: %s.").\
            setDescription("A problem occurred opening a file. Check if " +\
                           "the file exists, and if permissions are " +\
                           "correctly defined.")
    logDef.addLogDefEl(logEl)

    logDef.dump()

    print logDef.genLog("ERR_OPEN_FILE", ["MyFile", "Serious Problems"])


#
# ___oOo___
