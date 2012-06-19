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
# "@(#) $Id: ngamsConfigBase.py,v 1.6 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  26/05/2003  Created
#

"""
Contains the ngamsConfigBase class to handle the NG/AMS Configuration.
"""

import sys

from ngams import *
import ngamsXmlMgr


class ngamsConfigBase:
    """
    Class to handle the NG/AMS Configuration
    """
    
    def __init__(self,
                 xmlDoc = None,
                 dbObj = None):
        """
        Constructor method.
        """
        self.setXmlDoc(xmlDoc)
        self.__dbObj = dbObj
        self.__xmlMgr = ngamsXmlMgr.ngamsXmlMgr("NgamsCfg")
        if (xmlDoc): self.__xmlMgr.load(xmlDoc)


    def setXmlDoc(self,
                  xmlDoc):
        """
        Set the name of the XML document.

        xmlDoc:   Name of XML document loaded into the object (string).

        Returns:  Reference to object itself.
        """
        self.__xmlDoc = xmlDoc
        return self


    def getXmlDoc(self):
        """
        Get the name of the XML document.

        Returns:   Name of XML document loaded into the object (string).
        """
        return self.__xmlDoc


    def setDbObj(self,
                 dbObj):
        """
        Set the DB connection object of this instance.

        dbObj:    DB connection object (ngamsDb).

        Returns:  Reference to object itself.
        """
        self.__dbObj = dbObj
        return self
    

    def getVal(self,
               xmlDicKey):
        """
        Return the value of a parameter. If the parameter is not defined
        None is returned.

        xmlDicKey:    XML dictionary name of the parameter (Simplified XPath
                      Syntax). Could e.g. be:

                         NgamsCfg.Server[1].ArchiveName              (string).

        Returns:      Value of element or attribute or None (<Value>|None).
        """
        if (xmlDicKey.find("NgamsCfg") != 0):
            xmlDicKey = "NgamsCfg." + xmlDicKey
        if (self.getXmlDic().has_key(xmlDicKey)):
            return str(self.getXmlDic()[xmlDicKey].getValue())
        else:
            return None

        
    def storeVal(self,
                 xmlDicKey,
                 value,
                 dbCfgGroupId = None):
        """
        Store the given value defined by the XML Dictionary Key in the
        configuration object.

        xmlDicKey:    XML dictionary name of the parameter (Simplified XPath
                      Syntax). Could e.g. be:

                         NgamsCfg.Server[1].ArchiveName              (string).
        
        value:        Value of the element/attribute (string).

        dbCfgGroupId: DB configuration group ID (string|None).

        Returns:      Reference to object itself.
        """
        self.__xmlMgr.storeVal(xmlDicKey, value, dbCfgGroupId)
        return self
        

    def load(self,
             xmlDoc):
        """
        Load an NG/AMS Configuration from an XML document.

        xmlDoc:   Name of XML document (string).

        Returns:  Reference to object itself.
        """
        T = TRACE()
        
        try:
            self.__xmlMgr.load(xmlDoc)
            self.setXmlDoc(xmlDoc)
        except Exception, e:
            errMsg = genLog("NGAMS_ER_LOAD_CFG", [xmlDoc, str(e)])
            raise Exception, errMsg  
        return self


    def save(self,
             xmlDoc = None,
             hideCritInfo = 0):
        """
        Save the NG/AMS Configuration into the connected XML document
        or the one given in the input parameter.

        xmlDoc:          Name of XML document (string).

        hideCritInfo:    If set to 1 passwords and other 'confidential'
                         information appearing in the log file, will
                          be hidden (integer/0|1).

        Returns:  Reference to object itself.
        """
        T = TRACE()
        
        if (hideCritInfo):
            self.__xmlMgr.save("NgamsCfg", "ngamsCfg.dtd", xmlDoc,["Password"])
        else:
            self.__xmlMgr.save("NgamsCfg", "ngamsCfg.dtd", xmlDoc)
        return self


    def genXml(self,
               hideCritInfo = 0):
        """
        Generate an XML DOM Node object from the contents of the object.

        Returns:    XML DOM Node (Node).
        """
        T = TRACE()
        
        if (hideCritInfo):
            critInfoNameList = ["Password"]
        else:
            critInfoNameList = []
        xmlDomObj = self.__xmlMgr._genXml("NgamsCfg", critInfoNameList)
        return xmlDomObj


    def genXmlDoc(self,
                  hideCritInfo = 0):
        """
        Generate an XML Document from the contents loaded in a string buffer
        and return this.

        hideCritInfo:   Hide critical information (integer/0|1).

        Returns:        XML document (string).        
        """
        T = TRACE()
        
        xmlDoc = self.__xmlMgr.genXmlDoc("NgamsCfg", "ngamsCfg.dtd",
                                         hideCritInfo)
        return xmlDoc


    def getXmlDic(self):
        """
        Return the XML Dictionary containing the contents of the XML
        document in a dictionary format. See man-page for ngamsXmlMgr for
        additional information.

        Returns:     Dictionary (dictionary).
        """
        return self.__xmlMgr.getXmlDic()


    def dumpXmlDic(self):
        """
        Dump the contents of the XML Dictionary in a buffer in the format:

          <Key> = <Value>
          <Key> = <Value>
          ...

        Returns:    Reference to string buffer with the XML Dictionary dump
                    (string).
        """
        return self.__xmlMgr.dumpXmlDic()


    def getXmlObj(self,
                  objPath):
        """
        Get an XML Element or Attribute Object from its XMl Dictionary Key
        (= path). See man-page for ngamsXmlMgr for additional information.

        objPath:   Path (= XML Dictionary Key) (string).

        Returns:   Object referred to or None
                   (ngamsElement|ngamsAttribute|None).
        """
        if (objPath.find("NgamsCfg") != 0): objPath = "NgamsCfg." + objPath
        if (self.getXmlDic().has_key(objPath)):
            return self.getXmlDic()[objPath]
        else:
            return None
 
        
    def loadFromDb(self,
                   name,
                   clear = 0):
        """
        Load a configuration from the DB via the given ID.

        name:       Name of the configuration in the DB (string).

        clear:      Clear the object before loading if set to 1 (integer/0|1).
        
        Returns:    Reference to object itself.
        """
        T = TRACE()
        
        if (not self.__dbObj):
            raise Exception, "No DB connection object associated to " +\
                  "ngamsConfigBase object. Cannot access DB!"
        cfgPars = self.__dbObj.getCfgPars(name)
        xmlDic = {}
        for cfgParInfo in cfgPars:
            groupId = cfgParInfo[0]
            key     = cfgParInfo[1]
            value   = cfgParInfo[2]
            if (value == "None"): value = None
            comment = cfgParInfo[3]
            if ((key[-1] != "]") and (len(key.split(".")) > 1)):
                attrName = key[key.rfind("."):]
                if (attrName[0] == "."): attrName = attrName[1:]
                tmpObj = ngamsXmlMgr.ngamsAttribute(attrName, value, comment,
                                                    groupId)
            else:
                elName = key.split(".")[-1].split("[")[0]
                tmpObj = ngamsXmlMgr.ngamsElement(elName, value, comment,
                                                  groupId)
            xmlDic[key] = tmpObj
        if (not xmlDic.has_key("NgamsCfg")):
            xmlDic["NgamsCfg"] = ngamsXmlMgr.ngamsElement("NgamsCfg", "")
        self.__xmlMgr.digestXmlDic(xmlDic, clear)
        return self


    def writeToDb(self):
        """
        Write the configuration to the NGAS DB.

        Returns:   Reference to object itself.
        """
        T = TRACE()
        
        if (not self.__dbObj):
            raise Exception, "No DB connection object associated to " +\
                  "ngamsConfigBase object. Cannot access DB!"
        xmlDic = self.__xmlMgr.getXmlDic()
        xmlDicKeys = xmlDic.keys()
        xmlDicKeys.sort()
        for xmlDicKey in xmlDicKeys:
            obj = xmlDic[xmlDicKey]
            self.__dbObj.writeCfgPar(obj.getContext(), xmlDicKey,
                                     obj.getValue(), obj.getComment())
        return self

    
if __name__ == '__main__':
    """
    Main program.
    """
    import ngamsDb
    dbCon = ngamsDb.ngamsDb("TESTSRV", "ngastst1", "ngas", "ngas_pw")
    trgXmlDoc = "/home/ngasmgr/NEW_ngamsConfig/ngamsConfigX-TEST.xml"
    if (0):
        cfg = ngamsConfigBase(sys.argv[1], dbCon)
        cfg.save(trgXmlDoc)
        cfg.writeToDb()
    else:
        cfg = ngamsConfigBase(None, dbCon).loadFromDb("GAR-AHU")
    xmlDic = cfg.getXmlDic()
    xmlDicKeys = xmlDic.keys()
    xmlDicKeys.sort()
    for key in xmlDicKeys:
        print "%s = %s" % (key, xmlDic[key].getValue())
        

# EOF
