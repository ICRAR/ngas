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
# "@(#) $Id: ngamsXmlMgr.py,v 1.7 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  26/05/2003  Created
#

"""
Contains the ngamsXmlMgr class, which is used to handle XML documents.

Internally the contents of the document is stored in a recursive structure
of objects of the type ngamsElement. Attributes are represented by the
class ngamsAttribute.

Also an internal dictionary is created with references to the ngamsElement
and ngamsAttribute objects for direct and fast access.

It is possible to generate a dictionary representation of the XML document.
This representation is as follows:

<Authorization Enable='0' Id='Authorization-Std'>
  <User Name='ngas' Password='bamdhcw=='/>
  <User Name='ngasmgr' Password='bbmdhc21ncg=='/>
  <User Name='ngas-int' Password='bcmdhcy1pbnQ='/>
</Authorization>

(double quotes replaced by single quotes in the example).

- maps into the following in the dictionary representation (where the
format is: <Key> = <Value>):

NgamsCfg.Authorization[1] = None
NgamsCfg.Authorization[1].User[1] = None
NgamsCfg.Authorization[1].User[1].Name = ngas
NgamsCfg.Authorization[1].User[1].Password = bamdhcw==
NgamsCfg.Authorization[1].User[2] = None
NgamsCfg.Authorization[1].User[2].Name = ngasmgr
NgamsCfg.Authorization[1].User[2].Password = bbmdhc21ncg==
NgamsCfg.Authorization[1].User[3] = None
NgamsCfg.Authorization[1].User[3].Name = ngas-int
NgamsCfg.Authorization[1].User[3].Password = bcmdhcy1pbnQ=
NgamsCfg.Authorization[1].Enable = 0
NgamsCfg.Authorization[1].Id = Authorization-Std

Using this scheme, a 1:1 mapping between the two representations is obtained.
"""

import sys
import xml.dom.minidom


from ngams import *


class ngamsAttribute:
    """
    Class to hold information about a single attribute.
    """
    
    def __init__(self,
                 name,
                 value = None,
                 comment = None,
                 context = None):
        """
        Initialize the object and set the member variables.

        name:     Name of attribute (string).

        value:    Value of attribute (boolean|integer|float|string|str-list).
        
        comment:  Optional comment (string).

        context:  Context of element (string).
        """
        self.__name     = None
        self.__value    = None

        # Handle input parameters.
        self.setNameValue(name, value)
        self.__comment  = comment
        self.__context    = context


    def setNameValue(self,
                     name,
                     value):
        """
        Set the name and value of the object.

        name:     Name of attribute (string).

        value:    Value of attribute (boolean|integer|float|string|str-list).

        Returns:  Reference to object itself.
        """
        self.__name = name
        self.__value = value
        return self


    def getName(self):
        """
        Return the name of the attribute.

        Returns:    Name (string).
        """
        return self.__name
        

    def getValue(self):
        """
        Get the value of the attribute.

        Return:  Value (string|integer|float).
        """
        return self.__value


    def setValue(self,
                 value):
        """
        Set the value of the object.

        value:    Value (string).

        Returns:  Reference to object itself.
        """
        self.__value = value
        return self


    def getComment(self):
        """
        Get comment related to the attribute.

        Returns:   Comment (string|None).
        """
        return self.__comment


    def setContext(self,
                   context):
        """
        Set the context of the object.

        context:   Context of element (string).

        Returns:   Reference to object itself.
        """
        self.__context = context
        return self


    def getContext(self):
        """
        Return the context of the object.

        Returns:   Context (string|None).
        """
        return self.__context
   

class ngamsElement(ngamsAttribute):
    """
    Class to hold information for one element.
    """

    def __init__(self,
                 name,
                 value = None,
                 comment = None,
                 context = None):   
        """
        Constructor method initializing the class.
        """
        ngamsAttribute.__init__(self, name, value, comment, context)
        self.__subElList = []
        self.__attrList  = []


    def addSubEl(self,
                 elObj):
        """
        Add a sub-element in the object.

        elObj:     Sub-element object (ngamsElement).

        Returns:   Reference to object itself.
        """
        self.__subElList.append(elObj)
        return self


    def getSubElList(self):
        """
        Get reference to list containing the sub-elements of this element.

        Returns:   List with sub-element objects (list/ngamsElement).
        """
        return self.__subElList


    def addAttr(self,
                attrObj):
        """
        Add an attribute object in the element.

        attrObj:     Reference to the attribute object (ngamsAttribute).

        Returns:     Reference to object itself.
        """
        self.__attrList.append(attrObj)
        return self


    def getAttrList(self):
        """
        Obtain reference to the element attribute object list.

        Returns:    List with attribute objects (list/ngamsAttribute).
        """
        return self.__attrList


    def getAttrVal(self,
                   attrName):
        """
        Get the value of an attribute of the element.

        attrName:   Name of attribute (string).

        Returns:    Reference to object itself.
        """
        for attrObj in self.getAttrList():
            if (attrObj.getName() == attrName): return attrObj.getValue()
        return None
        

class ngamsXmlMgr:
    """
    Generic class to handle XML documents.
    """
    
    def __init__(self,
                 rootEl,
                 xmlDoc = None):
        """
        Constructor method.
        """
        self.setXmlDoc(xmlDoc)

        # Dictionary with Simplified Xpath references (as keys) to the
        # ngamsElement and ngamsAttribute objects.
        self.__xmlDic    = {}

        # Root el. of XML document + dict.s to refer to elements/attributes.
        self.__rootElObj = ngamsElement(rootEl)
        self.__xmlDic[rootEl] = self.__rootElObj
        
        # Load XML document if specified.
        if (xmlDoc): self.load(xmlDoc)


    def clear(self):
        """
        Clear the instance.

        Returns:   Reference to object itself.
        """
        self.__xmlDic = {}
        return self
        

    def getRootElObj(self):
        """
        Get reference to root element object.

        Returns:  Reference to root element object (ngamsElement).
        """
        return self.__rootElObj
    

    def setXmlDoc(self,
                  xmlDoc):
        """
        Set the name of an XML document associated (loaded into) the object.

        xmlDoc:    Name of file hosting the XML document (string).

        Returns:   Reference to object itself.
        """
        self.__xmlDoc = xmlDoc
        return self


    def getXmlDoc(self):
        """
        Return the name of the associated (loaded into) the object.

        Returns:    Name of XMl document loaded into the object (string).
        """
        return self.__xmlDoc

        
    def load(self,
             xmlDoc):
        """
        Load an NG/AMS Configuration from an XML document.

        xmlDoc:   Name of XML document (string).

        Returns:  Reference to object itself.
        """
        T = TRACE()

        try:
            fd = open(self.setXmlDoc(xmlDoc).getXmlDoc())
            doc = fd.read()
            fd.close()
        except Exception, e:
            errMsg = genLog("NGAMS_ER_LOAD_CFG", [xmlDoc, str(e)])
            raise Exception, errMsg  
        
        # The Expat parser does not like XSL declarations. Can be removed if
        # a parser is used which conforms with the XML standards.
        doc = re.sub('<\?xml:stylesheet', '<!-- ?xml:stylesheet', doc)
        doc = re.sub('.xsl"\?>', '.xsl"? -->', doc)
        
        self.unpackXmlDoc(doc)        
        return self


    def save(self,
             docType,
             schema,
             xmlDoc = None,
             critInfoNameList = []):
        """
        Save the NG/AMS Configuration into the connected XML document
        or the one given in the input parameter.

        xmlDoc:          Name of XML document (string).

        Returns:  Reference to object itself.
        """
        T = TRACE()

        if (xmlDoc):
            targetFile = xmlDoc
            rmFile(targetFile)
        else:
            targetFile = self.getXmlDoc()
        fo = open(targetFile, "w")
        fo.write(self.genXmlDoc(docType, schema, critInfoNameList))
        fo.close()
        return self


    def _genXml(self,
                elObj,
                critInfoNameList = []):
        """
        Generate an XML document from the contents of the given NG/AMS XML
        Element object.

        elObj:              XML element object (ngamsElement).

        critInfoNameList:   List of names of attributes and elements, which
                            value should be hidden (list).

        Returns:            XML DOM node (Node).
        """
        T = TRACE()

        elDomObj = xml.dom.minidom.Document().createElement(elObj.getName())

        # Go through attributes and add them in the DOM.
        for attrObj in elObj.getAttrList():
            hideValue = 0
            if (critInfoNameList):
                try:
                    critInfoNameList.index(attrObj.getName())
                    hideValue = 1
                except:
                    pass
            if (hideValue):
                val = "********"
            else:
                val = attrObj.getValue()
            elDomObj.setAttribute(attrObj.getName(), str(val))

        # Go through sub-elements and add them in the DOM.
        for subElObj in elObj.getSubElList():
            subElDomObj = self._genXml(subElObj, critInfoNameList)
            elDomObj.appendChild(subElDomObj)
  
        return elDomObj


    def genXml(self,
               critInfoNameList = []):
        """
        Generate an XML DOM Node object from the contents of the object.

        Returns:    XML DOM Node (Node).
        """
        T = TRACE()

        xmlDomObj = self._genXml(self.__rootElObj, critInfoNameList)
        return xmlDomObj


    def genXmlDoc(self,
                  docType = None,
                  schema = None,
                  critInfoNameList = []):
        """
        Generate an XML document in a string buffer and return reference to
        this.

        docType:            Doctype of the document (string).
        
        schema:             Schema used to verify the document (string).

        critInfoNameList:   List of names, which should be hidden in the
                            generated document (list).

        Returns:            Generate XMl document (string).
        """
        xmlDoc = '<?xml version="1.0" encoding="UTF-8"?>\n'
        if (docType and schema):
            xmlDoc += '<!DOCTYPE %s SYSTEM "%s">\n' % (docType, schema)
        xmlDoc += '\n'
        xmlDoc += self.genXml(critInfoNameList).toprettyxml("  ", "\n")[0:-1]
        # Due to a misconception in Oracle (""=NULL), we replace None in the document
        # with " ".
        xmlDoc = xmlDoc.replace('"None"', '" "')
        return xmlDoc
        
    
    def unpackXmlDoc(self,
                     xmlDoc):
        """
        Unpack the configuration file represented as the XML ASCII text.
        
        xmlDoc:      XML ASCII document (string).

        Returns:     Reference to object itself.
        """
        domObj = None
        try:
            domObj = xml.dom.minidom.parseString(xmlDoc)
        except Exception, e:
            if (domObj != None): domObj.unlink()
            ex = str(e)
            lineNo = str(e).split(":")[1]
            errMsg = "Error parsing NG/AMS XML Configuration. " +\
                     "Probably around line number: " + str(lineNo) + ". " +\
                     "Exception: " + str(e)
            errMsg = genLog("NGAMS_ER_CONF_FILE", [errMsg])
            raise Exception, errMsg

        # Check that the root element is present.
        rootElName = self.__rootElObj.getName()
        nodeList = domObj.getElementsByTagName(rootElName)
        if (len(nodeList) == 0):
            msg = "XML document, does not have the " +\
                  "proper root element: %s! Aborting."
            errMsg = genLog("NGAMS_ER_CONF_FILE", [msg % rootElName])
            raise Exception, errMsg
        try:
            self.clear()
            curId = None
            self._unpack(nodeList[0], "", None, curId)
            domObj.unlink()
        except Exception, e:
            if (domObj): domObj.unlink()
            raise e
        return self
        

    def _unpack(self,
                nodeObj,
                elDicKey,
                parElObj,
                curId):
        
        """
        Unpack configuration XML document starting from the node of the DOM.

        nodeObj:      DOM Node Object containing the reference to the root
                      element of the configuration file XML document (Node).

        Returns:      Reference to object itself.
        """
        if (elDicKey == ""):
            # It's the root element.
            refEl = self.__rootElObj
        else:
            # Create a new element.
            elName = str(nodeObj).split(" ")[2]
            refEl = ngamsElement(elName, value = None, comment = None,
                                 context = curId)
            parElObj.addSubEl(refEl)
        if (nodeObj._attrs.has_key("Id")):
            curId = nodeObj._attrs["Id"].nodeValue
            refEl.setContext(curId)
        elDicKey = self._addElXmlDic(elDicKey, refEl)

        # Get attributes of the element.
        for attrName in nodeObj._attrs.keys():
            val = nodeObj._attrs[attrName].nodeValue
            tmpAttrObj = ngamsAttribute(attrName, val, comment = None,
                                        context = curId)
            refEl.addAttr(tmpAttrObj)
            self._addAttrXmlDic(elDicKey, tmpAttrObj)
        
        # Check if the element has sub-elements.
        for childNode in nodeObj.childNodes:
            strRep = str(childNode)
            if (strRep.find("DOM Element") != -1):
                elName = strRep.split(" ")[2]
                self._unpack(childNode, elDicKey, refEl, curId)

        return self


    def getXmlDic(self):
        """
        Get reference to the XML Dictionary.

        Returns:   Reference to the XML Dictionary (dictionary).
        """
        return self.__xmlDic


    def _addElXmlDic(self,
                     elDicKey,
                     elObj):
        """
        Add an element in the XML Dictionary.

        elDicKey:       Current key in the XML Dictionary (string).
        
        elObj:          Reference to element object (ngamsElement).

        Returns:        Name of the key in the XML Dictionary (string).
        """
        if (elObj == self.__rootElObj):
            if (not self.__xmlDic.has_key(elDicKey)):
                self.__xmlDic[self.__rootElObj.getName()] = self.__rootElObj
            return self.__rootElObj.getName()
        elif (elDicKey):
            newElDicKeyFormat = elDicKey + "." + elObj.getName() + "[%d]"
            idx = 1
            while (1):
                newElDicKey = newElDicKeyFormat % idx
                if (not self.__xmlDic.has_key(newElDicKey)):
                    self.__xmlDic[newElDicKey] = elObj
                    break
                else:
                    idx += 1
            return newElDicKey
        else:
            return elObj.getName()


    def _addAttrXmlDic(self,
                       elDicKey,
                       attrObj):
        """
        Add an attribute element in the dictionary.

        elDicKey:       Current key in the XML Dictionary (string).
        
        attrObj:        Reference to the attribute object in question
                        (ngamsAttribute).
        """
        attrPath = elDicKey + "." + attrObj.getName()
        self.__xmlDic[attrPath] = attrObj
        return attrPath


    def addElOrAttr(self,
                    xmlDicKey,
                    obj):
        """
        Add an element or attribute object to the object.

        xmlDicKey:   XML Dictionary key for the object (string).

        obj:         Object to add (ngamsElement|ngamsAttribute).

        Returns:     Reference to object itself.
        """
        self.__xmlDic[xmlDicKey] = obj
        # Add this element in the parent element.
        if (xmlDicKey[-1] == "]"):
            # It's an element. Get parent path.
            pathEls = xmlDicKey.split(".")
            parElPath = ""
            for elPath in pathEls[:-1]:
                if (parElPath): parElPath += "."
                parElPath += elPath
            self.__xmlDic[parElPath].addSubEl(obj)
        else:
            # It's an attribute.
            parElPath = xmlDicKey[0:(xmlDicKey.rfind("."))]
            self.__xmlDic[parElPath].addAttr(obj)
        return self

        
    def storeVal(self,
                 xmlDicKey,
                 value,
                 context = None):
        """
        Store the given value defined by the XML Dictionary Key in the
        XML Manager Object.

        xmlDicKey:   XML dictionary name of the parameter (Simplified XPath
                     Syntax). Could e.g. be:

                         NgamsCfg.Server[1].ArchiveName              (string).
        
        value:       Value of the element/attribute (string).

        Returns:     Reference to object itself.
        """
        # TODO: Handles only attributes for now.
        pathEls = xmlDicKey.split(".")
        tmpPath = pathEls[0]
        for pathEl in pathEls[1:-1]:
            tmpPath += "." + pathEl
            # Check if this element is already in the otherwise add it.
            if (not self.__xmlDic.has_key(tmpPath)):
                self.addElOrAttr(tmpPath, ngamsElement(pathEl.split("[")[0],
                                                       context = context))
        self.addElOrAttr(tmpPath + "." + pathEls[-1],
                         ngamsAttribute(pathEls[-1], value, None, context))
        return self


    # TODO: Investigate a potential problem when clear=0, it seems that
    #       elements already in the internal dictionary are not contained
    #       in the final XML document.
    def digestXmlDic(self,
                     xmlDic,
                     clear = 0):
        """
        Go through the elements and attributes represented as an XML
        Dictionary, and build up the internal structure.

        xmlDic:    XML Dictionary (dictionary).

        clear:     Clear the object before processing the XML dictionary
                   (integer/0|1). 

        Returns:   Reference to object itself.
        """
        T = TRACE()
        
        if (clear): self.clear()
        xmlDicKeys = xmlDic.keys()
        xmlDicKeys.sort()
        # First element should be the root element.
        self.__rootElObj = xmlDic[xmlDicKeys[0]]
        self.__xmlDic[xmlDicKeys[0]] = xmlDic[xmlDicKeys[0]]
        for xmlDicKey in xmlDicKeys[1:]:
            info(5,"Handling configuration parameter with key: %s" % xmlDicKey)
            self.addElOrAttr(xmlDicKey, xmlDic[xmlDicKey])
        return self


    def dumpXmlDic(self,
                   sort = 1,
                   rmBlanks = 1):
        """
        Dump the contents of the XML Dictionary in a buffer in the format:

          <Key>: <Value>
          <Key>: <Value>
          ...

        sort:       If 1 the list will be sorted according to the key
                    names (integer/0|1).

        rmBlanks:   Remove blanks in the values (integer/0|1).

        Returns:    Reference to string buffer with the XML Dictionary dump
                    (string).
        """
        buf = ""
        keys = self.__xmlDic.keys()
        if (sort): keys.sort()
        for key in keys:
            if (self.__xmlDic[key].getValue() != None):
                if (rmBlanks):
                    buf += "%-55s: %s\n" % (key, self.__xmlDic[key].\
                                            getValue().replace(" ", ""))
                else:
                    buf += "%-55s: %s\n" % (key, self.__xmlDic[key].getValue())
        return buf

        
    def genXmlDic(self,
                  rmBlanks = 1):
        """
        Generate a Python dictionary with the values of the XML Dictionary
        document.

        rmBlanks:   Remove blanks in the values (integer/0|1).

        Returns:    Reference to string buffer with the XML Dictionary dump
                    (string).
        """
        dic = {}
        keys = self.__xmlDic.keys()
        for key in keys:
            if (self.__xmlDic[key].getValue() != None):
                if (rmBlanks):
                    dic[key] = self.__xmlDic[key].getValue().replace(" ", "")
                else:
                    dic[key] = self.__xmlDic[key].getValue()
        return dic

    
if __name__ == '__main__':
    """
    Main program.
    """
    srcXmlDoc = "/home/ngasmgr/NEW_ngamsConfig/ngamsServer_NewFormat.xml"
    trgXmlDoc = "/home/ngasmgr/NEW_ngamsConfig/ngamsServer_NewFormat-NEW.xml"
    cfg = ngamsXmlMgr("NgamsCfg", srcXmlDoc)
    cfg.save("NgamsCfg", "ngamsCfg.dtd", trgXmlDoc)
    xmlDic = cfg.getXmlDic()
    keyList = xmlDic.keys()
    keyList.sort()
    for key in keyList:
        print "%s: %s" % (key, xmlDic[key])
    

# EOF
