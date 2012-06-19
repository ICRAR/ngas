#*****************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccDidRec.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  10/11/2000  Created
#

"""
Module providing a class for handling a record within a DID.
"""

from   PccLog import *
import PccUtString
import PccDidBase, PccDidException


class PccDidRec(PccDidBase.PccDidBase):
    """
    Class to handle information for one DID Record.
    """

    def __init__(self,
                 parameterName = "",
                 keyClass      = "",
                 context       = "",
                 type          = "",
                 valueFormat   = "",
                 unit          = "",
                 commentFormat = "",
                 description   = "",
                 range         = "",
                 defaultValue  = "",
                 mandatory     = ""):
        """
        Constructor method initializing the class variables of the object.
        """
        self.__parameterName = parameterName
        self.__class         = keyClass
        self.__context       = context
        self.__type          = type
        self.__valueFormat   = valueFormat
        self.__unit          = unit
        self.__commentFormat = commentFormat
        self.__description   = description
        self.__range         = range
        self.__defaultValue  = defaultValue
        self.__mandatory     = mandatory


    def getRecFields(self,
                     didBuf,
                     permissive = 0):
        """
        Extract the information from a list with the lines from the DID
        and set the members of the class.
        
        Expects that the pointer in the PCC List Object is pointing
        to the first line of the DID Record.

        didBuf:       Reference to list containing the lines from the DID.

        permissive:   Run the parsing in Permissive Mode, whereby
                          certain errors (or malformed DID records) are
                          accepted.

        Returns:      Reference to object itself.
        """
        
        # Expects that the pointer in the PCC List Object is pointing
        # to the first line of the parameter record.
        didBuf.getPrevEl()
        
        # Parse parameter records. Expect format:
        #
        # Parameter Name:
        # Class:
        # Context:
        # Type:
        # Value Format:
        # Unit:
        # Comment Format:
        # Description:
        # [Range:]
        # [Default Value:]
        # [Mandatory:]
        manFieldSeq = ["Parameter Name:", "Class:", "Context:", "Type:",
                       "Value Format:", "Unit:", "Comment Format:",
                       "Description:"]
        fields = []
        extrField = []

        # Parse mandatory fields.
        count = 0
        while (count < len(manFieldSeq)):
            if (didBuf.remainingEls() > 0):
                line = PccUtString.trimString(didBuf.getNextEl(), " \t\n")
                if (line != "") and (line[0] != "#"):
                    fields = self.splitField(line)
                    if (fields[0] != manFieldSeq[count]):
                        # If permissive mode is enabled, try to make sense out
                        # of the DID anyway.
                        if (permissive == 1):
                            # Typical errors:
                            # "Comment Field: instead of "Comment Format:"
                            if ((fields[0] == "Comment Field:") and
                                (manFieldSeq[count] == "Comment Format:")):
                                extrField.append(fields[1])
                                count = count + 1
                        else:
                            raise PccDidException.PccDidException, \
                                  "Encountered unexpected field: \"" + \
                                  fields[0] + "\" in line " + \
                                  str(didBuf.getIndex() + 1) + "."
                    else:
                        extrField.append(fields[1])
                        count = count + 1      
            else:
                if (permissive):
                    info(1,"Found Parameter Record with a missing field - "+\
                         manFieldSeq[count] + ".  Skipping this record.")
                    # Set the pointer to the next record.
                    foundParRec = 1
                    while ((didBuf.remainingEls() > 0) and foundParRec):
                        didBuf.getNextEl()
                        if (len(didBuf.getCurEl()) >= 15):
                            if (didBuf.getCurEl()[0:15] == "Parameter Name:"):
                                foundParRec = 0
                    if (foundParRec):
                        didBuf.getPrevEl()
                    return
                else:
                    raise PccDidException.PccDidException, \
                          "Missing field in Parameter Record: " +\
                          manFieldSeq[count] + "."

        # Get rest of description if distributed over several lines.
        descr = PccUtString.trimString(extrField[7], " \t\n")
        run = 1
        while ((run == 1) and (didBuf.remainingEls() > 0)):
            line = PccUtString.trimString(didBuf.getNextEl(), " \t\n")
            if (line != "") and (line[0] == "#"):
                run = 0
            else:
                fields = self.splitField(line)
                if ((fields[0] == "Range:") or \
                    (fields[0] == "Default Value:") or \
                    (fields[0] == "Mandatory:") or \
                    (fields[0] == "Parameter Name:")):
                    run = 0
                else:
                    descr = descr + "\n" + PccUtString.trimString(line," \t\n")

        # Get optional fields.
        while (fields[0] == "Range:") or \
              (fields[0] == "Default Value:") or \
              (fields[0] == "Mandatory:"):
            if (fields[0] == "Range:"):
                self.setRange(fields[1])
            elif (fields[0] == "Default Value:"):
                self.setDefaultValue(fields[1])
            elif (fields[0] == "Mandatory:"):
                self.setMandatory(fields[1])
            else:
                raise PccDidException.PccDidException, \
                      "Encountered unexpected field in Parameter Record: " + \
                      fields[0] + " in line " + \
                      str(didBuf.getIndex() + 1) + "."
            line = PccUtString.trimString(didBuf.getNextEl(), " \t\n")
            fields = self.splitField(line)

        self.setParameterName(extrField[0])
        self.setClass(extrField[1])
        self.setContext(extrField[2])
        self.setType(extrField[3])
        self.setValueFormat(extrField[4])
        self.setUnit(extrField[5])
        self.setCommentFormat(extrField[6])
        self.setDescription(descr)

        # Rewind the list pointer one (should point to last line of
        # previous record (this record).
        didBuf.getPrevEl()

        return self


    def setParameterName(self,
                         parameterName):
        """
        Set name of parameter of object.

        parameterName:     Name of parameter.

        Returns:           Reference to object itself.
        """
        # IMPL.: Check format etc.
        self.__parameterName = parameterName
        return self


    def getParameterName(self):
        """
        Return name of parameter.

        Returns:           Name of parameter.
        """
        return self.__parameterName


    def setClass(self,
                 didClass):
        """
        Set the Class of the object.

        didClass:        Class of DID record.

        Returns:         Reference to object itself.
        """
        # IMPL.: Check format etc.
        self.__class = didClass
        return self


    def getClass(self):
        """
        Return Class for DID record.

        Returns:       Class name for DID record.
        """
        return self.__class
    

    def setContext(self,
                   context):
        """
        Set the Context for DID record.

        context:       Context for DID record.

        Returns:       Reference to object itself.
        """
        self.__context = context
        return self


    def getContext(self):
        """
        Return the Context for the DID record.

        Returns:         Context reference of object.
        """
        return self.__context

                         
    def setType(self,
                type):
        """
        Set the Type of the keyword.

        type:        Type of keyword (string, logical, integer, double).

        Returns:     Reference to object itself.
        """
        # IMPL.: Check format etc.
        self.__type = type
        return self


    def getType(self):
        """
        Return Type of the keyword.

        Returns:       Type of keyword.
        """
        return self.__type
        
                 
    def setValueFormat(self,
                       valueFormat):
        """
        Set Value Format for keyword.

        valueFormat:   Value Format for keyword.

        Returns:       Reference to object itself.
        """
        # IMPL.: Check format etc.
        self.__valueFormat = valueFormat
        return self


    def getValueFormat(self):
        """
        Return the Value Format of the keyword.

        Returns:     Value Format for keyword.  
        """
        return self.__valueFormat
        
                 
    def setUnit(self,
                unit):
        """
        Set Unit for the keyword.

        unit:        Unit of keyword.

        Returns:     Reference to object itself.
        """
        # IMPL.: Check format etc.
        self.__unit = unit
        return self


    def getUnit(self):
        """
        Return Unit for keyword.

        Returns:   Unit for keyword.
        """
        return self.__unit

                         
    def setCommentFormat(self,
                         commentFormat):
        """
        Set the Comment Format for the keyword.

        commentFormat:   Comment Format.
        
        Returns:         Reference to object itself.
        """
        self.__commentFormat = commentFormat
        return self


    def getCommentFormat(self):
        """
        Return the Comment Format of the object.

        Returns:        Comment Format of object.
        """
        return self.__commentFormat
                         

    def setDescription(self,
                       description):
        """
        Set the Description of the keyword.

        description:     Description for keyword.

        Returns:         Reference to object itself.
        """
        self.__description = PccUtString.trimString(description, " \t\n")
        return self


    def getDescription(self):
        """
        Return Description of DID record.

        Returns:        Description.
        """
        return self.__description


    def setRange(self,
                 range):
        """
        Set Range for the DID record.

        range:          Range.

        Returns:        Reference to object itself.
        """
        # IMPL.: Check syntax.
        self.__range = range
        return self


    def getRange(self):
        """
        Return Range of DID record.

        Returns:       Range.
        """
        return self.__range

                         
    def setDefaultValue(self,
                        defaultValue):
        """
        Set Default Value for keyword.

        defaultValue:   Default Value for key.

        Returns:        Reference to object itself.
        """
        self.__defaultValue = defaultValue
        return self


    def getDefaultValue(self):
        """
        Return the Default Value for the key.

        Returns:      Default Value.
        """
        return self.__defaultValue

                         
    def setMandatory(self,
                     mandatory):
        """
        Set the Mandatory Flag of the DID record.

        mandatory:     Mandatory Flag (0 or 1).

        Returns:       Reference to object itself.
        """
        self.__mandatory = str(mandatory)
        return self


    def getMandatory(self):
        """
        Return Mandatory Flag (0 or 1).
        
        Returns:     Mandatory Flag.
        """
        return self.__mandatory


    def dumpBuf(self):
        """
        Dump information for DID Record stored in object into string
        buffer and return this.

        Returns:      ASCII dump of DID Record.
        """
        recBuf = ""
        recBuf = recBuf + "Parameter Name:     " + self.getParameterName()
        recBuf = recBuf + "\nClass:              " + self.getClass()
        recBuf = recBuf + "\nContext:            " + self.getContext()
        recBuf = recBuf + "\nType:               " + self.getType()
        recBuf = recBuf + "\nValue Format:       " + self.getValueFormat()
        recBuf = recBuf + "\nUnit:               " + self.getUnit()
        recBuf = recBuf + "\nComment Format:     " + self.getCommentFormat()
        recBuf = recBuf + "\nDescription:  \n" + self.getDescription()
        if (self.getRange() != ""):
            recBuf = recBuf + "\nRange:              " + self.getRange()
        if (self.getDefaultValue() != ""):
            recBuf = recBuf + "\nDefault Value:      " + self.getDefaultValue()
        if ((self.getMandatory() != "0") and (self.getMandatory() != "")):
            recBuf = recBuf + "\nMandatory:          " + self.getMandatory()
        return recBuf


    def dump(self):
        """
        Dump information for DID Record on stdout.

        Returns:      Void.
        """
        print self.dumpBuf()
        
#
# ___oOo___
