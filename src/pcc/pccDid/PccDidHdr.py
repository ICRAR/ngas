#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccDidHdr.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  09/11/2000  Created
#

"""
Class representing a DID header.
"""

import sys, string
from PccLog import *
import PccUtString
import PccDidException

import PccDidBase, PccDidRevRec


class PccDidHdr(PccDidBase.PccDidBase):
    """
    Class to handle information in DID header.

    For further information about the format of DIDs, consult the
    DICB Definition Document ("dicb":http://www.eso.org/dicb/ ).
    """

        
    def __init__(self,
                 name           = "",
                 scope          = "",
                 source         = "",
                 versionControl = "",
                 revRecs        = None):
        """
        Constructor method that initializes the members of the class.

        name:             Name of DID.
        
        scope:            Scope for DID
        
        source:           Source of DID.
        
        versionControl:   Version control string for DID.
        
        revRecs:          List of Revision Records.
        """
        self.__name = name
        self.__scope = scope
        self.__source = source
        self.__versionControl = versionControl
        if (revRecs != None):
            self.__revRecs = revRecs
        else:
            self.__revRecs = []


    def getHdrFields(self,
                     pccListDidBuf):
        """
        Extract information for a DID header from a DID loaded
        into a PccList class.

        pccListDidBuf:    Instance of PccList into which the contents of
                          the DID is loaded.

        Returns:          Reference to the object itself.
        """

        # Extract header elements. Note this parsing is not doing a
        # thorough syntax check of the header.
        expFieldList = ["Dictionary Name:", "Scope:", "Source:",
                        "Version Control:"]
        count = 0
        while ((count < len(expFieldList)) and \
               (pccListDidBuf.remainingEls() > 0)):
            line = PccUtString.trimString(pccListDidBuf.getNextEl(), " \t\n")
            if ((line != "") and (line[0] != "#")):
                fields = self.splitField(line)
                if (fields[0] != expFieldList[count]):
                    raise PccDidException.PccDidException, \
                          "Encountered unexpected field in DID Header: \"" + \
                          fields[0] + "\" in line " + \
                          str(pccListDidBuf.getIndex() + 1) + "."
                else:
                    if (fields[0] == expFieldList[0]):
                        self.setName(fields[1])
                    elif (fields[0] == expFieldList[1]):
                        self.setScope(fields[1])
                    elif (fields[0] == expFieldList[2]):
                        self.setSource(fields[1])
                    else:
                        self.setVersionControl(fields[1])
                    count = count + 1
        if (count < len(expFieldList)):
            raise PccDidException.PccDidException, \
                  "Missing field(s) in DID Header!"
        
        # Find the possible Revision Records.
        run = 1
        while ((run == 1) and (pccListDidBuf.remainingEls() > 0)):
            line = PccUtString.trimString(pccListDidBuf.getNextEl(), " \t\n")
            if (line != "") and (line[0] != "#"):
                fields = self.splitField(line)
                if (fields[0] == "Revision:"):
                     revRec = PccDidRevRec.PccDidRevRec()
                     revRec.getRevFields(pccListDidBuf)
                     self.__revRecs.append(revRec)
                elif (fields[0] == "Parameter Name:"):
                    run = 0

        # Rewind the PCC List Object in order not to point to the
        # next record.
        pccListDidBuf.getPrevEl()

        return self


    def addRevRec(self,
                  revRec):
        """
        Add a Revision Record to the object.

        revRec:    Reference to instance of PccDidRevRec class.

        Returns:   Reference to object itself.
        """
        self.__revRecs.append(revRec)
        return self


    def getNoOfRevRecs(self):
        """
        Return the number of Revision Records contained in the object.

        Returns:   Number of Revision Records.
        """
        return len(self.__revRecs)


    def getRevRec(self,
                  no):
        """
        Get a specific Revision Record from the object.

        no:         Number (index) of the Revision Record. First record is
                    number 0.

        Returns:    Reference to requested instance of the PccDidRevRec
                    object.
        """
        return self.__revRecs[no]


    def getLastRevRec(self):
        """
        Get the last Revision Records stored in the object.

        Returns:    None or Reference to the last Revision Record contained
                    in object.
        """
        if (len(self.__revRecs) > 0):
            return self.__revRecs[len(self.__revRecs) - 1]
        else:
            return None


    def setName(self,
                name):
        """
        Set the name of the DID.

        name:       Name of DID.

        Returns:    Reference to object itself.
        """
        self.__name = name
        return self


    def getName(self):
        """
        Return the name of the DID.

        Returns:   Name of DID.
        """
        return self.__name


    def setScope(self,
                 scope):
        """
        Set Scope for DID.

        scope:     Scope of DID.

        Returns:   Reference to object itself.
        """
        self.__scope = scope
        return self


    def getScope(self):
        """
        Return the Scope of the DID.

        Returns:   Scope of DID.
        """
        return self.__scope

    
    def setSource(self,
                  source):
        """
        Set the Source of the DID.

        source:     Source of DID.

        Returns:   Reference to object itself.
        """
        self.__source = source
        return self


    def getSource(self):
        """
        Return Source of DID.

        Returns:  Source of DID.
        """
        return self.__source

    
    def setVersionControl(self,
                          versionControl):
        """
        Set the Version Control String of the DID.

        versionControl:   Version Control String.

        Returns:          Reference to object itself.
        """

        # The version control string in did_dictionaries, may be something
        # like (RCS/CMM):
        #
        # $Revision: 1.1 $/$Date: 2008/07/02 21:48:10 $
        # $Id: PccDidHdr.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $
        #
        # We only want the version number itself and has to extract this.
        # We look for the first element which is a float.
        #
        # IMPL.: This scheme can be discussed and may need to be refined
        # at a later stage.
        run = 1
        idx = 0
        vcList = string.split(versionControl, " ")
        ver = "-1.0"
        while ((run == 1) and (idx < len(vcList))):
            el = PccUtString.trimString(vcList[idx], " +")
            try:
                #fl = float(el)
                #ver = str(fl)
                ver = el
                run = 0
            except ValueError, e:
                idx = idx + 1
        self.__versionControl = ver
        return self


    def getVersionControl(self):
        """
        Return the Version Control String.

        Returns:   Version Control String.
        """
        return self.__versionControl
                 

    def dumpBuf(self):
        """
        Dump DID header into string object and return this.

        Returns:     Reference to string object containing the DID header.
        """
        # Dump Main header information.
        hdrBuf = ""
        hdrBuf = hdrBuf + "Dictionary Name:    " + self.getName() + "\n"
        hdrBuf = hdrBuf + "Scope:              " + self.getScope() + "\n"
        hdrBuf = hdrBuf + "Source:             " + self.getSource() + "\n"
        hdrBuf = hdrBuf + "Version Control:    " + self.getVersionControl()

        # Dump revision records.
        for revRec in self.__revRecs:
            hdrBuf = hdrBuf + "\n" + revRec.dumpBuf()
        
        return hdrBuf


    def dump(self):
        """
        Dump the DID header on stdout.

        Returns:   Void.
        """
        print self.dumpBuf()
        
        
#
# ___oOo___
