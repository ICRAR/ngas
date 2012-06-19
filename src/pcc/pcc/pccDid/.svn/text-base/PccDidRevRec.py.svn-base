#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccDidRevRec.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  09/11/2000  Created
#

"""
Contains the PccDidRevRec to handle a DID Revision Record.
"""


import string
from PccLog import *
import PccUtString, PccUtTime
import PccDidBase, PccDidException


class PccDidRevRec(PccDidBase.PccDidBase):
    """
    Class to handle information in connection with a DID Revision Record.
    """

    def __init__(self,
                 revision    = "",
                 date        = "",
                 status      = "",
                 description = ""):
        """
        Constructor method initializing the class member variables.

        revision:         Revision string.

        date:             Date for Revision Record.

        status:           Status for this Revision.

        description:      Description of this Revision.
        """
        self.__revision    = revision
        self.__date        = date
        self.__status      = status
        self.__description = description

        
    def getRevFields(self,
                     pccListDidBuf):
        """
        Get the Revision Record fields from a DID loaded into a Python
        list (one element per line).

        Assume that the DID PCC List Object index is referring to the
        start of the next Revision Record.

        pccListDidBuf:  Reference to instance of PccList object
                            containing the lines of the DID.

        Returns:        Void.
        """
        
        # Assume that the DID PCC List Object index is referring to the
        # start of the next Revision Record.
        pccListDidBuf.getPrevEl()

        # Expect a revision record to have the format:
        #
        # Revision:
        # Date:
        # Status:
        # Description:
        expFieldList = ["Revision:", "Date:", "Status:", "Description:"]
        valList = []
        count = 0
        while ((count < len(expFieldList)) and \
               (pccListDidBuf.remainingEls() > 0)):
            line = PccUtString.trimString(pccListDidBuf.getNextEl(), " \t\n")
            if (line != "") and (line[0] != "#"):
                fields = self.splitField(line)
                if (fields[0] != expFieldList[count]):
                    raise PccDidException.PccDidException, \
                          "Encountered unexpected field in Revision " + \
                          "Header: \"" + fields[0] + "\" in line " + \
                          str(pccListDidBuf.getIndex() + 1) + "."
                else:
                    valList.append(fields[1])
                    count = count + 1

        if (len(valList) > 0): self.setRevision(valList[0])
        if (len(valList) > 1): self.setDate(valList[1])
        if (len(valList) > 2): self.setStatus(valList[2])
        if (len(valList) > 3): descr = valList[3]
        
        # Get possible, additional lines for the description.
        run = 1
        while ((run == 1) and (pccListDidBuf.remainingEls() > 0)):
            line = PccUtString.trimString(pccListDidBuf.getNextEl(), " \t\n")
            if (line != "") and (line[0] == "#"):
                run = 0
            else:
                fields = self.splitField(line)
                if ((fields[0] == "Revision:") or \
                    (fields[0] == "Parameter Name:")):
                    run = 0
                else:
                    descr = descr + "\n" + line
            if (pccListDidBuf.remainingEls() == 0): run = 0
        self.setDescription(descr)

        # Rewind the PCC List Object index one in order not to point into
        # the next record.
        pccListDidBuf.getPrevEl()


    def setRevision(self,
                    revision):
        """
        Set Revision string.

        revision:    Revision string.

        Returns:     Reference to object itself.
        """
        # The Revision Record may be something like:
        #
        # $Revision: 1.1 $/$Date: 2008/07/02 21:48:10 $
        # $Id: PccDidRevRec.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $
        #
        # We only want the version number itself and has to extract this.
        # We look for the first element which is a float.
        #
        # IMPL.: This scheme can be discussed and may need to be refined
        # at a later stage.
        run = 1
        idx = 0
        vcList = string.split(revision, " ")
        rev = "0.0"
        while ((run == 1) and (idx < len(vcList))):
            try:
                fl = float(vcList[idx])
                rev = vcList[idx]
                run = 0
            except ValueError, e:
                idx = idx + 1
        self.__revision = rev
        return self


    def getRevision(self):
        """
        Return Revision string of object.

        Returns:        Revision string.
        """        
        return self.__revision


    def setDate(self,
                date):
        """
        Set Date for Revision Record.

        date:      Date in ISO8601 format.
 
        Returns:   Reference to object itself.
        """
        # Check that the format is ISO8601.
        tim = PccUtTime.TimeStamp()
        try:
            tim.initFromTimeStamp(date)
        except ValueError, e:
            raise PccDidException.PccDidException, \
                  "Illegal date format found in Revision Record: " + date + "."
        self.__date = date
        return self


    def getDate(self):
        """
        Return Date for Revision Record.

        Returns:   Date.
        """
        return self.__date

       
    def setStatus(self,
                  status):
        """
        Set Status ofRevision Record.

        status:     Status.

        Returns:    Reference to object itself.
        """
        self.__status = status
        return self


    def getStatus(self):
        """
        Return Status of Revision Record.

        Returns:    Status.
        """
        return self.__status

    
    def setDescription(self,
                       description):
        """
        Set Description of Revision Record.

        description:     Description.

        Returns:         Reference to object itself.
        """
        self.__description = PccUtString.trimString(description, " \t\n")
        return self


    def getDescription(self):
        """
        Return Description of Revision Record.

        Returns:       Description.
        """
        return self.__description


    def dumpBuf(self):
        """
        Dump contents of Revision Record in a string buffer and
        return this.

        Returns:       Revision Record in DID (string) format.
        """
        recBuf = ""
        recBuf = recBuf + "Revision:           " + self.getRevision() + "\n"
        recBuf = recBuf + "Date:               " + self.getDate() + "\n"
        recBuf = recBuf + "Status:             " + self.getStatus() + "\n"
        recBuf = recBuf + "Description:    \n" + self.getDescription()
        return recBuf


    def dump(self):
        """
        Dump the Revision Record to stdout.

        Returns:    Reference to object itself.
        """
        print self.dumpBuf()
        return self

#
# ___oOo___
