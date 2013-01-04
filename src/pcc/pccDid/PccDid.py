#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccDid.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/11/2000  Created
#

"""
Class to handle DIDs.
"""

import sys, exceptions
from PccLog import *
import PccUtString, PccUtList
import PccDidBase, PccDidHdr, PccDidRec, PccDidException


class PccDid(PccDidBase.PccDidBase):
    """
    Class to handle DIDs.

    The class represents a DID and can be used in Python code to query
    information from a DID or to manipulate the contents of a DID.
    """

    def __init__(self):
        """
        Constructor method to initialize class member variables.
        """
        self.__didHdr = PccDidHdr.PccDidHdr()
        self.__didRecs = []
             

    def load(self,
             didFileName,
             permissive = 0):
        """
        Load DID into object.

        didFileName:   Filename of DID.
         
        permissive:    Use Permissive Mode for the parsing of the DID.

        Returns:       Reference to the object itself.
        """        
        info(1,"Loading DID: " + didFileName,"PccDid")
        
        info(2,"Opening DID: " + didFileName,"PccDid")
        didBuf = PccUtList.PccUtList()
        didBuf.loadFileInList(didFileName)

        # Get header.
        if (permissive == 0):
            self.__didHdr.getHdrFields(didBuf)
        else:
            try:
                self.__didHdr.getHdrFields(didBuf)
            except PccDidException.PccDidException, e:
                info(1,"Error in DID format. DID: " + didFileName + "." +\
                     " Permissive Mode is enabled - continuing.")

        # Parse parameter records. Find always the start of the next
        # "Parameter Name:" record and leave it up to the DidRec class to
        # parse in the record.
        while (didBuf.remainingEls() > 0):
            if (self.splitField(didBuf.getNextEl())[0] == "Parameter Name:"):
                didRec = PccDidRec.PccDidRec()
                didRec.getRecFields(didBuf, permissive)
                self.__didRecs.append(didRec)

        return self


    def setDidHdr(self,
                  didHdr):
        """
        Set the DID header of the DID object.

        didHdr:   Reference to instance of PccDidHdr.

        Returns:  Reference to object itself.
        """
        self.__didHdr = didHdr
        return self


    def getDidHdr(self):
        """
        Return reference to DID header of DID.

        Returns:   Reference to instance of PccDidHdr.
        """
        return self.__didHdr


    def addDidRec(self,
                  didRec):
        """
        Add a DID Record in the object.

        didRec:    Reference to instance of the PccDidRec class.

        Returns:   Reference to object itself.
        """
        self.__didRecs.append(didRec)
        return self


    def getDidRecList(self):
        """
        Return reference to list of DID Records.

        Returns:   Reference to list of DID Records.
        """
        return self.__didRecs


    def getNoOfDidRecs(self):
        """
        Return the number of DID Records.

        Returns:  Number of DID Records contained in the object.
        """
        return len(self.__didRecs)


    def getDidRec(self,
                  no):
        """
        Return reference to specific DID Record.

        The record is indicated by its number (index).

        no:        DID Record number. First record is 0.

        Returns:   Reference to instance of PccDidRec class.
        """
        return self.__didRecs[no]


    def dump(self):
        """
        Dump/print the contents of the DID to stdout.

        Returns:   Void.
        """
        print self.dumpBuf()


    def dumpBuf(self):
        """
        Dump contents of DID into a string buffer.

        Returns:   Reference to string object containing the DID.
        """
        dicBuf = ""
        dicBuf = dicBuf + self.__didHdr.dumpBuf() + "\n"
        for rec in self.__didRecs:
            dicBuf = dicBuf + "\n\n" + rec.dumpBuf()
        dicBuf = dicBuf + "\n\n\n# --- oOo ---\n"

        return dicBuf

    

if __name__ == '__main__':
    """
    Small test main program to load in a DID.
    """
    setLogCond(0, "", 0)
    didName = ""
    dump = 0
    permissive = 0
    for par in sys.argv[1:]:
        if (par == "-v"):
            setLogCond(0, "", 2)
        elif (par == "-dump"):
            dump = 1
        elif (par == "-h") or (par == "-help"):
            print "\n% PccDid [-v] [-dump] [-help] [-p] <DID>\n"
            sys.exit(0)
        elif (par == "-p"):
            permissive = 1
        else:
            didName = par
    if (didName == ""):
        print "\n% PccDid [-v] [-dump] [-help] [-p] <DID>\n"
        sys.exit(1)
    did = PccDid()
    #did.load("ESO-VLT-DIC.OBS")
    if (getVerboseLevel() > 0):
        did.load(didName, permissive)
    else:
        try:
            did.load(didName, permissive)
        except exceptions.Exception, e:
            print str(e)
            sys.exit(1)
    if (dump == 1):
        did.dump()



#
# ___oOo___
