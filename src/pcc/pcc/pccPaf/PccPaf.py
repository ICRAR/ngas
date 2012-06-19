#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccPaf.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  21/11/2000  Created
#

"""
Module providing the PccPaf class to handle PAF files.

See documentation for PccPaf for futher information.
"""

import sys
import PccUtString, PccUtList, PccKey


class PccPaf:
    """
    Class to handle PAF files programmatically from Python. It is possible
    to load and save PAF files. Each keyword in the PAF file is contained
    in a PccKey object which can be accessed from the PccPaf object.
    """

    def __init__(self,
                 allowRepetition):
        """
        Constructor method for the object. Keyword repetition is enabled
        with 1. Otherwise 0 must be given in.

        allowRepetition: Indicates if keyword repetition should be allowed
                             (=1). Otherwise it can be disabled (=0).
        """
        self.__fileName = ""
        self.__allowRepetition = allowRepetition
        self.__keyObjs = {}
        self.__keyList = []
        self.__keyCount = {}
        self.__trailer = []


    def load(self,
             fileName):
        """
        Load PAF file into object. Complete path should be given if
        PAF file not stored in directory from where the application loading
        is called.
        
        fileName:    Name of PAF file to load.

        Returns:     Reference to object itself.
        """
        fileBuf = PccUtList.PccUtList()
        fileBuf.loadFileInList(fileName)

        # Parse always until next key or end of file.
        lineBuf = []
        while (fileBuf.remainingEls() > 0):
            line = PccUtString.trimString(fileBuf.getNextEl(), " \t")
            lineBuf.append(line)
            if ((line[0] != "\n") and (line[0] != "#")):
                # Handle the next block.
                key = PccKey.PccKey()
                key.setFieldsKeywordBlock(lineBuf)
                self.addKey(key)
                lineBuf = []

        # Keep also the final lines (after the last keyword).
        self.__trailer = lineBuf

        self.__setFileName(fileName)
        
        return self
        

    def __setFileName(self,
                    fileName):
        """
        Set the filename loaded into the object.

        fileName:   Name of PAF file loaded.

        Returns:    Reference to object itself
        """
        self.__fileName = fileName
        return self


    def getFileName(self):
        """
        Return the name of the PAF file loaded.

        Returns:   Name of PAF file.
        """
        return self.__fileName


    def addKey(self,
               keyObj):
        """
        Add a key in the object.

        keyObj:   Reference to instance of the PccKey class.
        
        Returns:  Reference to object itself.
        """
        # Note, internal key number index starts with 1.
        if (self.__keyCount.has_key(keyObj.getKey()) == 0):
            self.__keyCount[keyObj.getKey()] = 0
        if (self.getAllowRepetition() == 1):
            # If repetition is enabled, just add the keyword.
            count = self.__incKeyCount(keyObj.getKey())
            key = keyObj.getKey() + "___" + str(count)
            self.__keyObjs[key] = keyObj
            self.__keyList.append(key)
        else:
            # If repetition is not enabled, check if the keyword is there,
            # if yes, replace the existing one.
            count = set.setKeyCount_(keyObj.getKey(), 1)
            key = keyObj.getKey() + "___" + str(count)
            if (self.__keyList.count(key) == 0):
                self.__keyList.append(key)
                self.__keyObjs[key] = keyObj

        return self


    def __setKeyCount(self,
                      key,
                      count):
        """
        Set internal key counter.

        key:      Name of key.
        
        count:    Count.

        Returns:  Key counter for that key.
        """
        self.__keyCount[key] = count
        return self.__keyCount[key]


    def __incKeyCount(self,
                      key):
        """
        Increment internal key counter.

        key:       Name of key.

        Returns:   Incremented key count for the key.
        """
        if (self.__keyCount.has_key(key) == 0): self.__keyCount[key] = 0
        self.__keyCount[key] = self.__keyCount[key] + 1
        return self.__keyCount[key]


    def __decKeyCount(self,
                      key):
        """
        Decrease key count for a specific key.

        key:       Name of key.

        Returns:   Decremented key count for the key.
        """
        if (self.__keyCount.has_key(key) == 0): self.__keyCount[key] = 0       
        self.__keyCount[key] = self.__keyCount[key] - 1
        return self.__keyCount[key]


    def getKeyCount(self,
                    key):
        """
        Return the internal key counter for a specific key, indicating
        how many instances are contained of this key in the object.

        key:       Name of the key.

        Returns:   Number of keyword objects for the specified key. 
        """
        if (self.hasKey(key) == 1):
            return self.__keyCount[key]
        else:
            return 0


    def getNoOfKeys(self):
        """
        Get the total number of keys stored in the object.

        Returns:   Total number of keys stored in object.
        """
        return len(self.__keyList)


    def getAllowRepetition(self):
        """
        Return the flag indicating if repetition is allowed.

        Returns:   Keyword Repitition Flag (0 or 1).
        """
        return self.__allowRepetition


    def getKeyObj(self,
                  key,
                  no = 0):
        """
        Get a key object referred to by the key name. If repetition is
        enabled, a certain keyword number can be queried. The first
        occurrence has number 0.

        key:           Name of the keyword.

        no:           Number of the keyword in case keyword repetition is
                           enabled.

        Returns:       PccKey keyword object.
        """
        keyword = key + "___" + str(no + 1)
        return self.__keyObjs[keyword]


    def getKeyObjs(self,
                   key,
                   value = None):
        """
        Return list with all keyword objects. It is possible to indicate
        in addition, a value that the keywords must have.

        key:            Name of the keyword.

        value:          Value the keywords selected must have. The pattern
                            given in, is matched according to the rules of
                            Regular Expression.

        Returns:        List with the keywords found.
        """
        keyList = []
        keys = self.__keyObjs
        tmpKey = key + "___"
        for keyIdx in keys:
            if ((string.find(keyIdx, tmpKey) != -1) and (value != None)):
                if (re.search(value,self.__keyObjs[keyIdx].getValue())!=None):
                    keyList.append(self.__keyObjs[keyIdx])
            elif (string.find(keyIdx, tmpKey) != -1):
                keyList.append(self.__keyObjs[keyIdx])
        return keyList
        

    def getKeyValue(self,
                    key,
                    no = 0):
        """
        Get a key value referred to by the key name. If repetition is
        enabled, a certain keyword number can be queried. The first
        occurrence has number 0.

        key:           Name of the keyword.

        no:            Number of the keyword in case keyword repetition is
                           enabled.

        Returns:       The value of the keyword as a string.        
        """
        keyword = key + "___" + str(no + 1)
        return self.__keyObjs[keyword].getValue()


    def getKeyObjNo(self,
                    no):
        """
        Get a keyword object according to its number. First keyword has
        number 0.

        no:          Number (index) of keyword.

        Returns:     Reference to requested PccKey object.
        """
        return self.__keyObjs[self.__keyList[no]]


    def getKeyValueNo(self,
                      no):
        """
        Get a keyword object according to its number. First keyword has
        number 0.

        no:           Get the value of the reference key.

        Returns:      Value of requested key (as string).
        """
        return self.__keyObjs[self.__keyList[no]].getValue()


    def hasKey(self,
               key):
        """
        Returns 1 if the keyword is contained in the object,
        otherwise 0.

        key:        Name of the keyword to probe for.

        Returns:    Flag indicating if key is contained in buffer (0 oe 1).
        """
        return self.__keyCount.has_key(key)


    def dumpBuf(self):
        """
        Dump keywords stored in PAF object into a string buffer.

        Returns:   Generated PAF in string buffer.
        """
        pafBuf = ""
        for key in self.__keyList:
            pafBuf = pafBuf + self.__keyObjs[key].dumpBuf() + "\n"
        for line in self.__trailer:
            pafBuf = pafBuf + line
        return pafBuf
            

    def dump(self):
        """
        Dump the contents of the PAF object on stdout.

        Returns:   Reference to object itself.
        """
        print PccUtString.trimString(self.dumpBuf(), "\n")
        return self

   
# Test main function loading in a PAF into the object.
if __name__ == '__main__':
    """
    Small main/test program loading in a PAF into an instance of PccPaf
    and dumping the contents to stdout.
    """
    if (len(sys.argv) != 2):
        print "Correct usage:\n"
        print "% PccPaf <paf file>\n\n"
        sys.exit(1)

    paf = PccPaf(1)
    paf.load(sys.argv[1])
    paf.dump()

#
# ___oOo___
