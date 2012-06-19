#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccKey.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  21/11/2000  Created
#

"""
Contains classes functions to handle a FITS/PAR keyword card.
"""

import types
import PccUtString, PccUtUtils


def splitKeywordLine(keywordLine):
    """
    Function that splits up a keyword line into
    its basic components.

    **keywordLine:**        Keyword line to be split.

    **Returns:**            List containing [<key>, <value>, <comment>]. The
                            <value> is in raw format.
    """
    PccUtUtils.checkType("keywordLine", keywordLine,
                         "PccKey.splitKeywordLine()", types.StringType)
    
    line = PccUtString.trimString(keywordLine, " \t")
    ll = len(line)

    # Get key.
    i = 0
    key = ""
    while ((i < ll) and (line[i] != " ") and (line[i] != "\t")):
        key = key + line[i]
        i = i + 1
    key = PccUtString.trimString(key, "; \t")
    
    # Skip blanks between keyword and value.
    while ((i < ll) and ((line[i] == " ") or (line[i] == "\t"))): i = i + 1

    # Get value.
    value = ""
    while ((i < ll) and (line[i] != "\0") and (line[i] != ";") and
           (line[i] != "#")):
        value = value + line[i]
        i = i + 1
    value = PccUtString.trimString(value, " ")

    # Skip blanks, semicolon(s) and hash signs between value and the comment.
    while ((i < ll) and ((line[i] == " ") or (line[i] == "\t") or
                         (line[i] == ";") or (line[i] == "#"))):
        i = i + 1

    # Get comment.
    comment = ""
    if (i < ll):
        comment = line[i:]
        comment = PccUtString.trimString(comment, " ")

    return [key, value, comment]


class PccKey:
    """
    Class to handle the information in connection with one FITS or PAF
    keyword.
    """


    def __init__(self,
                 key = "",
                 value = "",
                 comment = ""):
        """
        Constructor method initializing the class
        variables of the object.

        **key:**      Keyword name.
        
        **value:**    Value.
        
        **comment:**  Comment.
        """
        self.__preamble = ""
        self.__key = key
        self.__value = PccUtString.trimString(str(value), " ")
        self.__comment = comment


    def setFieldsKeywordLine(self,
                             keywordLine):
        """
        Set the members of the class
        from a PAF keyword line.

        **keywordLine:**      Reference to PAF keyword line.

        **Returns:**          Reference to object itself.
        """
        lineEls = splitKeywordLine(keywordLine)
        self.setKey(lineEls[0])
        self.setValue(lineEls[1])
        self.setComment(lineEls[2])
        return self


    def setFieldsKeywordBlock(self,
                              lineBufList):
        """
        This method takes a number of lines extracted from a PAF file
        and sets the fields of the PAF object accordingly.

        **lineBufList:**  List containing the lines for the next keyword block.

        **Returns:**      Reference to object itself.
        """
        for line in lineBufList:
            line = PccUtString.trimString(line, " \t") 
            if ((line[0] == "#") or (line[0] == "\n")):
                self.__preamble = self.__preamble + line
            else:
                self.setFieldsKeywordLine(line)
                return
        return self

 
    def setKey(self,
               key):
        """
        Set the keyword
        of the object.

        **key:**         Name of keyword.

        **Returns:**     Reference to object itself.
        """
        self.__key = PccUtString.trimString(key, " \"\n\t")
        return self


    def getKey(self):
        """
        Return keyword
        for object.

        **Returns:**        Name of keyword.
        """
        return self.__key


    def setValue(self,
                 value):
        """
        Set the value of
        the object.

        **value:**           Value for object as string.

        **Returns:**         Reference to object itself.
        """
        self.__value = PccUtString.trimString(value, " \"\n\t")
        return self


    def getValue(self):
        """
        Return the value of
        the object.

        **Returns:**         Value of object.
        """
        return self.__value


    def setComment(self,
                   comment):
        """
        Set the comment
        of the object.

        **comment:**     Reference to string object containing comment.

        **Returns:**     Reference to object itself.
        """
        self.__comment = PccUtString.trimString(comment, " \"\n\t")
        return self
        

    def getComment(self):
        """
        Return comment
        of object.

        **Returns:**      String object with comment.
        """
        return self.__comment


    def setPreamble(self,
                    commentLines):
        """
        Set preamble
        of object.

        **commentLines:**   Preamble (comment lines before keyword contained in
                            this object).
        
        **Returns:**        Reference to object itself.
        """
        self.__preamble = commentLines
        return self


    def getPreamble(self):
        """
        Return the preamble
        for the keyword.

        **Returns:**        String buffer with preamble.
        """
        return self.__preamble


    def getKeywordLine(self,
                       keywordMargin = 25,
                       putValueQuotes = "no"):
        """
        Generate a PAF keyword line from the contents of the object and
        return this in a string buffer.

        **keywordMargin:**  Number of characters to reserve for the keyword.
        
        **putValueQuotes:** Indicates if quotes should be put around the value.

        **Returns:**        Reference to string object containing the
                            generated PAF keyword line.
        """
        # 1                  25 27             52 54                     
        # |                   | |               | |                      
        # INS.OPTI3.ID.TYPExxxx string;         # Keyword type
        if (len(self.getComment()) > 0):
            if (putValueQuotes == "no"):
                format = '%-' + str(keywordMargin) + 's %s; # %s'
            else:
                format = '%-' + str(keywordMargin) + 's "%s"; # %s'
            line = format %\
                   (self.getKey(), self.getValue(), self.getComment())
        else:
            if (putValueQuotes == "no"):
                format = '%-' + str(keywordMargin) + 's %s'
            else:
                format = '%-' + str(keywordMargin) + 's "%s"'
            line = format % (self.getKey(), self.getValue())
        return line


    def dumpBuf(self,
                addPreamble = 1):
        """
        Dump the contents of the object into a string (PAF keyword line) and
        return this.

        **addPreamble:**   Indicates if a possible preamble before the
                           keyword should be returned as well.

        **Returns:**       Keyline + possible preamble.
        """
        buf = ""
        if (addPreamble == 1):
            buf = buf + self.getPreamble()
        buf = buf + self.getKeywordLine()
        return buf


    def dump(self):
        """
        Write contents of
        object to stdout.

        **Returns:**        Void.
        """
        print PccUtString.trimString(self.dumpBuf(), "\n")
   

if __name__ == '__main__':
    """
    Main test program.
    """
    print splitKeywordLine("DET.CHIP.NO                2;  " +\
                           "# Detector Type 2 = Hawaii")
    print splitKeywordLine("PAF.HDR.START;                 " +\
                           "# Start of PAF Header")
    print splitKeywordLine("DET.CLKP.SWHWCLK1  \"17, 18, 19," +\
                           "20, 21, \";")
    print splitKeywordLine("DET.CLKP.SWHWCLK3  \"  27,33,47\"")

#
# ___oOo___
