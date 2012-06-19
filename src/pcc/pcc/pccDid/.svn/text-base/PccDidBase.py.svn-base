#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccDidBase.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  09/11/2000  Created
#

"""
Base class for the PCC DID project classes.
"""

import string
import PccUtString

class PccDidBase:
    """
    Base class for the PCC DID module.
    """

    def splitField(self,
                   line):
        """
        Split a colon sepated set of two fields.

        The extracted elements returned in a list. The resulting strings
        are trimmed for blanks and quotes.

        line:        Line containing the colon separated set of fields

        Returns:     List with the trimmed fields.
        """
        fields = string.split(line, ":")
        if (len(fields) == 2):
            tag = fields[0] + ":"
            val = fields[1]
        elif (len(fields) > 2):
            tag = fields[0] + ":"
            val = ""
            for el in fields[1:]: val = val + el + ":"
            # Remove the last colon.
            val = val[0:(len(val) - 1)]
        elif (len(fields) == 1):
            tag = fields[0]
            val = ""
        else:
            tag = ""
            val = ""

        return [PccUtString.trimString(tag, " \t\n\""),
                PccUtString.trimString(val, " \t\n\"")]
        

#
# ___oOo___
