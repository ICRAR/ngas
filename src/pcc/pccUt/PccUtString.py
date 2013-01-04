#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccUtString.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  04/05/2000  Created
#

"""
Module providing string manipulating functions.
"""

import string


def trimString(rawString,
               trimChars):
    """
    Trims a string from both ends according to the list of characters
    given in the "trimChars" input parameter.
    
    rawString:     String to be trimmed.
    
    trimChars:     Characters to trim away from the ends of "rawString".
    
    Returns:       The trimmed string.
    """
    if (len(rawString) == 0):
        return ""
    idx1 = 0
    lenRawStr = (len(rawString) - 1)
    while ((string.find(trimChars, rawString[idx1]) != -1) and
           (idx1 < lenRawStr)):
        idx1 = (idx1 + 1)
    if (idx1 == lenRawStr) and (string.find(trimChars, rawString[idx1]) == -1):
        return rawString[idx1]
    elif (idx1 == lenRawStr):
        return ""
    idx2 = (len(rawString) - 1)
    while (string.find(trimChars, rawString[idx2]) != -1):
        idx2 = (idx2 - 1)
    trimmedString = rawString[idx1:(idx2 + 1)]

    return trimmedString


def lastChar(txt):
    """
    Return the last char of a string.

    txt:     Variable referring to the string.
    """
    return txt[len(txt) - 1]


#
# ___oOo___
