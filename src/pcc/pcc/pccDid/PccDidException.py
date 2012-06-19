#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccDidException.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  09/11/2000  Created
#

"""
Exception class for the PCC DID project.
"""

import exceptions

class PccDidException(exceptions.Exception):
    """
    Exception class for PCC DID.
    """

    def __init__(self,
                 args = None):
        """
        Constructor method for exception
        class.

        args:   Arcguments to pass on to the Python Exception class
        """
        self.args = args

#
# ___oOo___
