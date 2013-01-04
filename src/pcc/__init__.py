#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: __init__.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# awicenec  29/05/2001  Created
#

"""

                        ######   #####   #####
                        #     # #     # #     #
                        #     # #       #
                        ######  #       #
                        #       #       #
                        #       #     # #     #
                        #        #####   #####

         Copyright (c) 2001-2004, European Southern Observatory
                         All rights reserved.


Python Common Components (PCC). Contains various classes and functions,
of possible common interest for application development in Python.
"""

__all__ = [
	"pccDid",
	"pccFits",
	"pccKey",
	"pccUt",
	"pccLog",
	"pccPaf",
]
from sys import path
path.extend([__path__[0] + '/pccDid',
             __path__[0] + '/pccFits',
             __path__[0] + '/pccKey',
             __path__[0] + '/pccLog',
             __path__[0] + '/pccPaf',
             __path__[0] + '/pccUt'])

__path__.extend([__path__[0] + '/pccDid',
                 __path__[0] + '/pccFits',
                 __path__[0] + '/pccKey',
                 __path__[0] + '/pccLog',
                 __path__[0] + '/pccPaf',
                 __path__[0] + '/pccUt'])

# --- oOo ---
