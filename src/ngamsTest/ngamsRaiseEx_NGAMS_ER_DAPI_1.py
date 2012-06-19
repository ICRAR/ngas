#
#    ALMA - Atacama Large Millimiter Array
#    (c) European Southern Observatory, 2002
#    Copyright by ESO (in the framework of the ALMA collaboration),
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#

#******************************************************************************
#
# "@(#) $Id: ngamsRaiseEx_NGAMS_ER_DAPI_1.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  10/05/2001  Created
#

"""
This DAPI exits with an NGAMS_ER_DAPI exception, which should make the
NG/AMS Server Back-Log Buffer the file.

The errors in the stack are: NGAMS_ER_DAPI and NGAMS_ER_DB_COM.
"""

from ngams import *

# DAPI function.
def ngamsRaiseEx_NGAMS_ER_DAPI_1(srvObj,
                               reqPropsObj):
    """
    Test DAPI, which raises an NGAMS_ER_DAPI Exception.

    Returns:  Void.
    """
    raise Exception, genLog("NGAMS_ER_DAPI",
                            [genLog("NGAMS_ER_DB_COM", ["TEST EXCEPTION"])])

# EOF
