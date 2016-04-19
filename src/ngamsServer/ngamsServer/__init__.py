#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
#    Copyright by UWA (in the framework of the ICRAR)
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
# "@(#) $Id: __init__.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  11/06/2001  Created

# Dummy __init__.py file to build up the documentation for the module.

__all__ = ["ngamsArchiveCmd",
"ngamsArchiveUtils",
"ngamsAuthUtils",
"ngamsCacheControlThread",
"ngamsCacheDelCmd",
"ngamsCacheServer",
"ngamsCheckFileCmd",
"ngamsCloneCmd",
"ngamsCmdHandling",
"ngamsConfigCmd",
"ngamsDataCheckThread",
"ngamsDiscardCmd",
"ngamsExitCmd",
"ngamsFileUtils",
"ngamsHelpCmd",
"ngamsInitCmd",
"ngamsJanitorThread",
"ngamsLabelCmd",
"ngamsMirroringControlThread",
"ngamsOfflineCmd",
"ngamsOnlineCmd",
"ngamsRearchiveCmd",
"ngamsRegisterCmd",
"ngamsRemDiskCmd",
"ngamsRemFileCmd",
"ngamsRemUtils",
"ngamsRetrieveCmd",
"ngamsServer",
"ngamsSrvUtils",
"ngamsStatusCmd",
"ngamsSubscribeCmd",
"ngamsSubscriptionThread",
"ngamsUnsubscribeCmd",
"ngamsUserServiceThread",
]

import pkg_resources
__doc__ = pkg_resources.resource_string(__name__, 'README')  # @UndefinedVariable

# EOF: