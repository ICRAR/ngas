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

# Dummy __init__.py file to make it possible to view the README
# file of the ngamsLib sub-module using e.g. pydoc.

__all__ = ["ngamsCacheEntry",
"ngamsConfig",
"ngamsConfigBase",
"ngamsContainer",
"ngamsDapiStatus",
"ngamsDb",
"ngamsDbBase",
"ngamsDbCore",
"ngamsDbJoin",
"ngamsDbm",
"ngamsDbMirroring",
"ngamsDbNgasCache",
"ngamsDbNgasCfg",
"ngamsDbNgasContainers",
"ngamsDbNgasDisks",
"ngamsDbNgasDisksHist",
"ngamsDbNgasFiles",
"ngamsDbNgasHosts",
"ngamsDbNgasSubscribers",
"ngamsDiskInfo",
"ngamsDiskUtils",
"ngamsDppiDef",
"ngamsDppiStatus",
"ngamsEvent",
"ngamsFileInfo",
"ngamsFileList",
"ngamsFileSummary1",
"ngamsHighLevelLib",
"ngamsHostInfo",
"ngamsLib",
"ngamsMIMEMultipart",
"ngamsMirroringRequest",
"ngamsMirroringSource",
"ngamsNotification",
"ngamsPhysDiskInfo",
"ngamsPlugInApi",
"ngamsReqProps",
"ngamsSmtpLib",
"ngamsStatus",
"ngamsStorageSet",
"ngamsStream",
"ngamsSubscriber",
"ngamsThreadGroup",
"ngamsXmlMgr",]


import pkg_resources

__doc__ = pkg_resources.resource_string(__name__, 'README')


# EOF

