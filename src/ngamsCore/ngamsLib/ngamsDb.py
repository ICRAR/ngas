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
# "@(#) $Id: ngamsDb.py,v 1.6 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  03/02/2006  Created
#

"""
Contains the ngamsDb class
"""

import ngamsDbCore, ngamsDbNgasCfg, ngamsDbNgasDisks, ngamsDbNgasDisksHist
import ngamsDbNgasFiles, ngamsDbNgasHosts, ngamsDbNgasSubscribers
import ngamsDbMirroring, ngamsDbNgasCache, ngamsDbJoin
import ngamsDbNgasContainers


class ngamsDb(ngamsDbCore.ngamsDbCore,
              ngamsDbNgasCache.ngamsDbNgasCache,
              ngamsDbNgasCfg.ngamsDbNgasCfg,
              ngamsDbNgasDisks.ngamsDbNgasDisks,
              ngamsDbNgasDisksHist.ngamsDbNgasDisksHist,
              ngamsDbNgasFiles.ngamsDbNgasFiles,
              ngamsDbNgasHosts.ngamsDbNgasHosts,
              ngamsDbNgasSubscribers.ngamsDbNgasSubscribers,
              ngamsDbMirroring.ngamsDbMirroring,
              ngamsDbJoin.ngamsDbJoin,
              ngamsDbNgasContainers.ngamsDbNgasContainers):
    """
    Front-end class for the DB access module.

    This class inherits from all the DB sub-classes (which in turn inherit from
    ngamsDbCore), thus exposing to the rest of the software a single class that
    implements all the database logic.
    """