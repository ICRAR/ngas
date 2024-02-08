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

import logging

from . import ngamsDbNgasCfg, ngamsDbNgasDisks, ngamsDbNgasDisksHist
from . import ngamsDbMirroring, ngamsDbNgasCache, ngamsDbJoin
from . import ngamsDbNgasContainers
from . import ngamsDbNgasFiles, ngamsDbNgasHosts, ngamsDbNgasSubscribers


logger = logging.getLogger(__name__)

class ngamsDb(ngamsDbNgasCache.ngamsDbNgasCache,
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


def __params_for_log(params):
    copy = {}
    for k,v in params.items():
        if k.lower() in ('pwd', 'password', 'passwd'):
            v = '*****'
        copy[k] = v
    return copy

def from_config(cfg, maxpool=None):
    """
    Create a database object from a configuration object. If `maxpool` is not
    `None`, it overrides the value loaded from the configuration for the number
    of connections held by the pool.
    """

    driver   = cfg.getDbInterface()
    creSnap  = cfg.getDbSnapshot()
    drvPars  = cfg.getDbParameters()
    maxpool  = maxpool or cfg.getDbMaxPoolCons()
    sess_sql = cfg.getDbSessionSql()
    use_file_ignore = cfg.getDbUseFileIgnore()
    use_prepared_statements = cfg.getDbUsePreparedStatements()

    # HACK, HACK, HACK
    # The sqlite3 doesn't allow by default to make call to objects created on
    # one thread from a different one because underneath sqlite doesn't support
    # concurrency well enough. On NGAS we use semaphores already anyway to make
    # sure only one thread is using a database object at a time, so we can avoid
    # this check. Exposing this detail to the users is not nice though, so
    # instead of forcing them to include this option in their configuration
    # files we inject it ourselves.
    # If we restructure the ngamsDbCore code to use a thread and a queue to make
    # all the SQL calls from the same thread when necessary we wouldn't need
    # this bit of code, which would be the ideal world.
    if driver == 'sqlite3':
        drvPars['check_same_thread'] = False

    logger.info("Connecting to DB with module %s", driver)
    msg = "Additional DB parameters: snapshot: %d, params: %r"
    logger.debug(msg, creSnap, __params_for_log(drvPars))
    return ngamsDb(driver, parameters = drvPars, createSnapshot = creSnap,
                   maxpoolcons = maxpool, use_file_ignore=use_file_ignore,
                   session_sql=sess_sql, use_prepared_statements=use_prepared_statements)
