#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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
"""Deals with logging rotation files, archiving them, and removing old ones"""

import errno
import functools
import glob
import logging
import os
import socket

from ngamsLib.ngamsCore import rmFile, mvFile, loadPlugInEntryPoint
from ngamsLib import ngamsLib
from ngamsPClient import ngamsPClient


logger = logging.getLogger(__name__)

# Load plug-ins only once and store them globally
_lh_plugins = None
def get_logfile_handler_plugins(cfg):
    global _lh_plugins

    if _lh_plugins is None:
        _lh_plugins = []
        try:
            for name, pars in cfg.logfile_handler_plugins:
                func = loadPlugInEntryPoint(name, 'run')
                pars = ngamsLib.parseRawPlugInPars(pars)
                _lh_plugins.append(functools.partial(func, pars))
        except:
            # Only report the error, but nothing else
            logger.exception("Error while loading logfile handler plug-ins,"
                             "no extra functionality available")

    return _lh_plugins


def try_archiving(cfg, srv, fname):
    if not cfg.getArchiveRotatedLogfiles():
        return
    file_uri = "file://" + fname
    host, port = srv.get_self_endpoint()
    proto = srv.get_server_access_proto()
    ngamsPClient.ngamsPClient(host, port, proto=proto).archive(
        file_uri, 'ngas/nglog')


def run(srvObj, stopEvt):

    cfg = srvObj.getCfg()
    logdir = os.path.dirname(cfg.getLocalLogFile())

    # The LOG-ROTATE-<timestamp>.nglog.unsaved pattern is produced by the
    # ngamsServer.NgasRotatingFileHandler class, which is the file handler
    # attached to the root logger in the server process
    logger.debug("Checking if there are unsaved rotated logfiles")
    for unsaved in sorted(glob.glob(os.path.join(logdir, 'LOG-ROTATE-*.nglog.unsaved'))):

        # Remove the .unsaved bit, leave the rest
        fname = '.'.join(unsaved.split('.')[:-1])
        try:
            mvFile(unsaved, fname)
            try_archiving(cfg, srvObj, fname)
        except Exception as e:
            mvFile(fname, unsaved)
            if isinstance(e, socket.error) and e.errno == errno.ECONNREFUSED:
                # this is expected when the server is just starting up,
                # let's ignore for now
                logger.warning("Server not up yet, postponing processing of %s", unsaved)
                continue
            raise

        # Do additional things with our logfiles
        for plugin in get_logfile_handler_plugins(cfg):
            try:
                plugin(srvObj, fname)
            except:
                logger.exception("Error while handling logfile %s", fname)

    max_rotations = max(min(cfg.getLogRotateCache(), 100), 0)
    logfiles = glob.glob(os.path.join(logdir, 'LOG-ROTATE-*.nglog'))
    logfiles.sort()
    to_remove = max(len(logfiles) - max_rotations, 0)
    logger.info("Found %d old rotated logfiles, will remove oldest %d", len(logfiles), to_remove)
    for f in logfiles[:to_remove]:
        logger.info("Removing rotated logfile %s", f)
        rmFile(f)