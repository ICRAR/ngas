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
# "@(#) $Id: ngamsCmdHandling.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/01/2002  Created
#
"""
Contains various functions for handling commands.
"""

import importlib
import logging
import sys

from ngamsLib.ngamsCore import NGAMS_SUCCESS

from . import NoSuchCommand


logger = logging.getLogger(__name__)

_builtin_cmds = {
    'ARCHIVE', 'BBCPARC', 'CACHEDEL', 'CAPPEND', 'CARCHIVE', 'CCREATE',
    'CDESTROY', 'CHECKFILE', 'CLIST', 'CLONE', 'CONFIG', 'CREMOVE', 'CRETRIEVE',
    'DISCARD', 'EXIT', 'HELP', 'INIT', 'LABEL', 'LARCHIVE', 'OFFLINE', 'ONLINE', 'QARCHIVE',
    'QUERY', 'REARCHIVE', 'REGISTER', 'REMDISK', 'REMFILE', 'RETRIEVE', 'STATUS',
    'SUBSCRIBE', 'UNSUBSCRIBE'
}

# The reload function has moved around a bit
if sys.version_info[0] < 3:
    from __builtin__ import reload
elif sys.version_info[0:2] < (3, 4):
    from imp import reload
else:
    reload = importlib.reload


def handle_cmd(srvObj, reqPropsObj, httpRef):
    """Passes down the request to the corresponding command handling module"""

    msg = _get_module(srvObj, reqPropsObj).handleCmd(srvObj, reqPropsObj, httpRef)
    if msg is not None:
        if httpRef.reply_sent:
            logger.warning("Module returned message to send back to client, but reply has been sent, ignoring")
            return
        status = NGAMS_SUCCESS
        code = 200
        if isinstance(msg, (tuple, list)):
            assert len(msg) == 3
            code, status, msg = msg
        httpRef.send_status(msg, status=status, code=code)

def _get_module(server, request):

    # Interpret the command + parameters.
    cmd = request.getCmd()
    logger.info("Received command: %s", cmd)

    # Special handling for certain commands
    # TODO: these should certainly disappear at some point
    if cmd == 'robots.txt':
        cmd = 'robots'
    if cmd == 'favicon.ico':
        cmd = 'favicon'
    if cmd == "ngamsInternal.dtd":
        request.setCmd('RETRIEVE').addHttpPar("internal", cmd)
        cmd = 'RETRIEVE'

    # Is it a built-in or a plug-in?
    if cmd in _builtin_cmds:
        modname = __package__ + '.commands.' + cmd.lower()
    elif cmd in server.cfg.cmd_plugins:
        modname = server.cfg.cmd_plugins[cmd]
    else:
        modname = 'ngamsPlugIns.ngamsCmd_%s' % cmd

    # Reload the module if requested.
    reload_mod = 'reload' in request and int(request['reload']) == 1

    # We previously checked sys.modules before doing import_module, but that's
    # unnecessary, and moreover thread-usafe, since modules are put in sys.modules
    # before they are fully built (leading to errors like "module 'ngamsServer.commands.qarchive'
    # has no attribute 'handleCmd'", which it definitly has)
    logger.debug("Importing dynamic command module: %s", modname)
    try:
        mod = importlib.import_module(modname)
    except ImportError:
        logger.error("No module %s found", modname)
        raise NoSuchCommand()
    except:
        logger.exception("Error while importing %s", modname)
        raise
    if reload_mod:
        logger.debug("Re-loading dynamic command module: %s", modname)
        mod = reload(mod)
    return mod

# EOF