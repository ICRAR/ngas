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
# "@(#) $Id: ngamsAuthUtils.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  27/09/2002  Created
#

"""
This module utilities used to authorization.
"""

import logging
import base64


logger = logging.getLogger(__name__)

class UnauthenticatedError(Exception):
    """Authentication is required but none has been given"""
    def __init__(self, msg):
        self.msg = msg

class UnauthorizedError(Exception):
    """An authorized user is not allow to run a command"""
    def __init__(self, user):
        self.user = user

def cmdPermitted(cfg, reqPropsObj, reqUser):
    """Whether the command is allows to be run by the user"""

    commands = cfg.getAuthUserCommands(reqUser)
    if not commands:
        return False
    elif '*' == commands:
        return True

    return reqPropsObj.getCmd().strip() in commands.split(',')


def authorize(cfg, reqPropsObj):
    """Check if the request is authorized for the authenticated user, if any"""

    if not cfg.getAuthorize():
        logger.debug("Authorization is disabled, continuing anonymously")
        return

    # For now only Basic HTTP Authentication is implemented.
    if not reqPropsObj.getAuthorization():
        raise UnauthenticatedError('Unauthorized request received')

    auth_parts = reqPropsObj.getAuthorization().split(' ')
    if auth_parts[0] != 'Basic':
        raise UnauthenticatedError('Invalid authentication scheme: ' + auth_parts[0])
    if len(auth_parts) != 2:
        raise UnauthenticatedError('Invalid Basic authentication, missing value')

    user_pass = base64.b64decode(auth_parts[1]).split(b':')
    if len(user_pass) < 2:
        raise UnauthenticatedError('Invalid Basic authentication, no password provided')

    user, password = user_pass[0], b':'.join(user_pass[1:])

    # Get the user from the configuration.
    stored_pass = cfg.getAuthUserInfo(user)
    if not stored_pass:
        raise UnauthenticatedError("unknown user specified")

    # Password matches and command is allowed
    stored_pass = base64.decodestring(stored_pass)
    if password != stored_pass:
        raise UnauthenticatedError("wrong password for user " + user)
    if not cmdPermitted(cfg, reqPropsObj, user):
        raise UnauthorizedError(user)

    logger.info("Successfully authenticated user %s", user)
