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
# "@(#) $Id: ngamsAuthorizationTest.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  25/06/2004  Created
#
"""
This module contains the Test Suite for the Authorization Feature of NG/AMS.
"""

import base64
import contextlib
import os

from ngamsLib import ngamsHttpUtils
from .ngamsTestLib import ngamsTestSuite


class ngamsAuthorizationTest(ngamsTestSuite):
    """
    Synopsis:
    Test the Authorization Service implemented by NG/AMS.

    Description:
    The NG/AMS Server implements the basic HTTP authorization. In this
    Test Suite the proper functioning of this feature is exercised. This
    both with authorization enabled and disabled and valid/non-valid
    authorization code.

    Missing Test Cases:
    - Test internal authorization when:
        - Cloning files between two nodes.
        - Retrieving files between two nodes.
        - Other combinations of retrieve requests.
        - Other commands where a node may act as proxy.
    """

    def _assert_code(self, code, bauth=None, raw_auth=None, cmd='STATUS'):
        auth = raw_auth or 'Basic ' + base64.b64encode(bauth) if bauth else None
        resp = ngamsHttpUtils.httpGet('127.0.0.1', 8888, cmd, auth=auth)
        with contextlib.closing(resp):
            self.assertEqual(code, resp.status)

    def test_NoAuth_1(self):
        """Authentication/authorization is fully disabled"""
        self.prepExtSrv()
        self._assert_code(200)


    def test_UnAuthReq_1(self):
        """Authorization/authentication is enabled"""

        self.prepExtSrv(cfgProps=[["NgamsCfg.Authorization[1].Enable","1"]])
        self._assert_code(401)
        self._assert_code(401, raw_auth="Not basic at all")
        self._assert_code(401, raw_auth="Basic")
        self._assert_code(401, raw_auth="Basic ")
        self._assert_code(401, raw_auth="Basic user")


    def test_UnAuthReq_2(self):
        """Authorization/authentication is enabled, commands are restricted"""

        # Start the server with a set of configured users
        pass1 = os.urandom(16)
        pass2 = os.urandom(16)
        pass3 = os.urandom(16)
        u1 = (('Name', 'test1'), ('Password', base64.b64encode(pass1)), ('Commands', '*'))
        u2 = (('Name', 'test2'), ('Password', base64.b64encode(pass2)), ('Commands', 'STATUS'))
        u3 = (('Name', 'test3'), ('Password', base64.b64encode(pass3)), ('Commands', 'DOESNT_EXIST'))

        cfg = [('Enable', '1')]
        for i, u in enumerate((u1, u2, u3)):
            cfg += [('User[%d].%s' % (i, name), str(val)) for name, val in u]
        cfg = [("NgamsCfg.Authorization[1].%s" % name, val) for name, val in cfg]
        self.prepExtSrv(cfgProps=cfg)

        auth1 = b'test1:' + pass1
        auth2 = b'test2:' + pass2
        auth3 = b'test3:' + pass3

        # No authentication
        self._assert_code(401)

        # Users 1 and 2 are allowed to STATUS, user 3 isn't
        self._assert_code(200, bauth=auth1)
        self._assert_code(200, bauth=auth2)
        self._assert_code(403, bauth=auth3)

        # Users 1 and 3 are allowed to send the DOESNT_EXIST command, user 3 isn't
        # We get 404s here because the command actually doesn't exist, which yields
        # that HTTP code, and also because HTTP authentication happens before
        # command processing
        self._assert_code(404, bauth=auth1, cmd='DOESNT_EXIST')
        self._assert_code(403, bauth=auth2, cmd='DOESNT_EXIST')
        self._assert_code(404, bauth=auth3, cmd='DOESNT_EXIST')