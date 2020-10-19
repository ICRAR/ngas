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

from ngamsLib import ngamsHttpUtils, utils
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
        auth = raw_auth or (b'Basic ' + base64.b64encode(bauth) if bauth else None)
        resp = ngamsHttpUtils.httpGet('127.0.0.1', 8888, cmd, auth=auth)
        with contextlib.closing(resp):
            self.assertEqual(code, resp.status)

    def test_NoAuth_1(self):
        """Authentication/authorization is fully disabled"""
        self.prepExtSrv()
        self._assert_code(200)

    def _authorization_cfg(self, user_specs, exclude_commands=None):
        uspecs = [(('Name', u[0]),
                   ('Password', utils.b2s(base64.b64encode(u[1]))),
                   ('Commands', u[2]))
                  for u in user_specs]
        cfg = [('Enable', '1')]
        if exclude_commands is not None:
            cfg.append(('Exclude', exclude_commands))
        for i, u in enumerate(uspecs):
            cfg += [('User[%d].%s' % (i, name), str(val)) for name, val in u]
        cfg = [("NgamsCfg.Authorization[1].%s" % name, val) for name, val in cfg]
        return cfg

    def test_UnAuthReq_1(self):
        """Authorization/authentication is enabled"""
        passwd = os.urandom(16)
        self.prepExtSrv(cfgProps=self._authorization_cfg([('test', passwd, '*')]))
        self._assert_code(401)
        self._assert_code(401, raw_auth="Not basic at all")
        self._assert_code(401, raw_auth="Basic")
        self._assert_code(401, raw_auth="Basic ")
        self._assert_code(401, raw_auth="Basic user")

        # Produce a clean shutdown of the server so coverage information
        # is not lost due to the test killing the server with SIGINT
        self.termExtSrv(self.extSrvInfo.pop(), auth=b'test:' + passwd)

    def test_UnAuthReq_2(self):
        """Authorization/authentication is enabled, commands are restricted"""

        # Start the server with a set of configured users
        pass1, pass2, pass3, pass4 = [os.urandom(16) for _ in range(4)]
        uspecs = ('test1', pass1, '*'), ('test2', pass2, 'STATUS'), ('test3', pass3, 'DOESNT_EXIST'), ('test4', pass4, '')
        self.prepExtSrv(cfgProps=self._authorization_cfg(uspecs))

        auth1 = b'test1:' + pass1
        auth2 = b'test2:' + pass2
        auth3 = b'test3:' + pass3
        auth4 = b'test4:' + pass4

        # No authentication
        self._assert_code(401)

        # Users 1 and 2 are allowed to STATUS, users 3 and 4 aren't
        self._assert_code(200, bauth=auth1)
        self._assert_code(200, bauth=auth2)
        self._assert_code(403, bauth=auth3)
        self._assert_code(403, bauth=auth4)

        # Users 1 and 3 are allowed to send the DOESNT_EXIST command, user 3 isn't
        # We get 404s here because the command actually doesn't exist, which yields
        # that HTTP code, and also because HTTP authentication happens before
        # command processing
        self._assert_code(404, bauth=auth1, cmd='DOESNT_EXIST')
        self._assert_code(403, bauth=auth2, cmd='DOESNT_EXIST')
        self._assert_code(404, bauth=auth3, cmd='DOESNT_EXIST')

        # Manually shut down the server
        # This is merely so the tearDown() method doesn't kill it wih -9, which
        # in turn means we get no coverage measurement of what we actually tested
        self.termExtSrv(self.extSrvInfo.pop(), auth=auth1)

    def test_unknown_user_password(self):
        """Authorization/authentication is enabled, unknown user/passed is given"""

        # Use a known password value so we can reliably change it afterwards
        suffix = os.urandom(15)
        passwd = b'\x00' + suffix
        passwd_wrong = b'\x01' + suffix
        correct_auth = b'test:' + passwd

        # Test normal usage, wrong user and wrong password
        self.prepExtSrv(cfgProps=self._authorization_cfg([('test', passwd, '*')]))
        self._assert_code(200, bauth=correct_auth)
        self._assert_code(401, bauth=(b'test1:' + passwd))
        self._assert_code(401, bauth=(b'test:' + passwd_wrong))
        self.termExtSrv(self.extSrvInfo.pop(), auth=correct_auth)

    def test_excluded_commands(self):
        """
        Authorization/authentication is enabled, commands are restricted but
        some are excluded
        """

        # Start the server with a set of configured users
        pass1, pass2, pass3, pass4 = [os.urandom(16) for _ in range(4)]
        auth_user_list = ('test1', pass1, '*'),\
                         ('test2', pass2, 'STATUS'), \
                         ('test3', pass3, 'INIT'),\
                         ('test4', pass4, '')
        auth_exclude_commands = "STATUS"
        auth_config_element = self._authorization_cfg(auth_user_list, auth_exclude_commands)
        self.prepExtSrv(cfgProps=auth_config_element)

        auth1 = b'test1:' + pass1
        auth2 = b'test2:' + pass2
        auth3 = b'test3:' + pass3
        auth4 = b'test4:' + pass4

        # No authentication
        self._assert_code(200, cmd="STATUS")
        self._assert_code(401, cmd="INIT")

        self._assert_code(200, bauth=auth1, cmd="STATUS")
        self._assert_code(200, bauth=auth1, cmd="INIT")

        self._assert_code(200, bauth=auth2, cmd="STATUS")
        self._assert_code(403, bauth=auth2, cmd="INIT")

        self._assert_code(200, bauth=auth3, cmd="STATUS")
        self._assert_code(200, bauth=auth3, cmd="INIT")

        self._assert_code(200, bauth=auth4, cmd="STATUS")
        self._assert_code(403, bauth=auth4, cmd="INIT")

        # User 1 is allowed to send the DOESNT_EXIST command
        # We get 404s here because the command actually doesn't exist, which
        # yields that HTTP code, and also because HTTP authentication happens
        # before command processing
        self._assert_code(404, bauth=auth1, cmd='DOES_NOT_EXIST')
        self._assert_code(403, bauth=auth2, cmd='DOES_NOT_EXIST')

        # Manually shut down the server
        # This is merely so the tearDown() method doesn't kill it wih -9, which
        # in turn means we get no coverage measurement of what we actually tested
        self.termExtSrv(self.extSrvInfo.pop(), auth=auth1)
