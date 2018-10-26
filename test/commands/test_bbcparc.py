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
#
"""
Contains the Test Suite for the BBCP Command.
"""

import contextlib
import io
import random
import subprocess
import unittest

import six

from ngamsLib import ngamsHttpUtils
from ngamsServer import ngamsFileUtils
from ..ngamsTestLib import ngamsTestSuite

# If there's any problem getting bbcp's version
# e assume that the program is not there, and therefore skip all the tests
try:
    out = subprocess.check_output(['bbcp', '--version'], shell=False)
    bbcp_version = tuple(map(int, out.strip().split(b'.')))
except:
    bbcp_version = None


@unittest.skipIf(bbcp_version is None, 'BBCP not found')
class ngamsBBCPArchiveTest(ngamsTestSuite):

    def test_BBCPArchive(self):
        """
        Synopsis:
            Test BBCP archive plugin
        """

        self.prepExtSrv()

        query_args = {'filename': '/bin/cp',
                      'bnum_streams': 2,
                      'mime_type': 'application/octet-stream'}

        response = ngamsHttpUtils.httpGet('localhost', 8888, 'BBCPARC',
                                          pars=query_args, timeout=50)
        with contextlib.closing(response):
            self.assertEqual(200, response.status)


    def _test_correct_checksum_with_msb(self, crc_variant, msb):

        _, db = self.prepExtSrv()

        # Generate content until the most significant bit of the checksum is
        # 1 or 0, as required
        content = [b'\0'] * 1024
        while True:
            content[random.randint(0, 1023)] = six.b(chr(random.randint(0, 255)))
            f = io.BytesIO(b''.join(content))
            expected_crc = ngamsFileUtils.get_checksum(64*1024*240, f, crc_variant)
            crc_msb = (expected_crc & 0xffffffff) >> 31
            if crc_msb == 0 and msb == 0:
                break
            elif crc_msb == 1 and msb == 1:
                break

        # This file needs to actually be under /tmp because our BBCP command
        # rejects file pulls from some hardcoded locations (/dev/, /var, etc)
        fname = ('/tmp/dummy')
        with open(fname, 'wb') as f:
            f.write(b''.join(content))

        query_args = {'filename': fname,
                      'bnum_streams': 2,
                      'mime_type': 'application/octet-stream',
                      'crc_variant': crc_variant}

        # Archive with BBCP first, then with QARCHIVE
        for cmd in ('BBCPARC', 'QARCHIVE'):
            response = ngamsHttpUtils.httpGet('localhost', 8888, cmd,
                                              pars=query_args, timeout=50)
            with contextlib.closing(response):
                self.assertEqual(200, response.status)

        # Both checksums are equal (i.e., when put in a set the set has one element)
        checksums = db.query2('SELECT checksum FROM ngas_files ORDER BY file_version ASC');
        checksums = set(c[0] for c in checksums)
        self.assertEqual(1, len(checksums))

        # And they are equal to the expected value
        self.assertEqual(str(expected_crc), str(next(iter(checksums))))

        self.terminateAllServer()

    def _test_correct_checksum(self, crc_variant):
        # We check that BBCPARC works well in both cases when the checksum
        # value has its MSB set to 0 and to 1. This is important, because
        # depending on the checksum variant we are using BBCPARC needs to
        # interpret the checksum value as a signed or unsigned integer
        # (to match the checksum result produced by the NGAS crc variant
        # as supported by NGAS).
        self._test_correct_checksum_with_msb(crc_variant, 0)
        self._test_correct_checksum_with_msb(crc_variant, 1)

    def _test_unsupported_checksum(self, crc_variant):
        # Ask for a variant that bbcp doesn't support, it should fail
        self.prepExtSrv()
        query_args = {'filename': '/bin/cp',
                      'bnum_streams': 2,
                      'mime_type': 'application/octet-stream',
                      'crc_variant': crc_variant}
        response = ngamsHttpUtils.httpGet('localhost', 8888, 'BBCPARC',
                                          pars=query_args, timeout=50)
        with contextlib.closing(response):
            self.assertNotEqual(200, response.status)

    def test_bbcp_with_crc32(self):
        self._test_correct_checksum('crc32')

    def test_bbcp_with_crc32z(self):
        self._test_correct_checksum('crc32z')

    def test_bbcp_with_crc32c(self):

        if bbcp_version[:2] >= (17, 1):
            self._test_correct_checksum('crc32c')
        else:
            self._test_unsupported_checksum('crc32c')