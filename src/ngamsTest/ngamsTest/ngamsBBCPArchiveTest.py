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
import subprocess
import sys
import unittest

from ngamsLib import ngamsHttpUtils
from ngamsTestLib import ngamsTestSuite, runTest
from ngamsServer import ngamsFileUtils

# If there's any problem getting bbcp's version
# e assume that the program is not there, and therefore skip all the tests
try:
    out = subprocess.check_output(['bbcp', '--version'], shell=False)
    bbcp_version = map(int, out.strip().split('.'))
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


    def _test_correct_checksum(self, crc_variant):

        _, db = self.prepExtSrv()

        fname = '/bin/cp'
        expected_crc = ngamsFileUtils.get_checksum(64*1024*240, fname, crc_variant)

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

    def _test_unsupported_checksum(self, crc_variant):
                # Ask for crc32c (variant = 1), bbcp doesn't support it so it should fail
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

    def test_bbcp_with_crc32c(self):

        if tuple(bbcp_version[:2]) >= (17, 1):
            self._test_correct_checksum('crc32c')
        else:
            self._test_unsupported_checksum('crc32c')

def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    runTest(["ngamsBBCPArchiveTest"])


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    runTest(sys.argv)

