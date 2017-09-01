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

import sys
import subprocess
import contextlib

from ngamsLib import ngamsHttpUtils
from ngamsTestLib import ngamsTestSuite, runTest
from unittest.case import skipIf

bbcp_cmd = True
cmd = ["which", "bbcp"]
p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
out, err = p.communicate()
if p.returncode == 0:
    bbcp_cmd = False


@skipIf(bbcp_cmd, 'BBCP not found')
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


    def test_correct_checksum(self):

        _, db = self.prepExtSrv()

        query_args = {'filename': '/bin/cp',
                      'bnum_streams': 2,
                      'mime_type': 'application/octet-stream',
                      'crc_variant': '0'}

        # Archive with BBCP first, then with QARCHIVE
        for cmd in ('BBCPARC', 'QARCHIVE'):
            response = ngamsHttpUtils.httpGet('localhost', 8888, cmd,
                                              pars=query_args, timeout=50)
            with contextlib.closing(response):
                self.assertEqual(200, response.status)

        # Both checksums are equal
        checksums = db.query2('SELECT checksum FROM ngas_files ORDER BY file_version ASC');
        checksums = set(c[0] for c in checksums)
        self.assertEqual(1, len(checksums))

    def test_bbcp_no_crc32c(self):

        # Ask for crc32c (variant = 1), bbcp doesn't support it so it should fail
        self.prepExtSrv()
        query_args = {'filename': '/bin/cp',
                      'bnum_streams': 2,
                      'mime_type': 'application/octet-stream',
                      'crc_variant': '1'}
        response = ngamsHttpUtils.httpGet('localhost', 8888, 'BBCPARC',
                                          pars=query_args, timeout=50)
        with contextlib.closing(response):
            self.assertNotEqual(200, response.status)

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

