#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
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
import json

from ..ngamsTestLib import ngamsTestSuite


class ngamsQueryCmdTest(ngamsTestSuite):

    def assert_query(self, pars=(), expectedStatus='SUCCESS'):
        return self.get_status("QUERY", pars=pars, expectedStatus=expectedStatus)

    def test_invalid_requests(self):

        self.prepExtSrv()

        # No query
        self.assert_query(expectedStatus='FAILURE')

        # Invalid queries
        self.assert_query(pars=(('query', 'file_list'),), expectedStatus='FAILURE')
        self.assert_query(pars=(('query', 'file_listss'),), expectedStatus='FAILURE')
        self.assert_query(pars=(('query', ''),), expectedStatus='FAILURE')

    def test_files_list(self):

        self.prepExtSrv()

        # No files archived, there was an error on the previous implementation
        self.assert_query(pars=[['query', 'files_list'], ['format', 'list']])

        # One file archived, let's see what happens now
        self.archive("src/SmallFile.fits")
        self.assert_query(pars=[['query', 'files_list'], ['format', 'list']])

        # Make sure that the other formats work as well
        self.assert_query(pars=[['query', 'files_list'], ['format', 'text']])
        self.assert_query(pars=[['query', 'files_list'], ['format', 'json']])
        stat = self.assert_query(pars=[['query', 'files_list']])

        # Check that the archived file is listed
        data = stat.getData()
        self.assertTrue(b"TEST.2001-05-08T15:25:00.123" in data)

        # Try again getting with format=list. There was a bug previously with
        # python 3 that prevents results from showing up
        stat = self.assert_query(pars=[['query', 'files_list'], ['format', 'list']])
        self.assertTrue(b"TEST.2001-05-08T15:25:00.123" in stat.getData())

    def test_column_names(self):
        """Check that column names are correctly bound to data by reading some
        of the cells and making sure they make sense. If column names are not
        correclty bound to columns then data might not be convertible and might
        not make sense"""

        cfg, _ = self.prepExtSrv()

        stat = self.assert_query(pars=[['query', 'disks_list'], ['format', 'json']])
        results = json.loads(stat.getData())
        self.assertEqual(0, int(results[0]['number_of_files']))
        self.assertEqual(cfg.getArchiveName(), results[0]['archive'])