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
from .ngamsTestLib import sendPclCmd, ngamsTestSuite


class ngamsQueryCmdTest(ngamsTestSuite):

    def test_invalid_requests(self):

        self.prepExtSrv()
        client = sendPclCmd()

        # No query
        self.assertStatus(client.get_status('QUERY'), 'FAILURE')

        # Invalid queries
        self.assertStatus(client.get_status('QUERY', pars=(('query', 'file_list'),)), 'FAILURE')
        self.assertStatus(client.get_status('QUERY', pars=(('query', 'file_listss'),)), 'FAILURE')
        self.assertStatus(client.get_status('QUERY', pars=(('query', ''),)), 'FAILURE')

    def test_files_list(self):

        self.prepExtSrv()
        client = sendPclCmd()

        # No files archived, there was an error on the previous implementation
        stat = client.get_status("QUERY", pars = [['query', 'files_list'], ['format', 'list']])
        self.assertStatus(stat)

        # One file archived, let's see what happens now
        stat = client.archive("src/SmallFile.fits")
        self.assertStatus(stat)
        stat = client.get_status("QUERY", pars = [['query', 'files_list'], ['format', 'list']])
        self.assertStatus(stat)

        # Make sure that the other formats work as well
        stat = client.get_status("QUERY", pars = [['query', 'files_list'], ['format', 'text']])
        self.assertStatus(stat)
        stat = client.get_status("QUERY", pars = [['query', 'files_list'], ['format', 'json']])
        self.assertStatus(stat)
        stat = client.get_status("QUERY", pars = [['query', 'files_list']])
        self.assertStatus(stat)

        # Check that the archived file is listed
        data = stat.getData()
        self.assertTrue("TEST.2001-05-08T15:25:00.123" in data)