#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2018
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

from ..ngamsTestLib import ngamsTestSuite, tmp_path

class LarchiveTest(ngamsTestSuite):

    def test_simple(self):

        _, db = self.prepExtSrv()
        copy_of_copy = tmp_path('cp_cp')
        self.cp('/bin/cp', copy_of_copy)
        pars = [['fileUri', copy_of_copy]]
        self.archive('file:' + copy_of_copy, 'application/octet-stream', pars=pars, cmd='LARCHIVE')

        # Check that it exists in the database (duplicated) and that it can be retrieved
        self.assertEqual(2, db.query2('SELECT count(*) FROM ngas_files')[0][0])
        self.assert_ngas_status(self.client.retrieve, 'cp_cp', targetFile=tmp_path())