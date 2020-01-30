#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2020
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
from test import ngamsTestLib

class TestFitsDapi(ngamsTestLib.ngamsTestSuite):

    def test_arcfile_setting(self):
        '''Checks whether we can safely bypass checking the ARCFILE keyword'''
        self.prepExtSrv()
        # No arcfile skipping, file ID will be that coming from the FITS file
        self.archive('src/SmallFile.fits')
        self.retrieve('TEST.2001-05-08T15:25:00.123', targetFile=ngamsTestLib.tmp_path())
        # arcfile skipping, file ID will be the filename given at archiving time
        self.archive('src/SmallFile.fits', pars=(('ignore_arcfile', 1),))
        self.retrieve('SmallFile.fits', targetFile=ngamsTestLib.tmp_path())

    def test_no_compression(self):
        '''Checks whether we FITS files can be archived with no compression'''

        # Check that archiving works, first of all
        for fits_conf in ('compression=not understood', ''):
            self.terminateAllServer()
            # Stream[2] is the stream for mime-type=image/x-fits
            conf = ('NgamsCfg.Streams[1].Stream[2].PlugInPars', fits_conf),
            _, db = self.prepExtSrv(cfgProps=conf)
            self.archive('src/SmallFile.fits')

        # Check that uncompressed files are declared so in the DB, and that
        # their file_size columns match
        sql = """
        SELECT
            compression, uncompressed_file_size, file_size
        FROM
            ngas_files
        WHERE
            file_id = 'TEST.2001-05-08T15:25:00.123'
        """
        for result in db.query2(sql):
            self.assertEqual('', result[0])
            self.assertEqual(result[1], result[2])