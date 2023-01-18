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
from uuid import uuid4
import os


class TestGenDapi(ngamsTestLib.ngamsTestSuite):
    OCTET_STREAM = 'application/octet-stream'

    def test_archive_and_retrieve_file(self):
        """ test basic archive/retrieve sequence, with a user-defined file ID """

        file_id = str(uuid4())
        self.prepExtSrv()
        self.archive('src/TinyTestFile.fits', pars=(('mime_type', self.OCTET_STREAM),
                                                    ('file_id', file_id),))
        self.retrieve(file_id, targetFile=ngamsTestLib.tmp_path())

    def test_archive_and_compress_file(self):
        """ test compression when archiving a file """

        file_id = str(uuid4())
        self.prepExtSrv()
        self.archive('src/SmallFile.fits', pars=(('compression', 'gzip'),
                                                 ('compression_ext', 'gz'),
                                                 ('mime_type', self.OCTET_STREAM),
                                                 ('target_mime_type', self.OCTET_STREAM),
                                                 ('file_id', file_id)))
        status = self.status(pars=(('file_id', file_id),))
        file_info = status.getDiskStatusList()[0].getFileObjList()[0]
        self.assertEqual(file_id, file_info.getFileId())
        self.assertEqual('SmallFile.fits.gz', os.path.basename(file_info.getFilename()))
        self.assertEqual('gzip', file_info.getCompression())
        self.assertEqual(69120, file_info.getUncompressedFileSize())
        self.assertEqual(self.OCTET_STREAM, file_info.getFormat())

    def test_compression_none(self):
        """ test special case of compression=none, meaning no compression """

        file_id = str(uuid4())
        self.prepExtSrv()
        self.archive('src/SmallFile.fits', pars=(('compression', 'none'),
                                                 ('mime_type', self.OCTET_STREAM),
                                                 ('file_id', file_id)))
        status = self.status(pars=(('file_id', file_id),))
        file_info = status.getDiskStatusList()[0].getFileObjList()[0]
        self.assertEqual(file_id, file_info.getFileId())
        self.assertEqual('SmallFile.fits', os.path.basename(file_info.getFilename()))
        self.assertEqual('NONE', file_info.getCompression())
        self.assertEqual(69120, file_info.getFileSize())
        self.assertEqual(69120, file_info.getUncompressedFileSize())
        self.assertEqual(self.OCTET_STREAM, file_info.getFormat())

    def test_archive_compressed_file(self):
        """
        test passing the uncompressed file size when archiving an already compressed file.
        """

        file_id = str(uuid4())
        self.prepExtSrv()
        self.archive('src/SmallFile.fits.gz', pars=(('compression', 'gzip'),
                                                    ('uncompressed_file_size', 69120),
                                                    ('mime_type', self.OCTET_STREAM),
                                                    ('file_id', file_id)))
        status = self.status(pars=(('file_id', file_id),))
        file_info = status.getDiskStatusList()[0].getFileObjList()[0]
        self.assertEqual(file_id, file_info.getFileId())
        self.assertEqual('SmallFile.fits.gz', os.path.basename(file_info.getFilename()))
        self.assertEqual('gzip', file_info.getCompression())
        self.assertEqual(45823, file_info.getFileSize())
        self.assertEqual(69120, file_info.getUncompressedFileSize())
        self.assertEqual('1379947874', file_info.getChecksum())
        self.assertEqual(self.OCTET_STREAM, file_info.getFormat())
