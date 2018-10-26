#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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

import functools
import io
import os
import random
import string
import tempfile

from ngamsLib import ngamsMIMEMultipart
from ngamsLib.ngamsCore import rmFile, checkCreatePath
from . import ngamsTestLib


class ngamsMIMEMultipartTest(ngamsTestLib.ngamsTestSuite):

    mydirs = [
        "toplevel",
        "toplevel/1",
        "toplevel/2",
        "toplevel/3",
        "toplevel/3/subdir",
        "toplevel/3/subdir/anotherSubdir"
    ]

    myfiles = [
        "toplevel/file1",
        "toplevel/file2",
        "toplevel/1/musicFile",
        "toplevel/2/fitsFile.fits",
        "toplevel/3/subdir/apple",
        "toplevel/3/subdir/anotherSubdir/orange"
    ]

    def tearDown(self):
        if os.path.isdir("toplevel"):
            rmFile("toplevel")

    def _createDirectories(self):
        for mydir in self.mydirs:
            checkCreatePath(mydir)

    def _createFiles(self):
        for myfile in self.myfiles:
            sysRandom = random.SystemRandom()
            with open(myfile, "w") as f:
                n = sysRandom.randint(10, 100)
                content = ''.join(sysRandom.choice(string.printable) for _ in range(n))
                f.write(content)

    def _countContainers(self, container):
        count = 1
        for c in container.getContainers():
            count += self._countContainers(c)
        return count

    def _countFiles(self, container):
        count = len(container.getFilesInfo())
        for c in container.getContainers():
            count += self._countFiles(c)
        return count

    def _findOccurences(self, string, substring):
        occurences = 0
        idx = 0
        while True:
            idx=string.find(substring, idx)
            if idx == -1:
                break
            else:
                idx += len(substring)
                occurences += 1
        return occurences

    def _createMIMEMessage(self, onlyDirs):
        self._createDirectories()
        if not onlyDirs:
            self._createFiles()

        cinfo = ngamsMIMEMultipart.cinfo_from_filesystem('toplevel', 'application/octet-stream')
        bs = 65536
        output = io.BytesIO()
        reader = ngamsMIMEMultipart.ContainerReader(cinfo)
        rfunc = functools.partial(reader.read, bs)
        for buf in iter(rfunc, b''):
            output.write(buf)
        message = output.getvalue()

        self.assertEqual(len(reader), len(message), "Message size calculated by the reader is wrong: %d != %d" % (len(reader), len(message)))
        return message

    def _test_MultipartWriter(self, onlyDirs):

        contents = self._createMIMEMessage(onlyDirs)

        self.assertTrue(contents, "No contents found")
        self.assertTrue(b"MIME-Version: 1.0" in contents, "Content is not a MIME message")
        self.assertTrue(b"Content-Type: multipart/mixed" in contents, "Content is not a multipart message")

        # There should be a different boundaries declaration for each directory
        # since each will result in a multipart message
        nBoundaries = self._findOccurences(contents, b'boundary="')
        self.assertEqual(nBoundaries, len(self.mydirs), "Didn't find all boundary definitions that were expected")

        # There should be a "filename" declaration for each file
        # since each will result in a MIME message inside one of the multiparts
        if not onlyDirs:
            nFilenames = self._findOccurences(contents, b'filename="')
            self.assertEqual(nFilenames, len(self.myfiles), "Didn't find all filename definitions that were expected")

    def _test_MultipartParser(self, onlyDirs):

        message = self._createMIMEMessage(onlyDirs)
        inputContent= io.BytesIO(message)
        handler = ngamsMIMEMultipart.ContainerBuilderHandler()
        parser = ngamsMIMEMultipart.MIMEMultipartParser(handler, inputContent, len(message), 1024000)
        parser.parse()

        root = handler.getRoot()

        self.assertTrue(root, "No container found")
        self.assertEqual("toplevel", root.getContainerName())

        nContainers = self._countContainers(root)
        self.assertEqual(nContainers, len(self.mydirs), "Not all containers found in the MIME message")

        if not onlyDirs:
            nFiles = self._countFiles(root)
            self.assertEqual(nFiles, len(self.myfiles), "Not all files found in the MIME message")

    def test_MultipartWriterOnlyDirs(self):
        self._test_MultipartWriter(True)

    def test_MultipartWriterDirsAndFiles(self):
        self._test_MultipartWriter(False)

    def test_MultipartParserOnlyDirs(self):
        self._test_MultipartParser(True)

    def test_MultipartParserDirsAndFiles(self):
        self._test_MultipartParser(False)

    def test_MultipartParserSeveralReadingSizes(self):
        # The pure fact that the parser ends is good,
        # there's really nothing else to check.
        for size in [2**i for i in range(20)]:
            message = self._createMIMEMessage(True)
            inputContent= io.BytesIO(message)
            handler = ngamsMIMEMultipart.ContainerBuilderHandler()
            parser = ngamsMIMEMultipart.MIMEMultipartParser(handler, inputContent, len(message), size)
            parser.parse()

    def test_FileInfoReader(self):

        size = random.randint(10, 100)
        fd, name = tempfile.mkstemp('.bin', dir=ngamsTestLib.tmp_root)
        with os.fdopen(fd, 'wb') as f:
            f.write(b' ' * size)

        finfo = ngamsMIMEMultipart.file_info('application/octet-stream', name, size, lambda: open(name, 'rb'))
        reader = ngamsMIMEMultipart.FileReader(finfo)
        rlen = len(reader)

        message = io.BytesIO()
        rfunc = functools.partial(reader.read, 65536)
        for buf in iter(rfunc, b''):
            message.write(buf)
        message = message.getvalue()
        mlen = len(message)

        self.assertEqual(mlen, rlen)