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

import StringIO
import os, sys, string, random

from ngamsLib import ngamsMIMEMultipart, ngamsLib
from ngamsLib.ngamsCore import rmFile, checkCreatePath
import ngamsTestLib


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

        absDirname = os.path.abspath("toplevel")
        filesInformation, absPaths = ngamsLib.collectFiles(absDirname)
        writer = ngamsLib.ngamsMIMEMultipart.MIMEMultipartWriter(filesInformation)
        messageSize = writer.getTotalSize()

        output = StringIO.StringIO()
        writer.setOutput(output)
        ngamsLib.writeDirContents(writer, absPaths[1], 1024, 0)
        message = output.getvalue()

        self.assertEqual(messageSize, len(message), "Message size calculated by the writer is wrong")
        return message

    def _test_MultipartWriter(self, onlyDirs):

        contents = self._createMIMEMessage(onlyDirs)

        self.assertTrue(contents, "No contents found")
        self.assertTrue(contents.find("MIME-Version: 1.0") != -1, "Content is not a MIME message")
        self.assertTrue(contents.find("Content-Type: multipart/mixed") != -1, "Content is not a multipart message")

        # There should be a different boundaries declaration for each directory
        # since each will result in a multipart message
        nBoundaries = self._findOccurences(contents, 'boundary="')
        self.assertEqual(nBoundaries, len(self.mydirs), "Didn't find all boundary definitions that were expected")

        # There should be a "filename" declaration for each file
        # since each will result in a MIME message inside one of the multiparts
        if not onlyDirs:
            nFilenames = self._findOccurences(contents, 'filename="')
            self.assertEquals(nFilenames, len(self.myfiles), "Didn't find all filename definitions that were expected")

    def _test_MultipartParser(self, onlyDirs):

        message = self._createMIMEMessage(onlyDirs)
        inputContent= StringIO.StringIO(message)
        handler = ngamsMIMEMultipart.ContainerBuilderHandler()
        parser = ngamsMIMEMultipart.MIMEMultipartParser(handler, inputContent, len(message), 1024000)
        parser.parse()

        root = handler.getRoot()

        self.assertTrue(root, "No container found")
        self.assertEquals("toplevel", root.getContainerName())

        nContainers = self._countContainers(root)
        self.assertEquals(nContainers, len(self.mydirs), "Not all containers found in the MIME message")

        if not onlyDirs:
            nFiles = self._countFiles(root)
            self.assertEquals(nFiles, len(self.myfiles), "Not all files found in the MIME message")

    def test_MultipartWriterOnlyDirs(self):
        self._test_MultipartWriter(True)

    def test_MultipartWriterDirsAndFiles(self):
        self._test_MultipartWriter(False)

    def test_MultipartParserOnlyDirs(self):
        self._test_MultipartParser(True)

    def test_MultipartParserDirsAndFiles(self):
        self._test_MultipartParser(False)

    def test_MultipartPrserSeveralReadingSizes(self):
        # The pure fact that the parser ends is good,
        # there's really nothing else to check.
        for size in [2**i for i in xrange(20)]:
            message = self._createMIMEMessage(True)
            inputContent= StringIO.StringIO(message)
            handler = ngamsMIMEMultipart.ContainerBuilderHandler()
            parser = ngamsMIMEMultipart.MIMEMultipartParser(handler, inputContent, len(message), size)
            parser.parse()

def run():
    """
    Run the complete test.

    Returns:   Void.
    """
    ngamsTestLib.runTest(["ngamsMIMEMultipartTest"])

if __name__ == "__main__":
    """
    Main program executing the test cases of the module test.
    """
    ngamsTestLib.runTest(sys.argv)

# EOF