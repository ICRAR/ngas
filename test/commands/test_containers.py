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

import os
import random
import string

from ngamsLib.ngamsCore import NGAMS_SUCCESS, NGAMS_FAILURE, toiso8601
from ngamsLib.ngamsCore import rmFile, checkCreatePath, getFileSize
from ..ngamsTestLib import ngamsTestSuite, tmp_path


class ngamsContainerTest(ngamsTestSuite):

    mydirs = [
        "toplevel",
        "toplevel/1",
        "toplevel/2",
        "toplevel/3",
        "toplevel/3/subdir",
        "toplevel/3/subdir/anotherSubdir"
    ]

    # Mind you that they all have different basenames
    myfiles = [
        "toplevel/file1",
        "toplevel/file2",
        "toplevel/1/musicFile",
        "toplevel/2/fitsFile.fits",
        "toplevel/3/subdir/apple",
        "toplevel/3/subdir/anotherSubdir/orange"
    ]

    def setUp(self):
        ngamsTestSuite.setUp(self)
        self._createDirectories()
        self._createFiles()

    def tearDown(self):
        ngamsTestSuite.tearDown(self)
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

    def _filesSize(self):
        return sum([getFileSize(f) for f in self.myfiles])

    def test_CreateDestroy(self):

        # Server and client
        self.prepExtSrv()

        #------------------------------------------------------------------
        # We start testing with a single container creation/deletion,
        # which is simpler
        #------------------------------------------------------------------
        containerName = "testing"

        # Create a container, shouldn't be a problem
        status = self.ccreate(containerName)
        self.assertEqual(1, len(status.getContainerList()))
        self.assertEqual(containerName, status.getContainerList()[0].getContainerName())

        # Destroy it, shouldn't be a problem
        self.cdestroy(containerName)

        #------------------------------------------------------------------
        # We now do a container hierarchy creation/deletion.
        #------------------------------------------------------------------

        # Create a simple container first
        status = self.ccreate(containerName)
        self.assertEqual(1, len(status.getContainerList()))
        self.assertEqual(containerName, status.getContainerList()[0].getContainerName())

        # Now create a hierarchy. When we succeed, let's collect the IDs
        # of the root containers. The parentId is an attribute of the main
        # Container element, and thus we have a fixed template that then fill
        # with the correct attribute value
        containerId = status.getContainerList()[0].getContainerId()
        containerHierarchyTpl = '<Container name="1"%s><Container name="1.1"></Container>' +\
                             '<Container name="1.2"><Container name="1.2.1"></Container>' + \
                             '</Container></Container>'
        rootContainerIds = []
        for parentId, expectedStatus in [("DOESNT EXIST", NGAMS_FAILURE),
                                         (containerId, NGAMS_SUCCESS),
                                         (None, NGAMS_SUCCESS)]:

            parentId = ' parentContainerId="' + parentId + '"' if parentId else ""
            containerHierarchy = containerHierarchyTpl % (parentId)

            status = self.ccreate(None, containerHierarchy=containerHierarchy, expectedStatus=expectedStatus)
            if expectedStatus == NGAMS_SUCCESS:
                rootContainer = status.getContainerList()[0]
                rootContainerIds.append(rootContainer.getContainerId())
                self.assertEqual("1", rootContainer.getContainerName())
                self.assertEqual(2, len(rootContainer.getContainers()))
                self.assertEqual(0, len(rootContainer.getFilesInfo()))

        # Let's try to delete one of the "1" root containers, it should fail
        # because there is more than one container with that name, and therefore
        # they are not uniquely addressable by their name
        self.cdestroy_fail("1")

        # Let's try by ID, but without recursion, it should fail also
        self.cdestroy_fail(None, containerId=rootContainerIds[0])

        # Now it should really work
        for containerId in rootContainerIds:
            self.cdestroy(None, containerId=containerId, recursive=True)

    def test_AppendRemove(self):

        # Server and client
        self.prepExtSrv()

        # Create a container, shouldn't be a problem
        containerName = "testing"
        self.ccreate(containerName)
        self._checkContainerClosed(containerName, False)

        #------------------------------------------------------------------
        # We start testing with a single file append/removal,
        # which is a simple use case
        #------------------------------------------------------------------

        # Archive a single file, append it to the container
        # and check the file is there and it contributes to the container size
        myfile = self.myfiles[0]
        myfileId = os.path.basename(myfile)
        myfileSize = getFileSize(myfile)
        self.qarchive(myfile, "application/octet-stream")
        self.cappend(myfileId, containerName=containerName)
        self._checkFilesAndContainerSize(containerName, 1, myfileSize)
        self._checkContainerClosed(containerName, False)

        # Remove the file now and check that the file is not there anymore,
        # decreasing the container size
        self.cremove(myfileId, containerName=containerName)
        self._checkFilesAndContainerSize(containerName, 0, 0)
        self._checkContainerClosed(containerName, False)

        #------------------------------------------------------------------
        # We continue testing with a list of files for append/removal,
        # which is a bit more complex
        #------------------------------------------------------------------

        # Store the rest of the files
        # Append all files to the container (including the first one, we removed it)
        # and check that files are there, and that they contribute to the container size
        fileIds = ':'.join([os.path.basename(f) for f in self.myfiles])
        allFilesSize = self._filesSize()
        for myfile in self.myfiles[1:]:
            self.qarchive(myfile, "application/octet-stream")
        self.cappend(None, fileIdList=fileIds, containerName=containerName)
        self._checkFilesAndContainerSize(containerName, len(self.myfiles), allFilesSize)
        self._checkContainerClosed(containerName, False)

        # Remove the files now and check that the files are not part of the container anymore,
        # decreasing the container size
        self.cremove(None, fileIdList=fileIds, containerName=containerName)
        self._checkFilesAndContainerSize(containerName, 0, 0)
        self._checkContainerClosed(containerName, False)


        #------------------------------------------------------------------
        # Now we re-archive the same files, effectively creating a second
        # version of them. We then check which version is considered as
        # part of the container.
        #
        # This test comes in two flavors:
        # - Add the files to the container, re-archive them
        # - Re-archive the files, then add them to the container
        #------------------------------------------------------------------

        # Add the files to the container and then re-archive them
        # The new version of the files should be 2, and the new container size
        # should correspond to the new files' sizes
        self._createFiles()
        allFilesSize = self._filesSize()
        self.cappend(None, fileIdList=fileIds, containerName=containerName)
        for myfile in self.myfiles:
            self.qarchive(myfile, "application/octet-stream")
        self._checkFilesAndContainerSize(containerName, len(self.myfiles), allFilesSize, fileVersion=2)
        self._checkContainerClosed(containerName, False)

        # Remove all files and check that the container is empty
        self.cremove(None, fileIdList=fileIds, containerName=containerName)
        self._checkFilesAndContainerSize(containerName, 0, 0)
        self._checkContainerClosed(containerName, False)

        # Re-archive the files, add them to the container and check again
        # The new version of the files should be 3, and the new container size
        # should correspond to the new files' sizes
        self._createFiles()
        allFilesSize = self._filesSize()
        for myfile in self.myfiles:
            self.qarchive(myfile, "application/octet-stream")
        self.cappend(None, fileIdList=fileIds, containerName=containerName)
        self._checkFilesAndContainerSize(containerName, len(self.myfiles), allFilesSize, fileVersion=3)
        self._checkContainerClosed(containerName, False)

        # Remove all files and check that the container is empty
        self.cremove(None, fileIdList=fileIds, containerName=containerName)
        self._checkFilesAndContainerSize(containerName, 0, 0)
        self._checkContainerClosed(containerName, False)


        #------------------------------------------------------------------
        # The final test in this section is about "closing" a container
        # When a container is created via CCREATE it remains "opened",
        # until a CAPPEND command specifies that it wants to "close" the
        # container. A container is closed when its ingestion date is set.
        #------------------------------------------------------------------

        # First of all, check that the container is still currently opened
        self._checkContainerClosed(containerName, False)

        # Append a file, check that the container is still opened
        myfileId = os.path.basename(self.myfiles[0])
        self.cappend(myfileId, containerName=containerName)
        self._checkContainerClosed(containerName, False)

        # Append a second file and mark the container as closed
        myfileId = os.path.basename(self.myfiles[1])
        self.cappend(myfileId, containerName=containerName, closeContainer=True)
        self._checkContainerClosed(containerName, True)


    def _checkFilesAndContainerSize(self, containerName, nFiles, filesSizeInDisk, fileVersion=1):
        status = self.clist(containerName)
        self.assertEqual(1, len(status.getContainerList()))
        container = status.getContainerList()[0]
        self.assertEqual(containerName, container.getContainerName())

        # Flatten all containers into a nice list
        def collConts(c, l):
            l.append(c)
            for cc in c.getContainers():
                collConts(cc, l)
        containers = []
        collConts(container, containers)

        # On each container the container size is the total of its files
        # and each file has the expected version
        totalContFiles = 0
        totalContSize = 0
        for cont in containers:
            filesSize = sum([int(f.getUncompressedFileSize())  for f in cont.getFilesInfo()])
            self.assertEqual(filesSize, cont.getContainerSize())
            totalContSize += filesSize

            files = cont.getFilesInfo()
            totalContFiles += len(files)
            for f in files:
                self.assertEqual(fileVersion, f.getFileVersion())

        # Totals
        self.assertEqual(nFiles, totalContFiles)
        self.assertEqual(filesSizeInDisk, totalContSize)

    def _checkContainerClosed(self, containerName, isClosed):
        status = self.clist(containerName)
        self.assertEqual(1, len(status.getContainerList()))
        container = status.getContainerList()[0]
        self.assertEqual(containerName, container.getContainerName())

        # Containers are closed when they have an ingestion date
        self.assertEqual(isClosed, container.isClosed(), "Container's ingestion date is: '" + toiso8601(container.getIngestionDate()) + "'; expected isClosed=" + str(isClosed))

    def _test_archive_receive(self, as_tar):

        # Server and client
        self.prepExtSrv()
        containerName = "toplevel"

        # Archive the top-level directory
        self.carchive(containerName, 'application/octet-stream')
        self._checkFilesAndContainerSize(containerName, len(self.myfiles), self._filesSize(), 1)

        # Retrieve it
        self.cretrieve(containerName, targetDir=tmp_path(), as_tar=as_tar)
        self._assertEqualsDir(containerName, tmp_path(containerName))

    def test_archive_receive(self):
        self._test_archive_receive(False)

    def test_archive_receive_tar(self):
        self._test_archive_receive(True)

    def _test_archive_retrieve_in_cluster(self, as_tar):

        self.prepCluster((8888, 8889))
        container_name = 'toplevel'
        tgt_root = tmp_path(container_name)

        # Create our own new "root" to easily use self._assertEqualsDir later
        src_root = tmp_path('src', container_name)
        checkCreatePath(src_root)
        cp = lambda x: self.cp(x, os.path.join(src_root, os.path.basename(x)))
        cp(self.myfiles[0])
        cp(self.myfiles[1])

        # Create a container and archive two files into it with our clients
        def archive_into_container(port, fname):
            self.archive(port, fname, "application/octet-stream")
            self.cappend(port, os.path.basename(fname), containerName=container_name)

        self.ccreate(8888, container_name)
        archive_into_container(8888, self.myfiles[0])
        archive_into_container(8889, self.myfiles[1])

        # CRETRIEVE the container using both clients in turns,
        # both files should come back
        def retrieve_container(port):
            self.cretrieve(port, container_name, targetDir=tmp_path(), as_tar=as_tar)
            self._assertEqualsDir(src_root, tgt_root)
            rmFile(tgt_root)

        retrieve_container(8888)
        retrieve_container(8889)

    def test_archive_retrieve_in_cluster(self):
        self._test_archive_retrieve_in_cluster(False)

    def test_archive_retrieve_in_cluster_tar(self):
        self._test_archive_retrieve_in_cluster(True)

    def _assertEqualsDir(self, dir1, dir2):

        # Entries in dir are the same
        files1 = os.listdir(dir1)
        files2 = os.listdir(dir2)
        files1.sort()
        files2.sort()
        self.assertListEqual(files1, files2)

        # Add dir names so we can treat them as proper paths
        addDir = lambda d, f: d + os.sep + f
        files1 = list(map(addDir, [dir1 for _ in range(len(files1))], files1))
        files2 = list(map(addDir, [dir2 for _ in range(len(files2))], files2))

        # Files actually have the same size, we recurse on dirs
        for idx, f in [(idx, f) for idx, f in enumerate(files1)]:
            if os.path.isfile(f):
                self.assertEqual(getFileSize(f), getFileSize(files2[idx]))
            elif os.path.isdir(f):
                self._assertEqualsDir(f, files2[idx])