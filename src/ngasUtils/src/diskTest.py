#!/usr/bin/env python
#
#
#    (c) University of Western Australia
#    International Centre of Radio Astronomy Research
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
#    Copyright by UWA,
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

import time, pickle
import commands
import sys, os, socket, traceback, datetime, mmap, threading
import base64, glob
from multiprocessing import Pool, cpu_count


#    "d:s:t:i:b:z:c:e:r:f:l:womph",\
#           ["device","skip","testcount","iosize","blocksize", "sndbufsize",\
#            "crc", "lowio", "session", "datarate", "file",\
#            "write","old","method","parallel","help"])

DEFAULT_FNM = 'bspeed.pkl'


def usage():
    """
    This code contains methods to perform performance tests of
    block devices, e.g. hard disks. It also allows using files rather
    than devices directly.

    Synopsis: diskTest.py [-d device] [-s skip] [-t testcount] [-i iosize]
                          [-b blocksize] [-c {b|z|c}] [-f file] [-m] [-w] [-l] [-o]
                          [-p] [-h]

                  long arguments are allowed as well, e.g. --device

          [d]evice:    string, e.g. /dev/hdb1, can also be a file or
                       a url (for http disk write). If the first four characters
                       are /dev a number of additional checks are performed before
                       writing to the raw device.
          [s]kip:      integer [0 GB], e.g. 5
          [t]estcount: integer [1], number of consecutive tests [1]
          [i]osize:    integer [1073741824 bytes == 1 GB], full size of
                       one test, i.e. iosize/blocksize IOs will be
                       carried out before calculating statistics.
          [b]locksize: integer [1024 bytes], number of bytes in single
                       IO
          sndbufsi[z]e:the TCP send buffer size. This parameter
                       is used only when the device is a URL (HTTP
                       write test)
          [c]rc:       string, if 'b' binascii is used, if 'z' zlib, if 'c'
                       crc32c is used.
          [m]ethod:    flag, if set a python implementation of dd
                       will be used.
          [l]owio:     [direct, async], this will switch to lower level I/O
                       using either the O_DIRECT or the O_ASYNC flag when opening
                       the file.
          s[e]ssion:   string, session id for this HTTP write test
          data[r]ate:  the data rate for HTTP write test. This parameter
                       is used only when the device is a URL (HTTP
                       write test)
          [f]file:     string, filename of the file containing the results.
                       Default: bspeed.pkl
          [w]rite:     flag, if set writeTest is performed.
          [o]ld:       flag, if set the old implementation of python DD is used.
          [m]ethod:    flag, if set a python implementation of dd will be used.
                       Default: False, standard dd will be used.
          [p]arallel:  flag, if dev=='http', this will send <testcount> streams
                       in parallel.
          [h]elp:      flag, if set this help text is shown.

          NOTE: All byte values are forced to be an integer multiple
                 of 4.

          Typical usage:
          python ~/diskTest.py -d /mymnt/testio -b 262144 -w -m -c z -t 5 -l async

          This performs 5 consecutive write tests on files /mymnt/testio* using
          a 256kB block size, the internal Python implementation of dd,
          performs a CRC checksum calculation on the stream using the zlib
          based CRC algorithm, repeats the test 5 times and uses low-level
          I/O.

          Plotting: The myDD write test is producing a fairly concise timing
          profile of the whole I/O and processing performance. The result will
          be stored in a file called bspeed.pkl. Since usually the platform where
          the tests are executed are servers or machines without window servers
          the plotting functionality is split off into a stand-alone function.
          Typically you would need to copy the bspeed.pkl file to a desktop
          machine. There is an additional dependency on the python pylab module
          to do the actual plotting. The best way to do this is to change to the
          directory where the bspeed.pkl file is located and launch

          ipython --pylab
          >>> import diskTest
          >>> speedPlot()

    Author:  A. Wicenec [ESO, ICRAR]
    Date:    29-May-2002
    Version 1.0
    Version 2.0    20-Jun-2002: writeTest included
    Version 3.1    16-Aug-2013: various additions, CRC and stats writing.
                   also added plotting function.
    """
    import pydoc
    print pydoc.help('diskTest.usage')
    sys.exit()


class diskTest():
    def __init__(self):

        self.Test = 'read'           # default test is readTest
        self.skip = 0        # default skip [GB]
        self.testcount = 1        # default number of consecutive tests
        self.iosize = 1073741824l    # default size of one test: 1 GB
        self.blocksize = 1024    # default size of IO blocks
        self.tcpsndbuf = None    # default tcp send buffer size
        self.dev = None              # no default for the actual device
        self.method = 'myDD'          # default method for performing the tests
        self.pattern = 'abcd'        # default pattern to be used for writeTest
        self.bspeed = None
        self.old = 0
        self.crcfl = ''              # default is no crc
        self.llflag = 0              # default use normal I/O
        self.asyncflag = 0
        self.syncflag = 0
        self.dioflag = 0
        self.session_id = None       # For write to HTTP only, default is None
        self.data_rate = None  # default there is no data rate limit for HTTP
        self.parallel_stream = False
        self.writeStat = False

        self.NGAMS_HTTP_SUCCESS = 200
        self.NGAMS_HTTP_POST = 'POST'
        self.one_mb = 1024. ** 2
        self.DEFAULT_FNM = 'bspeed.pkl'

    def readTest(self):
        """
        This is actually running the read test.

        Input:
             dev:            String, device name
             skip:           integer, number of GB to be skipped
             testcount:      integer, number of tests
             iosize:         integer, number of bytes/test
             blocksize:      integer, number of bytes/write
        """

        self.blocksize = long(self.blocksize)/4 * 4   # blocksize multiple of 4
        self.iosize = long(self.iosize)/4 * 4   # iosize multiple of 4
        if os.path.isfile(self.dev) and self.iosize > os.path.getsize(self.dev):
            self.iosize = os.path.getsize(self.dev)/4 * 4
        self.iocount = self.iosize/self.blocksize

        for ii in range(self.testcount):
            st = time.time()
            if self.method == 'dd':
                command = 'dd if=%s of=/dev/null skip=%d bs=%d count=%d' \
                      % (self.dev, self.skip, self.blocksize, self.iocount)
                print command
                st = time.time()
                (status, output) = commands.getstatusoutput(command)
            else:
                st = time.time()
                flags = (self.syncflag, self.asyncflag, self.dioflag,
                         self.llflag, self.crcfl)
                print "myDD("+self.dev+", None, '/dev/null',"+str(self.skip) +\
                      "," + str(self.blocksize)+","+str(self.iocount) + \
                      "," + str(flags) + ")"
                status = myDD(self.dev, None, '/dev/null',
                              long(self.skip)*self.blocksize,
                              self.blocksize, self.iocount, flags=flags)

            if status == 256:
                print "Permission denied"
                print "Probably you don't have read access to "\
                      "device "+self.dev
                print "Bailing out!"
                sys.exit()
            else:
                elapsed = time.time()-st
                print 'Throughput (read): %3.2f MB/s (%5.2f s)' % \
                      (self.iosize/elapsed/1024./1024., elapsed)

            self.skip = self.skip + self.iocount
            return status

    def writeTestHTTP(self, dev, skip, testcount, iosize, blocksize,
                      sessionId=None, sndbufsize=None, parallel=False):
        """
        This is actually running the HTTP-based remote write test.
        It will read blocks of zeroes from /dev/zero and
        and send these blocks to the server as a single file

        Input:
             dev:            String, url to which data is sent
             sessionId:      String, this will be used to form the prefix of
                             the file name: sessionId_nodeId_count.dat
             testcount:      integer, number of tests
             iosize:         integer, number of bytes/test
             blocksize:      integer, number of bytes/write

        """
        self.blocksize = long(self.blocksize)/4 * 4   # blocksize multiple of 4
        self.iosize = long(self.iosize)/4 * 4   # blocksize multiple of 4
        self.iocount = self.iosize/self.blocksize
        bspeed = []
        cspeed = []
        tspeed = []
        nodeId = socket.gethostname()

        locTimeout = 3600
        mimeType = 'application/octet-stream'
        user = 'ngasmgr'
        pwd = 'ngas$dba'  # this should be passed in
        authHdrVal = "Basic " + base64.encodestring(user + ":" + pwd)
        if (authHdrVal[-1] == "\n"):
            authHdrVal = authHdrVal[:-1]

        if (not sessionId):     # if no sessionId, file names from different
                                # nodes will have different prefix
            dt = datetime.datetime.now()
            sessionId = dt.strftime('%Y%m%dT%H%M%S')

        import httplib

        if (parallel):
            tst = time.time()
            myDDThreads = []

        myblock = str(bytearray(os.urandom(self.blocksize)))
        for ii in range(self.testcount):
            st = time.time()
            fname = '%s_%s_%s.dat' % (sessionId, nodeId, str(ii))
            contDisp = "attachment; filename=\"%s\"; no_versioning=1" % fname
            # make HTTP headers
            url = self.dev
            idx = (url[7:].find("/") + 7)  # Separate the URL from the command.
            tmpUrl = url[7:idx]
            cmd = url[(idx + 1):]

            http = httplib.HTTP(tmpUrl)
            try:
                # print "Sending HTTP header ..."
                http.putrequest(self.NGAMS_HTTP_POST, cmd)
                http.putheader("Content-Type", mimeType)
                http.putheader("Content-Disposition", contDisp)
                http.putheader("Content-Length", str(self.iosize))
                http.putheader("x-ddn-policy", "replica-store")
                print "Content-Length = %s" % str(self.iosize)
                http.putheader("Authorization", authHdrVal)
                http.putheader("Host", nodeId)
                http.putheader("NGAS-File-CRC", "1533330096")
                http.endheaders()
                # send payload
                http._conn.sock.settimeout(locTimeout)
                if (sndbufsize):
                    try:
                        http._conn.sock.setsockopt(socket.SOL_SOCKET,
                                                   socket.SO_SNDBUF,
                                                   sndbufsize)
                        print("Set TCP SNDBUF to %d" % sndbufsize)
                    except Exception, eer:
                        print('Fail to set TCP SNDBUF to %d: %s' %
                              (sndbufsize, str(eer)))

                if (not parallel):
                    st = time.time()
                    status = myDD(block=myblock,
                                       skip=long(skip)*self.blocksize,
                                       httpobj=http)
                else:
                    kwargs = {'block': myblock,
                              'skip': long(self.skip)*self.blocksize,
                              'seek': 0, 'httpobj': http}
                    thrdName = 'myDDThrd_%d' % ii
                    ddThrRef = threading.Thread(None, myDD, thrdName,
                                                kwargs=kwargs)
                    ddThrRef.setDaemon(0)
                    ddThrRef.start()
                    myDDThreads.append(ddThrRef)

            except Exception, e:
                ex = str(e) + traceback.format_exc()
                print ex
                raise e
            finally:
                if (not parallel):
                    if (http):
                        try:
                            http.close()  # this may fail?
                        finally:
                            del http
            if (not parallel):
                elapsed = time.time()-st
                print 'Throughput (elapsed time during test): %3.2f MB/s ' + \
                      '(%5.2f s)\n' % (self.iosize/self.elapsed/1024./1024.,
                                       self.elapsed)
                bspeed += status[0]
                cspeed += status[1]
                tspeed += status[2]

        if (parallel):
            for dtr in myDDThreads:
                dtr.join()
            telapsed = time.time() - tst
            print 'Test throughput (elapsed time during test): ' +\
                '%3.2f MB/s (%5.2f s)\n' %\
                (self.iosize * self.testcount/telapsed/1024./1024., telapsed)

        return (bspeed, cspeed, tspeed)

    def parallelWriteTestDD(self):
        """
        This is actually running the parallel write test on multiple processes

        Input:
             dev:            String, device name
             skip:           integer, number of GB to be skipped
             testcount:      integer, number of tests
             iosize:         integer, number of bytes/test
             blocksize:      integer, number of bytes/write
        """
        blocksize = long(self.blocksize)/4 * 4   # blocksize multiple of 4
        iosize = long(self.iosize)/4 * 4   # blocksize multiple of 4
        iocount = iosize/blocksize
        bspeed = []
        cspeed = []
        tspeed = []
        myblock = str(bytearray(os.urandom(blocksize)))
        tst = time.time()
        # limit numer of parallel processes to number of CPUs
        if self.testcount > cpu_count():
            testcount = cpu_count()
        pool = Pool(processes=testcount)
        tres = []
        tstart = []
        print "Executing parallel write test with %d processes.\n" % testcount
        for ii in range(testcount):
            flags = (self.syncflag, self.asyncflag, self.dioflag,
                     self.llflag, self.crcfl)
            args = ('/dev/zero', myblock, self.dev+str(ii),
                    long(self.skip)*blocksize, blocksize,
                    iocount, 0, None, self.Test, False, flags)
            # thrdName = 'myDDThrd_%d' % ii
            tstart.append(time.time())
            res = pool.apply_async(myDD, args)
            tres.append(res)
        telapsed = time.time() - tst
        tbytes = 0
        tmin = time.time() + 10000
        tmax = 0
        for ii in range(testcount):
            res = tres[ii].get(timeout=100)
            bspeed += res[0]
            cspeed += res[1]
            tspeed += res[2]
            tbytes += res[3]
            tmin = min([tmin, res[4]])
            tmax = max([tmax, res[5]])

        tthrough = tbytes/(tmax-tmin)

        print
        print "Overall throughput: %6.2f MB/s in %5.2f s" % (tthrough,
                                                             (tmax-tmin))
        status = (bspeed, cspeed, tspeed)
        return status

    def writeTestDD(self):
        """
        This is actually running the write test.

        Input:
             dev:            String, device name
             skip:           integer, number of GB to be skipped
             testcount:      integer, number of tests
             iosize:         integer, number of bytes/test
             blocksize:      integer, number of bytes/write
             parallel:       boolean, [False], whether to run the testcount
                             tests on seperate processes.
        """

        blocksize = long(self.blocksize)/4 * 4   # blocksize multiple of 4
        iosize = long(self.iosize)/4 * 4   # blocksize multiple of 4
        iocount = iosize/blocksize
        if self.dioflag:
            blocksize = long(blocksize)/512 * 512   # blocksize multiple of 512
            iosize = long(iosize)/512 * 512   # blocksize multiple of 512
            iocount = iosize/blocksize

        bspeed = []
        cspeed = []
        tspeed = []
        myblock = str(bytearray(os.urandom(blocksize)))

        for ii in range(self.testcount):
            st = time.time()
            if self.method == 'dd':
                command = 'dd if=/dev/zero of=%s skip=%d bs=%d count=%d' \
                      % (self.dev, self.skip, blocksize, iocount)
                print command
                st = time.time()
                (status, output) = commands.getstatusoutput(command)
            else:
                if self.dev[:4] != '/dev':
                    devn = self.dev + str(ii)
                else:
                    devn = self.dev
                try:
                    st = time.time()
                    flags = (self.syncflag, self.asyncflag, self.dioflag,
                             self.llflag, self.crcfl)
                    print "myDD('/dev/zero', myblock, "+devn+"," +\
                          str(self.skip*blocksize) + "," + str(blocksize) +\
                          "," + str(iocount)+", 0, None, 'write'" + \
                          str(flags) + ")"
                    status = myDD('/dev/zero', myblock, devn,
                                  long(self.skip)*blocksize, blocksize,
                                  iocount, 0, None, 'write', flags=flags)

                except Exception, e:
                    ex = str(e) + traceback.format_exc()
                    print ex
                    raise e
                finally:
                    pass
                if status == 256:
                    print "Permission denied"
                    print "Probably you don't have read access to "\
                          "device "+devn
                    print "Bailing out!"
                    sys.exit()
                elif status == 255:
                    print "Fail to open file obj, make sure the file system "\
                          + "supports low-level I/O"
                    sys.exit(255)
                else:
                    elapsed = time.time()-st
                    print 'Throughput (elapsed time): %3.2f MB/s (%5.2f s)\n'\
                          % (iosize/elapsed/1024./1024., elapsed)
                    bspeed += status[0]
                    cspeed += status[1]
                    tspeed += status[2]
            if self.method != 'dd':
                status = (bspeed, cspeed, tspeed)

        return status

    def writeTest(self):
        """
        This is actually running the write test.

        Input:
             dev:            String, device name
             skip:           integer, number of GB to be skipped
             testcount:      integer, number of tests
             iosize:         integer, number of bytes/test
             blocksize:      integer, number of bytes/write
        """
        blocksize = long(self.blocksize)/4 * 4   # blocksize multiple of 4
        iosize = long(self.iosize)/4 * 4   # blocksize multiple of 4
        iocount = self.iosize/self.blocksize

        # create a block filled with pattern and write it to file
        block = self.pattern * (self.blocksize/len(self.pattern))

        ofil = open(self.dev, 'w')

        bspeed = []
        for ii in range(self.testcount):
                st = time.time()
                status = 0
                ofil.seek(self.skip, 0)
                for jj in range(iocount):
                        stb = time.time()
                        ofil.write(block)
                        bspeed.append(blocksize/1024.**2/(time.time() - stb))

                if status == 256:
                        print "Permission denied"
                        print "Probably you don't have read access to "\
                              "device "+self.dev
                        print "Bailing out!"
                        sys.exit()
                else:
                        elapsed = time.time()-st
                        print 'Throughput (elapsed time of test): %3.2f MB/s '\
                              + '(%5.2f s)' % (iosize/elapsed/1024./1024.,
                                               elapsed)
                        print

                self.skip = self.skip + iocount

        return bspeed

    def cleanup(self, fnm):
        """
        Remove the files produced during the write test
        """
        print "Cleaning up...."
        fils = glob.glob(fnm+'*')
        dum = map(lambda x: os.remove(x), fils)
        return dum


def myDD(ifil='/dev/zero', block=None, ofil='/dev/null', skip=0,
         blocksize=1024, count=1, seek=0, httpobj=None, Test='read',
         writeStat=False, flags=(0, 0, 0, 0, 0)):
    """
    """
    bspeed = []
    cspeed = []
    tspeed = []
    tsize = 0
    one_mb = 1024. ** 2
    bsize = blocksize/one_mb
    crc = 0
    sleepTime = None
    data_rate = None
    (syncflag, asyncflag, dioflag, llflag, crcfl) = flags
    NGAMS_HTTP_SUCCESS = 200

    ifil_not_zero = (ifil != '/dev/zero')
    if ifil_not_zero:
        try:
            inputf = open(ifil)
            inputf.seek(skip, 0)
        except:
            status = 256
            try:
                inputf.close()
            except:
                pass
            return status
    """
    else:
        block = str(bytearray(os.urandom(blocksize)))
    """
    # print "myDD(ifil='{0}',block = {1},ofil='{2}',skip={3},blocksize={4},
    # count={5},seek={6}, httpobj={7})".\
    # format(ifil, block, ofil, skip, blocksize, count, seek, httpobj)

#    if ofil != '/dev/null' and ofil != None:
    if ofil is not None:
        try:
            out = None
            if (httpobj):
                crcfl = 0  # http does not need to do client crc
                if (data_rate):
                    sleepTime = blocksize / (data_rate * one_mb)
            else:
                if llflag:
                    if asyncflag == 1 and dioflag == 0:
                        fd = os.open(ofil,
                                     os.O_CREAT | os.O_TRUNC | os.O_WRONLY |
                                     os.O_ASYNC)
                    elif asyncflag == 0 and dioflag == 0:
                        fd = os.open(ofil,
                                     os.O_CREAT | os.O_TRUNC | os.O_WRONLY)
                    if dioflag == 1:
                        if os.__dict__.has_key('O_DIRECT'):
                            fd = os.open(ofil,
                                         os.O_CREAT | os.O_TRUNC |
                                         os.O_DIRECT | os.O_WRONLY)
                        else:
                            print "The OS does not support direct I/O"
                            return 255
                    elif syncflag == 1:
                        fd = os.open(ofil, os.O_CREAT | os.O_TRUNC |
                                     os.O_SYNC | os.O_WRONLY)
                else:
                    out = open(ofil, 'w')
                    out.seek(seek)
        except Exception, ex:
            print "Fail to open file or http obj, exception: %s" % str(ex)
            status = 255
            if (out):
                out.close()
            return status
        if Test == 'write':
            print "Writing {0} blocks to {1}".format(count, ofil)
            tsize = count * bsize
        else:
            print "Reading {0} blocks from {1}".format(count, ifil)
            tsize = 0
        crctime = 0.0
        ebsize = bsize = blocksize/one_mb  # block size in MB

        write_time = 0.0
        read_time = 0.0
        zcount = 0
        short_read = 0
        sti = time.time()
        if dioflag:
            m = mmap.mmap(-1, blocksize)
        bavg = 0
        bavgStart = sti
        avgCount = 10
        for ii in range(count):
            stt = time.time()
            if ifil_not_zero:
                if (Test == 'read' or ifil != '/dev/zero'):
                    stb = time.time()  # start time for reading block
                block = inputf.read(blocksize)
                if (Test == 'read' or ifil != '/dev/zero'):
                    one_block_time = time.time() - stb
                    read_time += one_block_time
                    if ebsize != len(block)/one_mb:  # full block?
                        # NOTE: On Mac OSX reading from a partion block device
                        # results in just a few blocks being read correctly.
                        # Thus always read from the actual block device, e.g.
                        # /dev/rdisk0 instead of /dev/rdisk0s1. Linux does not
                        # create character devices, i.e. the 'r' version of the
                        # block devices is not available. The O_DIRECT flag
                        # achieves the same behaviour, but is not implemented
                        # in this program for read tests.
                        short_read += 1
                        bsize = len(block)/one_mb
                    tsize += bsize
            if crcfl:
                stc = time.time()  # start time for CRC calc
                crc = crc32(block, crc)
                crct = time.time() - stc
                if crct == 0:
                    crct = 10**-6
                if writeStat:
                    cspeed.append((bsize/(crct), stc, crct))
                crctime += crct
            else:
                cspeed.append((-1, time.time(), -1))  # dummy values
            if (Test == 'write'):
                stb = time.time()  # start time for writing block
                if (httpobj):
                    httpobj._conn.sock.sendall(block)
                else:
                    if llflag:
                        if dioflag:
                            m.seek(0, 0)
                            m.write(block)
                            os.write(fd, m)
                            # block = m
                        else:
                            os.write(fd, block)
                    else:
                        out.write(block)
                if ii > 0 and (ii % avgCount) == 0:
                    bavg = (time.time() - bavgStart)/avgCount
                    if bavg == 0:
                        avgCount *= 2
                    bavgStart = time.time()
                one_block_time = time.time() - stb
                if one_block_time == 0:
                    one_block_time = max(bavg, 10**-10)
                    zcount += 1
            tend = time.time()
            write_time += one_block_time
            if (sleepTime and sleepTime > one_block_time):
                time.sleep(sleepTime - one_block_time)
            if (bsize > 0):
                if writeStat:
                    bspeed.append((bsize / one_block_time, stb,
                                   one_block_time))
                total_block_time = tend - stt
                if writeStat:
                    tspeed.append((bsize/total_block_time, stt,
                                   total_block_time))
                # tspeed.append((bsize/one_block_time, stt,one_block_time))
        if (Test == 'write'):
            print "Pure write throughput:  %6.2f MB/s" % (tsize/write_time)
        elif (Test == 'read' or ifil != '/dev/zero'):
            print "Pure read throughput:  %6.2f MB/s" % (tsize/read_time)
        print "Zero time blocks:  %6d. Using running avarage of %d blocks."\
              % (zcount, avgCount)
        if (Test == 'read' and short_read != 0):
            print "Short reads:  %6d" % (short_read)
            print "CAUTION: short reads usually means that the test did not"
            print "work correctly. On Mac OSX please either use a file to read"
            print "from or the plain character device, e.g. /dev/rdisk0"
        writelabel = Test+' '
        if (Test == 'cpu'):
            writelabel = ''
        print "Internal throughput (%s[+crc]): %6.2f MB/s" % \
              (writelabel, tsize/(time.time()-sti))
        fst = time.time()
        if ifil != '/dev/zero':
            inputf.close()
        if (httpobj):
            # get HTTP response
            reply, msg, hdrs = httpobj.getreply()
            if (hdrs is None):
                errMsg = "Illegal/no response to HTTP request encountered!"
                raise Exception, errMsg
            # we do not check msg or data for simplicity

            if (reply != NGAMS_HTTP_SUCCESS):
                raise Exception("Error in HTTP response %d" % reply)
            # we do not close http or its internal socket inside this`
            # function
        else:
            if llflag or dioflag:
                # out.flush()
                os.fsync(fd)
                os.close(fd)
            else:
                # out.flush()
                out.close()
        ste = time.time()  # end time of this test
        print "File closing time: %5.2f s" % (time.time()-fst)
        if (crcfl):
            print "CRC throughput: %6.2f MB/s (%5.2f s)" % \
                    (tsize/crctime, crctime)
        print "Total throughput (%s[+ crc] + file-close): %6.2f MB/s" % \
              (writelabel, tsize/(ste-sti))
        return (bspeed, cspeed, tspeed, tsize, sti, ste)
    else:  # do just plain nothing if no output file is specified
        for ii in range(count):
            block = inputf.read(blocksize)

        inputf.close()
        status = 0
        return status


def main(opts):
    global crc32
    dT = diskTest()

    for o, v in opts:
        if o in ("-d", "--device"):
            dT.dev = v
        if o in ("-s", "--skip"):
            dT.skip = long(v) * 1073741824l/dT.blocksize
        if o in ("-t", "--testcount"):
            dT.testcount = int(v)
        if o in ("-i", "--iosize"):
            dT.iosize = int(v)
        if o in ("-b", "--blocksize"):
            dT.blocksize = int(v)
        if o in ("-z", "--sndbufsize"):
            dT.tcpsndbuf = int(v)
        if o in ("-c", "--crc"):
            crcfl = v
            if crcfl not in ['b', 'z', 'c']:
                crcfl = 'b'
                dT.crcfl = 1
            if crcfl == 'b':
                dT.crcfl = 1
                from binascii import crc32
            elif crcfl == 'z':
                dT.crcfl = 1
                from zlib import crc32
            else:
                dT.crcfl = 1
                from crc32c import crc32
        if o in ("-e", "--session"):
            session_id = v
        if o in ("-r", "--datarate"):
            dT.data_rate = int(v)
        if o in ("-f", "--file"):
            DEFAULT_FNM = v
        if o in ("-l", "--lowio"):
            dT.llflag = 1
            if v == 'direct':
                dT.dioflag = 1
                # DIRECT I/O only works with 512 mutiples
                dT.blocksize = long(dT.blocksize)/512 * 512   # blocksize 512
                dT.iosize = long(dT.iosize)/512 * 512   # blocksize 512
                dT.iocount = dT.iosize/dT.blocksize
            elif v == 'async':
                dT.asyncflag = 1
            elif v == 'sync':
                dT.syncflag = 1
        if o in ("-u", "--cpu"):
            dT.Test = 'cpu'  # cpu test only
        if o in ("-w", "--write"):
            dT.Test = 'write'
        if o in ("-o", "--old"):
            old = 1
        if o in ("-m", "--method"):
            dT.method = 'myDD'
        if o in ("-p", "--parallel"):
            dT.parallel_stream = True
        if o in ("-S", "--Stats"):
            writeStat = True
        if o in ("-h", "--help"):
            usage()

    if dT.dev is None:
        usage()

    if dT.Test == 'read':
        bspeed = dT.readTest()
    elif dT.Test == 'write' or dT.Test == 'cpu':
        if dT.dev[0:4].lower() == 'http':
            print "To test writing to a remote NGAS disk"
            bspeed = dT.writeTestHTTP()
        elif dT.dev[0:4] == '/dev':

            # All the rest here just to make sure that there
            # is no dummy out there doing something wrong on
            # a plain device.

            print "CAUTION: This test will write directly to " +\
                  "a device and will most probably destroy the " +\
                  "existing data!!!"
            print "Continue: Yes/No?"
            w = sys.stdin.readline()[0:-1]

            if w == 'Yes':
                bspeed = dT.writeTestDD()
            elif w == 'No':
                sys.exit()
            else:
                print "You have entered: ", w
                print "Neither Yes or No!"
                print "Bailing out!!"
                sys.exit()

        else:
            if dT.iosize > 1073741824l * 100:
                print "You have selected a file for the " +\
                      "write test, but the size of that " +\
                      "file would be ",\
                      dT.iosize/1073741824l * 100, " GB"
                print "This may cause trouble on some systems"
                print "Bailing out!!"
                sys.exit()

            # create the file with the necessary size
            ofil = open(dT.dev, 'w')
            ofil.truncate(dT.iosize)
            ofil.close()
            if not dT.old:
                if not dT.parallel_stream:
                    bspeed = dT.writeTestDD()
                else:
                    bspeed = dT.parallelWriteTestDD()

            else:
                bspeed = dT.writeTest()
            dT.cleanup(dT.dev)
    else:
        sys.exit()
    if dT.writeStat and bspeed:
        fo = open(DEFAULT_FNM, 'w')
        p = pickle.Pickler(fo)
        p.dump(bspeed)
        del(p)
        fo.close()


def speedPlot(ifile=DEFAULT_FNM, timefl=1):
    """
    Produce a plot from the output produced by the internal Python
    dd implementation of diskTest.
    The CRC throughput is shown in RED. The pure I/O in BLUE and the
    total throughput (CRC+I/O) in GREEN.

    INPUT:
    ifile:    string, file name of the pickle file as produced by
              diskTest (default: bspeed.pkl)
    timefl:   [0|1]: flag, if set the plot will use time on the x-axis
              else it will use block numbers.
    """
    import pylab  # This requires pylab for the plotting...
    import matplotlib.patches as mpatches
    f = open(ifile)
    p = pickle.Unpickler(f)
    (bspeed, cspeed, tspeed) = p.load
    f.close()
    bspeed = pylab.array(bspeed)
    cspeed = pylab.array(cspeed)
    tspeed = pylab.array(tspeed)
    tzero = tspeed[0, 1]
    if timefl:
        pylab.plot(bspeed[:, 1] - tzero, bspeed[:, 0], 'b+')
        pylab.xlabel('Time since start [s]')
        pylab.ylabel('Throughput[MB/s]')
        plt = pylab.plot(tspeed[:, 1] - tzero, tspeed[:, 0], 'bx', mfc='none')
        if cspeed[0:, 0].max() != -1:  # plot only if not dummy
            plt = pylab.plot(cspeed[:, 1] - tzero, cspeed[:, 0], 'r+')
            red_patch = mpatches.Patch(color='red', label='CRC performance')
    else:
        pylab.plot(bspeed[:, 0], 'b+')
        pylab.xlabel('Block #')
        pylab.ylabel('Throughput[MB/s]')
        plt = pylab.plot(tspeed[:, 0], 'bx', mfc='none')
        if cspeed[0:, 0].max() != -1:  # plot only if not dummy
            plt = pylab.plot(cspeed[:, 0], 'r+')
            red_patch = mpatches.Patch(color='red', label='CRC performance')

    totalSize = (tspeed[:, 0] * tspeed[:, 2]).sum()
    totalTime = tspeed[-1][1]-tspeed[0][1]
    pylab.plot([0, totalTime], [totalSize/totalTime, totalSize/totalTime], 'g')
    blue_patch = mpatches.Patch(color='blue', label='Block I/O')
    green_patch = mpatches.Patch(color='green', label='Total I/O')
    if cspeed[0:, 0].max() != -1:  # plot only if not dummy
        pylab.legend(handles=[red_patch, blue_patch, green_patch])
    else:
        pylab.legend(handles=[blue_patch, green_patch])

    pylab.title(os.path.basename(ifile))
    ymax = plt[0].axes.get_ylim()[1]  # get current maximum
    plt[0].axes.set_ylim([0, ymax])

    return


if __name__ == '__main__':
    import getopt

    opts, args = getopt.getopt(sys.argv[1:], "d:s:t:i:b:z:c:e:r:f:l:womphuS",
                               ["device", "skip", "testcount", "iosize",
                                "blocksize", "sndbufsize",
                                "crc", "lowio", "session", "datarate", "file",
                                "write", "old", "method", "parallel", "help",
                                "cpu"])

    main(opts)
