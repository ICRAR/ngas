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
import sys,re, os, socket, traceback, datetime, mmap, threading
import base64, glob

Test = 'read'           # default test is readTest
skip = 0        # default skip [GB]
testcount = 1        # default number of consecutive tests
iosize = 1073741824l    # default size of one test: 1 GB
blocksize = 1024    # default size of IO blocks
tcpsndbuf = None # default tcp send buffer size
dev = None              # no default for the actual device
method = 'dd'           # default method for performing the tests
pattern = 'abcd'        # default pattern to be used for writeTest
bspeed = None
old = 0
crcfl = ''              # default is no crc
llflag = 0              # default use normal I/O
asyncflag = 0
syncflag = 0
dioflag = 0
session_id = None       # For write to HTTP only, default is None
data_rate = None        # default there is no data rate limit for HTTP write
NGAMS_HTTP_SUCCESS = 200
NGAMS_HTTP_POST = 'POST'
one_mb = 1024. ** 2
DEFAULT_FNM = 'bspeed.pkl'
parallel_stream = False

#    "d:s:t:i:b:z:c:e:r:f:l:womph",\
#           ["device","skip","testcount","iosize","blocksize", "sndbufsize",\
#            "crc", "lowio", "session", "datarate", "file",\
#            "write","old","method","parallel","help"])

def usage():
    """
    This code contains methods to perform performance tests of
    block devices, e.g. hard disks. It also allows using files rather
    than devices directly.

    Synopsis: diskTest.py [-d device] [-s skip] [-t testcount] [-i iosize]
                          [-b blocksize] [-c {b|z}] [-f file] [-m] [-w] [-l] [-o]
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
          [c]rc:       string, if 'b' binascii is used, if 'z' zlib.
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

def readTest(dev,skip,testcount,iosize,blocksize):
    """
    This is actually running the read test.

    Input:
         dev:            String, device name
         skip:           integer, number of GB to be skipped
         testcount:      integer, number of tests
         iosize:         integer, number of bytes/test
         blocksize:      integer, number of bytes/write
    """

    blocksize = long(blocksize)/4 * 4   # blocksize multiple of 4
    iosize = long(iosize)/4 * 4   # blocksize multiple of 4
    iocount = iosize/blocksize

    for ii in range(testcount):
        st=time.time()
        if method == 'dd':
            command = 'dd if=%s of=/dev/null skip=%d bs=%d count=%d' \
                  % (dev,skip,blocksize,iocount)
            print command
            st=time.time()
            (status,output) = commands.getstatusoutput(command)
        else:
            st=time.time()
            print "myDD("+dev+",'/dev/null',"+str(skip)+","+\
                  str(blocksize)+","+str(iocount)+")"
            status = myDD(dev,'/dev/null',\
                           long(skip)*blocksize,blocksize,\
                           iocount)

        if status == 256:
            print "Permission denied"
            print "Probably you don't have read access to "\
                  "device "+dev
            print "Bailing out!"
            sys.exit()
        else:
            elapsed = time.time()-st
            print 'Throughput: %3.2f MB/s (%5.2f s)' % \
                  (iosize/elapsed/1024./1024., elapsed)

        skip =skip + iocount
        return status

def writeTestHTTP(dev, skip, testcount, iosize, blocksize, sessionId = None, sndbufsize = None, parallel = False):
    """
    This is actually running the HTTP-based remote write test.
    It will read blocks of zeroes from /dev/zero and
    and send these blocks to the server as a single file

    Input:
         dev:            String, url to which data is sent
         sessionId:      String, this will be used to form the prefix of the file name:
                         sessionId_nodeId_count.dat
         testcount:      integer, number of tests
         iosize:         integer, number of bytes/test
         blocksize:      integer, number of bytes/write

    """
    blocksize = long(blocksize)/4 * 4   # blocksize multiple of 4
    iosize = long(iosize)/4 * 4   # blocksize multiple of 4
    iocount = iosize/blocksize
    bspeed = []
    cspeed = []
    tspeed = []
    nodeId = socket.gethostname()

    locTimeout = 3600
    mimeType = 'application/octet-stream'
    user = 'ngasmgr'
    pwd = 'ngas$dba' # this should be passed in
    authHdrVal = "Basic " + base64.encodestring(user + ":" + pwd)
    if (authHdrVal[-1] == "\n"): authHdrVal = authHdrVal[:-1]

    if (not sessionId):# if no sessionId, file names from different nodes will have different prefix
        dt = datetime.datetime.now()
        sessionId = dt.strftime('%Y%m%dT%H%M%S')

    import httplib

    if (parallel):
        tst = time.time()
        myDDThreads = []

    for ii in range(testcount):
        st=time.time()
        fname = '%s_%s_%s.dat' % (sessionId, nodeId, str(ii))
        contDisp = "attachment; filename=\"%s\"; no_versioning=1" % fname
        # make HTTP headers
        url = dev
        idx = (url[7:].find("/") + 7) # Separate the URL from the command.
        tmpUrl = url[7:idx]
        cmd    = url[(idx + 1):]

        http = httplib.HTTP(tmpUrl)
        try:
            #print "Sending HTTP header ..."
            http.putrequest(NGAMS_HTTP_POST, cmd)

            http.putheader("Content-type", mimeType)
            http.putheader("Content-disposition", contDisp)
            http.putheader("Content-length", str(iosize))
            http.putheader("x-ddn-policy", "replica-store")
            print "Content-length = %s" % str(iosize)
            http.putheader("Authorization", authHdrVal)
            http.putheader("Host", nodeId)
            http.putheader("NGAS-File-CRC", "1533330096")
            http.endheaders()
            # send payload
            http._conn.sock.settimeout(locTimeout)
            if (sndbufsize):
                try:
                    http._conn.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbufsize)
                    print("Set TCP SNDBUF to %d" % sndbufsize)
                except Exception, eer:
                    print('Fail to set TCP SNDBUF to %d: %s' % (sndbufsize, str(eer)))

            if (not parallel):
                st=time.time()
                status = myDD('/dev/zero', dev, \
                               long(skip)*blocksize,blocksize,\
                               iocount, httpobj = http)
            else:
                #ifil='/dev/zero',ofil='/dev/null',skip=0,blocksize=1024,count=1,seek=0, httpobj=None
                args = ('/dev/zero', dev, \
                               long(skip)*blocksize,blocksize,\
                               iocount, 0, http)
                thrdName = 'myDDThrd_%d' % ii
                ddThrRef = threading.Thread(None, myDD, thrdName, args)
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
                        http.close() # this may fail?
                    finally:
                        del http
        if (not parallel):
            elapsed = time.time()-st
            print 'Throughput: %3.2f MB/s (%5.2f s)' % \
                  (iosize/elapsed/1024./1024., elapsed)
            bspeed += status[0]
            cspeed += status[1]
            tspeed += status[2]

    if (parallel):
        for dtr in myDDThreads:
            dtr.join()
        telapsed = time.time() - tst
        print 'Total throughput: %3.2f MB/s (%5.2f s)' % \
              (iosize * testcount /telapsed/1024./1024., telapsed)

    return (bspeed, cspeed, tspeed)

def writeTestDD(dev,skip,testcount,iosize,blocksize):
    """
    This is actually running the write test.

    Input:
         dev:            String, device name
         skip:           integer, number of GB to be skipped
         testcount:      integer, number of tests
         iosize:         integer, number of bytes/test
         blocksize:      integer, number of bytes/write
    """

    blocksize = long(blocksize)/4 * 4   # blocksize multiple of 4
    iosize = long(iosize)/4 * 4   # blocksize multiple of 4
    iocount = iosize/blocksize
    bspeed = []
    cspeed = []
    tspeed = []
    for ii in range(testcount):
        st=time.time()
        if method == 'dd':
            command = 'dd if=/dev/zero of=%s skip=%d bs=%d count=%d' \
                  % (dev,skip,blocksize,iocount)
            print command
            st=time.time()
            (status,output) = commands.getstatusoutput(command)
        else:
            if dev[:4] != '/dev':
                dev += str(testcount)
            st=time.time()
            print "myDD('/dev/zero',"+dev+","+str(skip*blocksize)+","+\
                  str(blocksize)+","+str(iocount)+")"
            status = myDD('/dev/zero', dev, \
                           long(skip)*blocksize,blocksize,\
                           iocount)

        if status == 256:
            print "Permission denied"
            print "Probably you don't have read access to "\
                  "device "+dev
            print "Bailing out!"
            sys.exit()
        else:
            elapsed = time.time()-st
            print 'Throughput: %3.2f MB/s (%5.2f s)' % \
                  (iosize/elapsed/1024./1024., elapsed)
            bspeed += status[0]
            cspeed += status[1]
            tspeed += status[2]
    if method != 'dd':
        status = (bspeed, cspeed, tspeed)

    return status



def writeTest(dev,skip,testcount,iosize,blocksize):
    """
    This is actually running the write test.

    Input:
         dev:            String, device name
         skip:           integer, number of GB to be skipped
         testcount:      integer, number of tests
         iosize:         integer, number of bytes/test
         blocksize:      integer, number of bytes/write
    """
    blocksize = long(blocksize)/4 * 4   # blocksize multiple of 4
    iosize = long(iosize)/4 * 4   # blocksize multiple of 4
    iocount = iosize/blocksize

    #create a block filled with pattern and write it to file
    block = pattern * (blocksize/len(pattern))

    ofil = open(dev,'w')

    bspeed = []
    for ii in range(testcount):
            st=time.time()
            status = 0
            ofil.seek(skip, 0)
            for jj in range(iocount):
                    stb = time.time()
                    ofil.write(block)
                    bspeed.append(blocksize/1024.**2/(time.time() - stb))

            if status == 256:
                    print "Permission denied"
                    print "Probably you don't have read access to "\
                          "device "+dev
                    print "Bailing out!"
                    sys.exit()
            else:
                    elapsed = time.time()-st
                    print 'Throughput: %3.2f MB/s (%5.2f s)' % \
                          (iosize/elapsed/1024./1024., elapsed)

            skip =skip + iocount

    return bspeed




def myDD(ifil='/dev/zero',ofil='/dev/null',skip=0,blocksize=1024,count=1,seek=0, httpobj=None):
    """
    """
    bspeed = []
    cspeed = []
    tspeed = []
    crc = 0
    sleepTime = None
    if ifil != '/dev/zero':
        try:
            inputf = open(ifil)
            inputf.seek(skip,0)
        except:
            status = 256
            try:
                inputf.close()
            except:
                pass
            return status
    else:
        block = str(bytearray(blocksize))

    if ofil != '/dev/null':
        try:
            if (httpobj):
                global crcfl
                crcfl = '' # http does not need to do crc at the client side
                if (data_rate):
                    sleepTime = blocksize / (data_rate * one_mb)
            else:
                if llflag:
                    if asyncflag == 1 and dioflag == 0:
                        fd = os.open(ofil, os.O_CREAT | os.O_TRUNC | os.O_WRONLY | os.O_ASYNC)
                    elif asyncflag == 0 and dioflag == 0:
                        fd = os.open(ofil, os.O_CREAT | os.O_TRUNC | os.O_WRONLY)
                    if dioflag == 1:
                        if os.__dict__.has_key('O_DIRECT'):
                            fd = os.open(ofil, os.O_CREAT | os.O_TRUNC | os.O_DIRECT  | os.O_WRONLY)
                    elif syncflag == 1:
                        fd = os.open(ofil, os.O_CREAT | os.O_TRUNC | os.O_SYNC  | os.O_WRONLY)
                else:
                    out = open(ofil,'w')
                    out.seek(seek)
        except Exception, ex:
            status = 255
            out.close()
            return status
        print "Writing {0} blocks to {1}".format(count, ofil)
        crctime = 0.0
        bsize = blocksize/one_mb
        tsize = bsize * count
        if dioflag:
            m = mmap.mmap(-1, blocksize)
            m.write(block)
            block = m

        sti = time.time()
        for ii in range(count):
            stt = time.time()
            if ifil != '/dev/zero':
                block=inputf.read(blocksize)
            if crcfl:
                stc = time.time()
                crc = crc32(block, crc)
                crct = time.time() - stc
                if crct == 0: crct = 10**-6
                cspeed.append((bsize/(crct), stc,crct))
                crctime += crct
            else:
                cspeed.append((-1,time.time(), -1))
            stb = time.time()
            if (httpobj):
                httpobj._conn.sock.sendall(block)
            else:
                if llflag:
                    os.write(fd, block)
                elif dioflag:
                    os.write(fd, block)
                else:
                    out.write(block)
            tend = time.time()
            one_block_time = tend - stt
            if (sleepTime and sleepTime > one_block_time):
                time.sleep(sleepTime - one_block_time)
            bspeed.append((bsize/(tend - stb), stb, tend-stb))
            #tspeed.append((bsize/(tend - stt), stt))
            tspeed.append((bsize/one_block_time, stt, one_block_time))
        print "Internal throughput: %6.2f MB/s" % \
              (tsize/(time.time()-sti))
        fst = time.time()
        if ifil != '/dev/zero': inputf.close()
        if (httpobj):
            # get HTTP response
            reply, msg, hdrs = httpobj.getreply()
            if (hdrs == None):
                errMsg = "Illegal/no response to HTTP request encountered!"
                raise Exception, errMsg
            # we do not check msg or data for simplicity

            if (reply != NGAMS_HTTP_SUCCESS):
                raise Exception("Error in HTTP response %d" % reply)
            # we do not close http or its internal socket inside this function
        else:
            if llflag or dioflag:
                os.fsync(fd)
                os.close(fd)
            else:
                # out.flush()
                out.close()
        print "File closing time: %5.2f s" % (time.time()-fst)
        if (crcfl):
            print "CRC throughput: %6.2f MB/s (%5.2f s)" % \
                    (tsize/crctime, crctime)
        return (bspeed,cspeed, tspeed)
    else: # do just plain nothing if no output file is specified

        for ii in range(count):
            block = inputf.read(blocksize)

        inputf.close()
        status = 0
        return status

def cleanup(fnm):
    """
    Remove the files produced during the write test
    """
    fils = glob.glob(fnm+'*')
    dum = map(lambda x:os.remove(x), fils)
    return dum

if __name__ == '__main__':

    import getopt

    opts,args = getopt.getopt(sys.argv[1:],"d:s:t:i:b:z:c:e:r:f:l:womph",\
           ["device","skip","testcount","iosize","blocksize", "sndbufsize",\
            "crc", "lowio", "session", "datarate", "file",\
            "write","old","method","parallel","help"])

    for o,v in opts:
        if o in ("-d","--device"):
            dev = v
        if o in ("-s","--skip"):
            skip = long(v) * 1073741824l/blocksize
        if o in ("-t","--testcount"):
            testcount = int(v)
        if o in ("-i","--iosize"):
            iosize = int(v)
        if o in ("-b","--blocksize"):
            blocksize = int(v)
        if o in ("-z","--sndbufsize"):
            tcpsndbuf = int(v)
        if o in ("-c","--crc"):
            crcfl = v
            if crcfl not in ['b', 'z']:
                crcfl = 'b'
            if crcfl == 'b':
                from binascii import crc32
            else:
                from zlib import crc32
        if o in ("-e", "--session"):
            session_id = v
        if o in ("-r", "--datarate"):
            data_rate = int(v)
        if o in ("-f", "--file"):
            DEFAULT_FNM = v
        if o in ("-l", "--lowio"):
            llflag = 1
            if v == 'direct':
                dioflag = 1
            elif v == 'async':
                asyncflag = 1
            elif v == 'sync':
                syncflag = 1
        if o in ("-w","--write"):
            Test = 'write'
        if o in ("-o","--old"):
            old = 1
        if o in ("-m","--method"):
            method = 'myDD'
        if o in ("-p","--parallel"):
            parallel_stream = True
        if o in ("-h","--help"):
            usage()

    if dev == None: usage()

    if Test == 'read':
        readTest(dev,skip,testcount,iosize,blocksize)
    elif Test == 'write':
        if dev[0:4].lower() == 'http':
            print "To test writing to a remote NGAS disk"
            bspeed = writeTestHTTP(dev, skip, testcount, iosize, blocksize, sndbufsize = tcpsndbuf, parallel = parallel_stream)
        elif dev[0:4] == '/dev':

            # All the rest here just to make sure that there
            # is no dummy out there doing something wrong on
            # a plain device.

            print "CAUTION: This test will write directly to "+\
                  "a device and will most probably destroy the "+\
                  "existing data!!!"
            print "Continue: Yes/No?"
            w = sys.stdin.readline()[0:-1]
            while (re.match('[^q]',w)):
                if w == 'Yes':
                    bspeed = writeTestDD(dev,skip,testcount,iosize,\
                          blocksize)

                elif w == 'No':
                    sys.exit()
                else:
                    print "You have entered: ",w
                    print "Neither Yes or No!"
                    print "Bailing out!!"
                    sys.exit()
        else:
            if iosize > 1073741824l:
                print "You have selected a file for the "+\
                      "write test, but the size of that "+\
                      "file would be ",\
                      iosize/1073741824l," GB"
                print "This may cause trouble on some systems"
                print "Bailing out!!"
                sys.exit()

            # create the file with the necessary size
            ofil = open(dev,'w')
            ofil.truncate(iosize)
            ofil.close()
            if not old:
                bspeed = writeTestDD(dev,skip,testcount,iosize,\
                  blocksize)
            else:
                bspeed = writeTest(dev,skip,testcount,iosize,\
                        blocksize)
            cleanup(dev)
    else:
        sys.exit()
    if bspeed:
        fo = open(DEFAULT_FNM,'w')
        p = pickle.Pickler(fo)
        p.dump(bspeed)
        del(p)
        fo.close()




def speedPlot(ifile=DEFAULT_FNM, timefl=1):
    """
    Produce a plot from the output produced by the internal Python
    dd implementation of diskTest.

    INPUT:
    ifile:    string, file name of the pickle file as produced by
              diskTest (default: bspeed.pkl)
    timefl:   [0|1]: flag, if set the plot will use time on the x-axis
              else it will use block numbers.
    """
    import pylab  # This requires pylab for the plotting...
    f = open(ifile)
    p=pickle.Unpickler(f)
    (bspeed,cspeed,tspeed) = p.load()
    f.close()
    bspeed = pylab.array(bspeed)
    cspeed = pylab.array(cspeed)
    tspeed = pylab.array(tspeed)
    tzero = tspeed[0,1]
    if timefl:
        pylab.plot(bspeed[:,1] - tzero, bspeed[:,0],'b+')
        pylab.plot(cspeed[:,1] - tzero, cspeed[:,0],'r+')
        pylab.xlabel('Time since start [s]')
        pylab.ylabel('Throughput[MB/s]')
        pylab.plot(tspeed[:,1] - tzero, tspeed[:,0], 'g+')
    else:
        pylab.plot(bspeed[:,0],'b+')
        pylab.plot(cspeed[:,0],'r+')
        pylab.xlabel('Block #')
        pylab.ylabel('Throughput[MB/s]')
        pylab.plot(tspeed[:,0], 'g+')

    totalSize = (tspeed[:,0] * tspeed[:,2]).sum()
    totalTime = tspeed[-1][1]-tspeed[0][1]
    pylab.plot([0,totalTime],[totalSize/totalTime,totalSize/totalTime], 'g')

    pylab.title(os.path.basename(ifile))
