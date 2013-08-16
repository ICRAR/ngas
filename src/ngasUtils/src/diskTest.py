#!/usr/bin/env python
#
import time, pickle
import commands
import sys,re, os

Test = 'read'           # default test is readTest
skip = 0        # default skip [GB]
testcount = 1        # default number of consecutive tests
iosize = 1073741824l    # default size of one test: 1 GB
blocksize = 1024    # default size of IO blocks
dev = None              # no default for the actual device
method = 'dd'           # default method for performing the tests
pattern = 'abcd'        # default pattern to be used for writeTest
bspeed = None
old = 0
crcfl = ''              # default is no crc
llflag = 0              # default use normal I/O

def usage():
    """
    This class contains methods to perform performance tests of
    block devices, e.g. hard disks.

    Synopsis: diskTest.py [-d device] [-s skip] [-t testcount] [-i iosize]
                          [-b blocksize] [-c {b|z}] [-m] [-w] [-l] [-h]

                  long arguments are allowed as well, e.g. --device

          [d]evice:    string, e.g. /dev/hdb1
          [s]kip:      integer [0 GB], e.g. 5
          [t]estcount: integer [1], number of consecutive tests [1]
          [b]locksize: integer [1024 bytes], number of bytes in single
                       IO
          [c]rc32:     string, if 'b' binascii is used, if 'z' zlib.
          [i]osize:    integer [1073741824 bytes == 1 GB], full size of
                       one test, i.e. iosize/blocksize IOs will be
                       carried out before calculating statistics.
          [m]ethod:    flag, if set a python implementation of dd
                       will be used (only for readTest)
          [w]rite:     flag, if set writeTest is performed.
          [l]owio:     flag, if set during write test low level I/O
                       will be used.

          NOTE: All byte values are forced to be an integer multiple
                 of 4.

        Author:  A. Wicenec [ESO]
    Date:    29-May-2002
    Version 1.0
    Version 2.0    20-Jun-2002: writeTest included
    CVS Version: $Id: diskTest.py,v 1.2 2006/12/01 14:09:08 awicenec Exp $
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




def myDD(ifil='/dev/zero',ofil='/dev/null',skip=0,blocksize=1024,count=1,seek=0):
    """
    """
    bspeed = []
    cspeed = []
    tspeed = []
    crc = 0
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
            if llflag:
                fd = os.open(ofil, os.O_CREAT | os.O_WRONLY)# | os.O_NONBLOCK)
            else:
                out = open(ofil,'w')
                out.seek(seek)
        except:
            status = 255
            out.close()
            return status
        print "Writing {0} blocks to {1}".format(count, ofil)
    if crcfl:
        crctime = 0.0
        bsize = blocksize/1024.**2
        tsize = bsize * count
        sti = time.time()
        for ii in range(count):
            stt = time.time()
            if ifil != '/dev/zero':
                block=inputf.read(blocksize)
            if crcfl:
                stc = time.time()
                crc = crc32(block, crc)
                crct = time.time() - stc
                cspeed.append((bsize/(crct), stc))
                crctime += crct
            else:
                cspeed.append((-1,time.time()))
            stb = time.time()
            if llflag:
                os.write(fd, block)
            else:
                out.write(block)
            tend = time.time()
            bspeed.append((bsize/(tend - stb), stb))
            tspeed.append((bsize/(tend - stt), stt))
        print "Internal throughput: %6.2f MB/s" % \
              (tsize/(time.time()-sti))
        fst = time.time()
        if ifil != '/dev/zero': inputf.close()
        if llflag:
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
            block=inputf.read(blocksize)

        inputf.close()
        status = 0
        return status

if __name__ == '__main__':

    import getopt

    opts,args = getopt.getopt(sys.argv[1:],"d:s:t:i:b:c:lomwh",\
           ["device","skip","testcount","iosize","blocksize",\
            "write","old","method","help","lowio"])

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
        if o in ("-o","--old"):
            old = 1
        if o in ("-m","--method"):
            method = 'myDD'
        if o in ("-w","--write"):
            Test = 'write'
        if o in ("-l", "--lowio"):
            llflag = 1
        if o in ("-c","--crcfl"):
            crcfl = v
            if crcfl not in ['b', 'z']:
                crcfl = 'b'
            if crcfl == 'b':
                from binascii import crc32
            else:
                from zlib import crc32
        if o in ("-h","--help"):
            usage()

    if dev == None: usage()

    if Test == 'read':
        readTest(dev,skip,testcount,iosize,blocksize)
    elif Test == 'write':
        if dev[0:4] == '/dev':

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
    else:
        sys.exit()
    if bspeed:
        fo = open('bspeed.pkl','w')
        p = pickle.Pickler(fo)
        p.dump(bspeed)
        del(p)
        fo.close()


DEFAULT_FNM = 'bspeed.pkl'

def speedPlot(ifile=DEFAULT_FNM, timefl=0):
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

    pylab.title(os.path.basename(ifile))

