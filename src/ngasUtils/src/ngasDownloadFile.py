import urllib2, re, time, threading
from Queue import Queue, Empty
from optparse import OptionParser

"""
This module is to test the I/O performance when downloadiing a file
from a remote NGAS server

Usage: python ngasDownloadFile.py [options]

Options:
  -h, --help            show this help message and exit
  -a, --async           use asynchronous I/O?
  -u URL, --url=URL     url from where to download a file
  -o OUT, --out=OUT     output path where file is saved
  -b BUFFSIZE, --buffsize=BUFFSIZE
                        buffer size

"""

'''
Parse fileID out of URI
'''
def parseURI(uri):
        #file = re.split(r'([=])', uri)
        file = re.split(r'file_id=', uri)
        return file[1]

def _readThread(file_size, u, buffsize, buffq, read_out):
    str = 0
    read_out.append(time.time())
    while True:
        strb = time.time()
        buffer = u.read(buffsize)
        str += time.time() - strb

        if not buffer:
            break
        buffq.put(buffer) #Queue is thread-safe, so no need to sync :-)
    read_out.append(time.time())
    read_out.append(str)

def _writeThread(file_name, file_size, f, buffq, write_out):
    stw = 0
    write_out.append(time.time())
    file_size_dl = 0
    while True:
        try:
            buffer = buffq.get(timeout = 0.01)
            stwb = time.time()
            f.write(buffer)
            stw += time.time() - stwb
            file_size_dl += len(buffer)
            #status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            #status = status + chr(8)*(len(status)+1)
            #print status,
        except Empty, e:
            if (file_size_dl == file_size):
                break
            else:
                continue
    write_out.append(time.time())
    write_out.append(stw)

def asyncDownload(url, out, buffsize):
    """
    Interleave network read with disk write,
    so that time spent on reading data
    is mostly hidden by time spent on writing data given
    that disk write is at least two times slower
    """
    buffq = Queue()
    # extract filename
    file_name = parseURI(url)
    try:
        # open file URL
        u = urllib2.urlopen(url, timeout = 1800)
        u.fp.bufsize = buffsize

        # get file size
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])

        if out == None or len(out) == 0:
            out = './'

        # check we have a forward slash before file
        if out[len(out)-1] != '/':
             out += '/'

        f = open(out + file_name, 'wb')

        print "Downloading: %s Bytes: %s" % (file_name, file_size)
        read_out = [] #  time_start, time_end
        write_out = [] # time_start, time_end
        read_args = (file_size, u, buffsize, buffq, read_out)
        write_args = (file_name, file_size, f, buffq, write_out)

        readthrd = threading.Thread(None, _readThread, "reader", read_args)
        readthrd.setDaemon(1)

        writethrd = threading.Thread(None, _writeThread, "writer", write_args)
        writethrd.setDaemon(1)

        stt = time.time()
        readthrd.start()
        writethrd.start()

        readthrd.join()
        writethrd.join()
        ste = time.time()

        print "Read throughput: \t%5.2f MB/s" % (file_size/read_out[2]/1024./1024.)
        print "Read Q overhead: \t%5.2f seconds" % (read_out[1] - read_out[0] - read_out[2])
        print "Write throughput: \t%5.2f MB/s" % (file_size/write_out[2]/1024./1024.)
        print "Write Q overhead: \t%5.2f seconds" % (write_out[1] - write_out[0] - write_out[2])

        """
        if (read_out[0] <= write_out[0]):
            stt = read_out[0]
        else:
            stt = write_out[0]

        if (read_out[1] >= write_out[1]):
            ste = read_out[1]
        else:
            ste = write_out[1]
        """

        print "Total throughput: \t%5.2f MB/s" % (file_size/(ste-stt)/1024./1024.)
    finally:
        if u:
            u.close()
        if f:
            f.close()


def download(url, out, buffsize):

    # extract filename
    file_name = parseURI(url)

    try:
        # open file URL
        u = urllib2.urlopen(url, timeout = 1800)
        u.fp.bufsize = buffsize

        # get file size
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])

        print "Downloading: %s Bytes: %s" % (file_name, file_size)

        file_size_dl = 0
        block_sz = buffsize

        try:

            if out == None or len(out) == 0:
                out = './'

            # check we have a forward slash before file
            if out[len(out)-1] != '/':
                 out += '/'

            f = open(out + file_name, 'wb')

            stt = time.time()
            str = 0
            stw = 0
            while True:
                strb = time.time()
                buffer = u.read(block_sz)
                str += time.time() - strb

                if not buffer:
                    break

                #file_size_dl += len(buffer)
                stwb = time.time()
                f.write(buffer)
                stw += time.time() - stwb
                #status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
                #status = status + chr(8)*(len(status)+1)
                #print status,

            ste = time.time()
            print "Read throughput: \t%5.2f MB/s" % (file_size/str/1024./1024.)
            print "Write throughput: \t%5.2f MB/s" % (file_size/stw/1024./1024.)
            print "Total throughput: \t%5.2f MB/s" % (file_size/(ste-stt)/1024./1024.)
            return file_name

        finally:
            if f:
                f.close()
    finally:
        if u:
            u.close()

def main():
    #url = 'http://cortex.ivec.org:7777/RETRIEVE?processing=ngamsMWACortexStageDppi&file_id=1059596416_20130803201959_gpubox19_00.fits&send_buffer=87380'
    url = 'http://cortex.ivec.org:7777/RETRIEVE?processing=ngamsMWACortexStageDppi&file_id=' #1059774680_20130805215103_gpubox18_00.fits'
    out = './'
    buffsize = 87380
    #buffsize = 16777216

    parser = OptionParser()
    parser.add_option("-a", "--async", action="store_true", dest="asyncDownload", help = "use asynchronous I/O?")
    parser.add_option("-f", "--fileid", dest = "fileId", help = "file id of the file to be downloaded")
    parser.add_option("-o", "--out", dest = "out", help = "output path where file is saved")
    parser.add_option("-b", "--buffsize", dest = "buffsize", help = "buffer size")
    parser.add_option("-s", "--nbuffsize", dest = "ngas_buffsize", help = "the size of the NGAS server send buffer")

    (options, args) = parser.parse_args()
    if (not options.fileId):
        print "Please specify the fileid using the -f option"
        parser.print_usage()
    else:
        url += options.fileId

    if (options.ngas_buffsize):
        url += '&send_buffer=%s' % options.ngas_buffsize

    if (options.out):
        out = options.out

    if (options.buffsize):
        buffsize = int(options.buffsize)

    if (options.asyncDownload):
        asyncDownload(url, out, buffsize)
    else:
        download(url, out, buffsize)

if __name__ == "__main__":
    main()