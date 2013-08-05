import urllib2
import re, time

'''
Parse fileID out of URI 
'''
def parseURI(uri):
        #file = re.split(r'([=])', uri)
        file = re.split(r'file_id=', uri)
        return file[1]

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
            print "Read throughput: %5.2f MB/s" % (file_size/str/1024./1024.)
            print "Write throughput: %5.2f MB/s" % (file_size/stw/1024./1024.)
            print "Total throughput: %5.2f MB/s" % (file_size/(ste-stt)/1024./1024.)
            return file_name
            
        finally:
            if f:
                f.close()
    finally:
        if u:
            u.close()

def main():
    #url = 'http://cortex.ivec.org:7777/RETRIEVE?processing=ngamsMWACortexStageDppi&file_id=1059596416_20130803201959_gpubox19_00.fits&send_buffer=87380'
    url = 'http://cortex.ivec.org:7777/RETRIEVE?processing=ngamsMWACortexStageDppi&file_id=1059596416_20130803201959_gpubox19_00.fits'
    out = './'
    buffsize = 87380
    #buffsize = 16777216
    
    download(url, out, buffsize)

if __name__ == "__main__":
    main()