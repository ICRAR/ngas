#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#

#******************************************************************************
#
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu       02/10/2013  Created
#

"""
Python-based UDT sender based on PyUDT
"""
import sys, os, base64, urlparse, traceback, threading
import socket as socklib
import udt4 

MAX_LINE_LEN = 16384
MAXELEMENTS = 100
NGAMS_SOCK_TIMEOUT_DEF = 3600

g_udt_started = False
udt_start_lock = threading.Semaphore(1)

settings = {
        'host'  :   '127.0.0.1',
        'port'  :   9000
        }


def create_socket(host, port, blockSize = 65536, timeout = NGAMS_SOCK_TIMEOUT_DEF):
    
    global g_udt_started
    global udt_start_lock
    
    udt_start_lock.acquire()
    try:
        if (not g_udt_started):
            udt4.startup()
            g_udt_started = True 
    finally:
        udt_start_lock.release()
    
    print('create_client(%s, %s)' % (host, port))


    socket = udt4.socket(
            socklib.AF_INET, socklib.SOCK_STREAM, socklib.AI_PASSIVE
            ) 
    
    
    
    #print "UDT_SNDTIMEO = %d" % udt4.getsockopt(socket, udt4.UDT_SNDTIMEO)
    #print "UDT_RCVTIMEO = %d" % udt4.getsockopt(socket, udt4.UDT_RCVTIMEO)
    #
    # set sock options 
    # 
    if (not timeout or timeout < 0):
        loc_timeout = NGAMS_SOCK_TIMEOUT_DEF  
    else:
        loc_timeout = timeout
        
    opts = [ (udt4.UDP_SNDBUF, blockSize),
             (udt4.UDP_RCVBUF, blockSize),
             (udt4.UDT_SNDTIMEO, loc_timeout)
             ]
    
    for opt in opts:
        udt4.setsockopt(socket, opt[0], opt[1]) 
    #print "UDT_SNDTIMEO = %d" % udt4.getsockopt(socket, udt4.UDT_SNDTIMEO)
    #print "UDT_RCVTIMEO = %d" % udt4.getsockopt(socket, udt4.UDT_RCVTIMEO)
#    
#    
#    udt4.setsockopt(socket, udt4.UDP_SNDBUF,640000)
#    udt4.setsockopt(socket, udt4.UDP_RCVBUF,640000)
#    
    #print "UDP_SNDBUF = %d" % udt4.getsockopt(socket, udt4.UDP_SNDBUF)
    #print "UDP_RCVBUF = %d" % udt4.getsockopt(socket, udt4.UDP_RCVBUF)
    
#    udt4.setsockopt(socket, udt4.UDT_SNDBUF,64)
#    udt4.setsockopt(socket, udt4.UDT_RCVBUF,64)
    localhost = '146.118.84.66'
    udt4.bind(socket, localhost, port)
   
    #print('connecting client')
    udt4.connect(socket, host, port)
#    try:
#        udt4.connect(socket, host, port)
#    except Exception as err:
#        #print('Exception: %s' % err)
#        raise err
    
    #print "%s ---------" % str(socket.UDTSOCKET)
    return socket


def send_file(udtsocket, file_path):
    #print('send_file...')
        
    fdIn = open(file_path, 'r')
    fsize = os.stat(file_path).st_size
    #print "File size = %d" % fsize
    #udt4.sendfile(udtsocket, f, 0, fsize) # this does not work
    blockSize = udt4.getsockopt(udtsocket, udt4.UDP_SNDBUF)
    block = "-"
    blockRead = 0
    while (blockRead < fsize):
        # read() reads at most size bytes from the file. 
        # If the read hits EOF before obtaining size bytes, then it reads only available bytes.
        block = fdIn.read(blockSize)
        blocklen = len(block)
        blockRead += blocklen
        if (blocklen > 0):
            ret = reliableUDTSend(udtsocket, block)
            if (ret < 0):
                print "Error in sending UDT data: %s" % udt4.getlasterror().getErrorMessage()
                return -1
    
    fdIn.close()
    return 0


def getAuthHttpHdrVal(user, pwd):
        """
        Generate the value to be sent with the HTTP Autorization Header.
        If no specific user is given, an arbitrary user is chosen.

        user:        Name of registered user (string|None).

        Returns:     Authorization HTTP Header value (string).
        """
        authHdrVal = "Basic " + base64.encodestring(user + ":" + pwd)
        return authHdrVal

def buildHTTPHeader(path, mime_type, file_size, authHdrVal, contentDisp):
    auth_hdr = "\015\012Authorization: %s" % authHdrVal
    
    ngamsUSER_AGENT = "NG/AMS UDT-PClient"
    
    header_format = "POST %s HTTP/1.0\015\012" +\
             "User-agent: %s\015\012" +\
             "Content-Length: %d\015\012" +\
             "Content-Type: %s\015\012" +\
             "Content-Disposition: %s%s\015\012\012"
    
    header = header_format % (path, ngamsUSER_AGENT, file_size, mime_type, contentDisp, auth_hdr)
    return header

def reliableUDTSend(udtsocket, buf):
    tosend = len(buf)
    sent = 0
    ret = 0
    while (tosend):
        ret = udt4.send(udtsocket, buf[sent:], tosend)
        if (ret <= 0):
            return -1
        
        sent += ret
        tosend -= ret
    
    if (sent != len(buf)):
        print "reliable UDT send failed. Sent %d bytes out of %d bytes" % (sent, len(buf))
    return sent

def reliableUDTRecv(udtsocket, buff_len):
    """
    udtsocket     UDT Socket
    buff_len      int
    Return        buffer read (String)
    """
    buf_list = [] 
    read = 0
    toread = buff_len
    ret = 0

    while (toread):
        tmp_buff = None
        try:
            tmp_buff = udt4.recv(udtsocket, toread)
        except Exception, eee:
            print "%s Exception in reading data from UDT socket" % str(udtsocket.UDTSOCKET)
            raise eee
            
        if (not tmp_buff):
            return -1
        buf_list.append(tmp_buff)
        ret = len(tmp_buff)
        read += ret
        toread -= ret
    
    return ''.join(buf_list)

class HTTPHeader:
    
    def __init__(self):
        self.status = 0
        self.vals = {}
        
    def getVal(self, key):
        if (self.vals.has_key(key)):
            return self.vals[key]
        else:
            return None
    
    def putVal(self, key, val):
        self.vals[key] = val
        
class HTTPPayload:
    
    def __init__(self):
        self.payloadsize = 0
        self.buff = None

def readLine(udtsocket):

    line = []
    while (1):
        c = reliableUDTRecv(udtsocket, 1)
        if (not c):
            return None

        if (c =='\n'):
            return ''.join(line)

        line.append(c)

        if (len(line) == MAX_LINE_LEN):
            return ''.join(line)
        
def readHTTPHeader(udtsocket, hdr): 

    first = True
    while (1):
        line = readLine(udtsocket)
        line_len = len(line)
        if (not line):
            print "readline error" 
            return -1

        if (line_len == MAX_LINE_LEN):
            print "max line reached" 
            return -1

        # Check if max number of HTTP lines reached
        if (len(hdr.vals) >= MAXELEMENTS):
            print "max number of http elements reached" 
            return -1

        # the end of http header
        if (line_len == 0):
            #print "end of header" 
            return 0

        # end of line but there is a single carridge return
        # http header ends with an empty line i.e \r\n
        if (line_len == 1 and line[0] == '\r'):
            #print "end of header" 
            return 0

        # read the status http header
        if (first):
            hdr.status = line
            print hdr.status 
            first = False
        else:
            # strip carridge return from end of string if it exists
            if (line[line_len - 1] == '\r'):
                #print "carridge return found at end of string" 
                line = line[0:line_len - 1]

            #split string into key:value
            found = line.find(":")
            if (found != -1):
                key_val = line.split(":")
                key = key_val[0]
                value = key_val[1]
                if (len(key) > 0):
                    # convert key to lowercase
                    # store key:value pair in a collection map
                    hdr.putVal(key.lower(), value)
                    # cout << key << " " << value 
        #end else
    #end while

    return 0

def readHTTPPacket(udtsocket, hdr, payload):
    # read the NGAS response header
    ret = readHTTPHeader(udtsocket, hdr)
    if (ret < 0):
        print "invalid HTTP header" 
        return -1
    

    # get the payload size from the HTTP header
    content = "content-length"
    if (not hdr.getVal(content)):
        print "content-length in HTTP header does not exist"
        return -1
    

    # convert filesize from string to int64
    contentsize = long(hdr.getVal(content)) 
    if (contentsize == 0):
        print "error parsing content-length"
        return -1
    

    payload.payloadsize = contentsize
    payload.buff = reliableUDTRecv(udtsocket, contentsize)
    if (not payload.buff):
        print "error reading http payload"
        payload.buff = None
        return -1

    return 0


def main():
    
    """
    This function is for testing only 
    """   
    
    if len(sys.argv) != 3:
        print "Usage: python ngamsUDTSender.py <file_full_path> <url>"
        print "Example: python ngamsUDTSender.py ~/Documents/10.1.1.137.1762.pdf houdt://127.0.0.1:9000/QARCHIVE"
        exit(1)
    
    file = sys.argv[1]

    if (not os.path.exists(file)):
        print ('Failed to locate file %s' % file)
        return 2
    
    #auth_hrd_val = getAuthHttpHdrVal('ngasmgr', 'ngas$dba') # this is to simulate the config function used by ngamsSusbscriptionThread
    auth_hrd_val = getAuthHttpHdrVal('ngasmgr', 'ngasmgr') # this is to simulate the config function used by ngamsSusbscriptionThread
    url = sys.argv[2]
    fileMimeType = 'application/octet-stream'
    basename = os.path.basename(file)
    contDisp = "attachment; filename=\"%s\"; no_versioning=1" % basename
    try:
        reply, msg, hdrs, data = \
               httpPostUrl(url, fileMimeType,
                                    contDisp, file, "FILE",
                                    blockSize=\
                                    65536,
                                    suspTime = 0,
                                    authHdrVal = auth_hrd_val)
        
        if (data):
            print "\nhttp return code = %d, payload: \n%s\n" % (reply, data)
            
    except Exception, err:
        ex = str(err)
        if ((ex != "") or (reply != 200)):
            print "Error %s occurred while delivering file: %s" % (ex, basename)
            print traceback.format_exc()
    
def httpPostUrl(url,
                mimeType,
                contDisp = "",
                dataRef = "",
                dataSource = "BUFFER",
                dataTargFile = "",
                blockSize = 65536,
                suspTime = 0.0,
                timeOut = None,
                authHdrVal = "",
                dataSize = -1):
    """
    This funciton is the UDT version of the same function 
    in src/ngamsLib/ngmasLib.py
    
    Eventually, these two functions will be merged in the near future, i.e. in the year 2030~2032
    
    Post the the data referenced on the given URL.

    The data send back from the remote server + the HTTP header information
    is return in a list with the following contents:

      [<HTTP status code>, <HTTP status msg>, <HTTP headers (list)>, <data>]

    url:          URL to where data is posted (string).

    mimeType:     Mime-type of message (string).

    contDisp:     Content-Disposition of the data (string).

    dataRef:      Data to post or name of file containing data to send
                  (string).

    dataSource:   Source where to pick up the data (string/BUFFER|FILE|FD).

    dataTargFile: If a filename is specified with this parameter, the
                  data received is stored into a file of that name (string).

    blockSize:    Block size (in bytes) used when sending the data (integer).

    suspTime:     Time in seconds to suspend between each block (double).

    timeOut:      Timeout in seconds to wait for replies from the server
                  (double).

    authHdrVal:   Authorization HTTP header value as it should be sent in
                  the query (string).

    dataSize:     Size of data to send if read from a socket (integer).

    Returns:      List with information from reply from contacted
                  NG/AMS Server (reply, msg, hdrs, data) (list).
    """
    
    #from ngams import *
    
    # Separate the URL from the command.
    # an example of url: - houdt://eor-08.mit.edu:7790/QARCHIVE
    #HoUDT - HTTP Over UDT
    if (dataSource.upper().strip() != "FILE"):
        raise Exception('currently only support send files via HTTP Over UDT (socket)')
    
    urlres = urlparse.urlparse(url)
    if (urlres.scheme.lower() != 'houdt'):
        raise Exception('The protocol scheme %s is not supported by UDT-based HTTP' % urlres.scheme)
    host = urlres.hostname
    port = urlres.port
    path = urlparse.urlparse(url).path # e.g. /QARCHIVE
    socket = create_socket(host, port, blockSize, timeOut)
    if not socket:
        raise Exception('failed to create the udt socket')    
    
    try:
        httpHdr = buildHTTPHeader(path, mimeType, os.path.getsize(dataRef), authHdrVal, contDisp)
        #print '\n\n%s' % httpHdr
        #info(4,"Sending HTTP header ...")
        print "%s Sending HTTP header" % str(socket.UDTSOCKET)
        ret = reliableUDTSend(socket, httpHdr)
        if (-1 == ret):
            raise Exception('failed to send HTTP header')
        print "%s HTTP header sent" % str(socket.UDTSOCKET)
        #info(4,"HTTP header sent")   
        #info(4,"Sending data ...")
        print "%s Sending HTTP data" % str(socket.UDTSOCKET)
        ret = send_file(socket, dataRef)
        print "%s HTTP data sent" % str(socket.UDTSOCKET)
        if (-1 == ret):
            raise Exception('failed to send the file')
        
        respHdr = HTTPHeader()
        respPay = HTTPPayload()
        print "%s Reading HTTP header from UDT" % str(socket.UDTSOCKET)
        status = readHTTPPacket(socket, respHdr, respPay);
        print "%s HTTP header from UDT read" % str(socket.UDTSOCKET)
        #reply = respHdr.getVal(key)
        if (status == 0):
            # something like HTTP/1.0 200 OK (see http://www.w3.org/Protocols/rfc2616/rfc2616-sec6.html)
            reply = int(respHdr.status.split(' ')[1])
            #print respPay.buff
        else:
            raise Exception('error getting response')
    finally:
        if (socket):
            udt4.close(socket)
            del socket
    
    return [reply, None, None, respPay.buff]

if __name__ == '__main__':
    main()

