#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
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

#******************************************************************************
#
# "@(#) $Id: ngamsLib.py,v 1.13 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu       02/10/2013  Created
#

"""
Python-based UDT sender based on PyUDT
"""
import time, os, base64, urllib2
import socket as socklib
import udt4 

print('__load__client')

settings = {
        'host'  :   '127.0.0.1',
        'port'  :   9000
        }


def create_socket(host, port):
    print('create_client(%s, %s)' % (host, port))


    socket = udt4.socket(
            socklib.AF_INET, socklib.SOCK_STREAM, socklib.AI_PASSIVE
            ) 
    
    #
    # set sock options 
    #
    opts = [ (udt4.UDP_SNDBUF, 64000),
             (udt4.UDP_RCVBUF, 64000)
             ]
    
    for opt in opts:
        udt4.setsockopt(socket, opt[0], opt[1]) 
#    print "UDP_SNDBUF = %d" % udt4.getsockopt(socket, udt4.UDP_SNDBUF)
#    print "UDP_RCVBUF = %d" % udt4.getsockopt(socket, udt4.UDP_RCVBUF)
#    
#    
#    udt4.setsockopt(socket, udt4.UDP_SNDBUF,640000)
#    udt4.setsockopt(socket, udt4.UDP_RCVBUF,640000)
#    
    print "UDP_SNDBUF = %d" % udt4.getsockopt(socket, udt4.UDP_SNDBUF)
    print "UDP_RCVBUF = %d" % udt4.getsockopt(socket, udt4.UDP_RCVBUF)
    
#    udt4.setsockopt(socket, udt4.UDT_SNDBUF,64)
#    udt4.setsockopt(socket, udt4.UDT_RCVBUF,64)
   
    print('connecting client')
    try:
        udt4.connect(socket, host, port)
    except Exception as err:
        print('Exception: %s' % err)
        return 0
   
    return socket


def send_file(udtsocket, file_path):
    print('send_file...')
        
    fdIn = open(file_path, 'r')
    fsize = os.stat(file_path).st_size
    print "File size = %d" % fsize
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
    
    fdIn.close()


def getAuthHttpHdrVal(user, pwd):
        """
        Generate the value to be sent with the HTTP Autorization Header.
        If no specific user is given, an arbitrary user is chosen.

        user:        Name of registered user (string|None).

        Returns:     Authorization HTTP Header value (string).
        """
        authHdrVal = "Basic " + base64.encodestring(user + ":" + pwd)
        return authHdrVal

def buildHTTPHeader(path, mime_type, file_name, file_size, authHdrVal):
    auth_hdr = "\015\012Authorization: %s" % authHdrVal
    contentDisp = "attachment; filename=\"%s\"; no_versioning=1" % file_name
    ngamsUSER_AGENT = "NG/AMS UDT-PClient"
    
    header_format = "POST /%s HTTP/1.0\015\012" +\
             "User-agent: %s\015\012" +\
             "Content-length: %d\015\012" +\
             "Content-type: %s\015\012" +\
             "Content-disposition: %s%s\015\012\012"
    
    header = header_format % (path, ngamsUSER_AGENT, file_size, mime_type, contentDisp, auth_hdr)
    return header

def reliableUDTSend(udtsocket, buf):
    tosend = len(buf)
    sent = 0
    ret = 0
    while (tosend):
        ret = udt4.send(udtsocket, buf[sent:], tosend)
        if (not ret):
            return -1
        
        sent += ret
        tosend -= ret
    
    if (sent != len(buf)):
        print "reliable UDT send failed. Sent %d bytes out of %d bytes" % (sent, len(buf))
    return sent

def main():
    udt4.startup() 
    
    #time.sleep(1) # just to wait for server
    socket = create_socket(settings['host'], settings['port'])
    if not socket:
        print('failed to create socket')
        return 1    
    file = '/home/chen/Downloads/128T_05.fits'
    #file = '/home/chen/Downloads/zzz.fits'
    #file = '/home/chen/Downloads/boost_1_47_0.tar'
    #file = '/home/chen/Documents/10.1.1.137.1762.pdf'
    if (not os.path.exists(file)):
        print ('Failed to locate file %s' % file)
        return 2
    
    auth_hrd_val = getAuthHttpHdrVal('ngasmgr', 'ngasmgr') # this is to simulate the config function used by ngamsSusbscriptionThread
    httpHdr = buildHTTPHeader('QARCHIVE', 'application/octet-stream', os.path.basename(file), os.stat(file).st_size, auth_hrd_val)
    ret = reliableUDTSend(socket, httpHdr)
    if (-1 == ret):
        print "Failed to send HTTP header \n%s" % httpHdr
        return 3
    else:
        print "Successfully sent the HTTP header\n %s" % httpHdr
    send_file(socket, file)
    print "Sleep 10 seconds for now"
    time.sleep(10);
    udt4.close(socket)

if __name__ == '__main__':
    main()

