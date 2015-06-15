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
#*************************************************************************

"""
To see how HttPostUrl works in the ngamsLib
"""

from ngams import *
import ngamsPlugInApi, ngamsStatus
import ngamsLib

import httplib, socket, time, sys, os, traceback

log_level = 3


mimeType = 'application/octet-stream'

def printinfo(level, msg):
    if (log_level >= level):
        print msg

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
    Post the the data referenced on the given URL.

    The data send back from the remote server + the HTTP header information
    is return in a list with the following contents:

      [<HTTP status code>, <HTTP status msg>, <HTTP headers (list)>, <data>]

    url:          URL to where data is posted (string).
    
    mimeType:     Mime-type of message (string).

    contDisp:     Content-disposition of the data (string).
    
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
    # Separate the URL from the command.
    idx = (url[7:].find("/") + 7)
    tmpUrl = url[7:idx]
    cmd    = url[(idx + 1):]
    http = httplib.HTTP(tmpUrl)
    printinfo(4,"Sending HTTP header ...")
    printinfo(4,"HTTP Header: %s: %s" % (NGAMS_HTTP_POST, cmd))
    http.putrequest(NGAMS_HTTP_POST, cmd)
    printinfo(4,"HTTP Header: %s: %s" % ("Content-type", mimeType))
    http.putheader("Content-type", mimeType)
    if (contDisp != ""):
        printinfo(4,"HTTP Header: %s: %s" % ("Content-disposition", contDisp))
        http.putheader("Content-disposition", contDisp)
    if (authHdrVal):
        if (authHdrVal[-1] == "\n"): authHdrVal = authHdrVal[:-1]
        printinfo(4,"HTTP Header: %s: %s" % ("Authorization", authHdrVal))
        http.putheader("Authorization", authHdrVal)
    if (dataSource == "FILE"):
        dataSize = ngamsPlugInApi.getFileSize(dataRef)
    elif (dataSource == "BUFFER"):
        dataSize = len(dataRef)

    if (dataSize != -1):
        printinfo(4,"HTTP Header: %s: %s" % ("Content-length", str(dataSize)))
        http.putheader("Content-length", str(dataSize))
    printinfo(4,"HTTP Header: %s: %s" % ("Host", getHostName()))
    http.putheader("Host", getHostName())
    http.endheaders()
    printinfo(4,"HTTP header sent")

    # Send the data.
    printinfo(4,"Sending data ...")
    if (dataSource == "FILE"):
        fdIn = open(dataRef)
        block = "-"
        blockAccu = 0
        loc_timeout = http._conn.sock.gettimeout()
        g_timeout = socket.getdefaulttimeout()
        printinfo(3, 'Current socket timeout = ' + str(loc_timeout))
        printinfo(3, 'Default socket timeout = ' + str(g_timeout))
        #http._conn.sock.setblocking(1)
        while (block != ""):
            block = fdIn.read(blockSize)
            blockAccu += len(block)
            http._conn.sock.sendall(block)
            if (suspTime > 0.0): time.sleep(suspTime)
        fdIn.close()
    elif (dataSource == "FD"):
        fdIn = dataRef
        dataRead = 0
        while (dataRead < dataSize):
            if ((dataSize - dataRead) < blockSize):
                rdSize = (dataSize - dataRead)
            else:
                rdSize = blockSize
            block = fdIn.read(rdSize)
            http._conn.sock.sendall(block)
            dataRead += len(block)
            if (suspTime > 0.0): time.sleep(suspTime)
    else:
        # dataSource == "BUFFER"
        http.send(dataRef)
    printinfo(4,"Data sent")

    # Receive + unpack reply.
    printinfo(4,"Waiting for reply ...")
    ngamsLib._setSocketTimeout(timeOut, http)
    reply, msg, hdrs = http.getreply()

    if (hdrs == None):
        errMsg = "Illegal/no response to HTTP request encountered!"
        raise Exception, errMsg
    
    if (hdrs.has_key("content-length")):
        dataSize = int(hdrs["content-length"])
    else:
        dataSize = 0
    if (dataTargFile == ""):
        ngamsLib._waitForResp(http.getfile(), timeOut)
        data = http.getfile().read(dataSize)
    else:
        fd = None
        try:
            data = dataTargFile
            fd = open(dataTargFile, "w")
            ngamsLib._waitForResp(http.getfile(), timeOut)
            fd.write(http.getfile().read(dataSize))
            fd.close()
        except Exception, e:
            if (fd != None): fd.close()
            raise e

    # Dump HTTP headers if Verbose Level >= 4.
    printinfo(4,"HTTP Header: HTTP/1.0 " + str(reply) + " " + msg)
    for hdr in hdrs.keys():
        printinfo(4,"HTTP Header: " + hdr + ": " + hdrs[hdr])
        
    if (http != None):
        http.close()
        del http    

    return [reply, msg, hdrs, data]

if __name__ == '__main__':
    if (len(sys.argv) < 2):
        print 'Usage: python ngamsTestHttpPostUrl.py <file_name> [target_url]'
        print 'e.g. python ngamsTestHttpPostUrl.py /home/chenwu/MWA_HSM/NGAS_MWA_RUNTIME/log/old_logs/LOG-ROTATE-2012-12-01T14:47:51.691.nglog'
        sys.exit(1)
    filename = sys.argv[1]
    if (len(sys.argv) == 3):
        targetUrl = sys.argv[2]
    else:
        targetUrl = 'http://eor-04.mit.edu:7777/QARCHIVE'
    baseName = os.path.basename(filename)
    contDisp = "attachment; filename=\"" + baseName + "\""
    contDisp += "; no_versioning=1"
    stat = ngamsStatus.ngamsStatus()
    try:
        reply, msg, hdrs, data = \
               httpPostUrl(targetUrl, mimeType,
                                    contDisp, filename, "FILE",
                                    65536)
        if (data.strip() != ""):
            stat.clear().unpackXmlDoc(data)
        else:
            # TODO: For the moment assume success in case no
            #       exception was thrown.
            stat.clear().setStatus(NGAMS_SUCCESS)
    except Exception, e:
        ex = str(e)
        print ex
        print ''
        traceback.print_exc(file=sys.stdout)