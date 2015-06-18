#!/bin/env python

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
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# dpallot  23/01/2015  Created
#

import os, socket, sys
import httplib
import base64

class NGASHttpArchive(object):
   
   def __init__(self, url, command, username, password, mimetype):
      self.url = url
      self.command = command
      self.username = username
      self.password = password
      self.mimetype = mimetype

   """
     Transfer a file using HTTP to an NGAS instance
     
     fullpath: full path including filename
     
     Returns: status, reason, NGAS XML response packet
   """
   def transferFile(self, fullpath):

      filename = os.path.basename(fullpath)
      if not filename:
         raise Exception('could not extract basename from %s' % fullpath)
      
      filesize = os.stat(fullpath).st_size   
   
      file = None
      conn = None
      
      try:
         conn = httplib.HTTPConnection(self.url)
         
         conn.putrequest("POST", self.command)
         
         base64string = base64.encodestring('%s:%s' % (self.username, self.password)).replace('\n', '')
         conn.putheader("Authorization", "Basic %s" % base64string)
         conn.putheader("Content-disposition", "attachment; filename=%s" % filename)
         conn.putheader("Content-length", filesize)
         conn.putheader("Host", socket.gethostname())
         conn.putheader("Content-type", self.mimetype)
         conn.endheaders()
         
         blocksize = 65536
   
         file = open(fullpath, "rb")
         
         # send data
         sent = 0
         while True:
            # read to EOF
            databuff = file.read(blocksize)
            if databuff:
               # send all data out
               conn.sock.sendall(databuff)
               sent += len(databuff)
            else:
               break
      
         if sent != filesize:
            raise Exception("data sent does not match filesize: %s %s" % (str(sent), str(filesize)))
            
         # read the response
         resp = conn.getresponse()
         data = ''
         while True:
            buff = resp.read()
            if buff:
               data += buff
            else:
               break
            
         return resp.status, resp.reason, data
      
      finally:
         if file:
            file.close()
            
         if conn:
            conn.close()

