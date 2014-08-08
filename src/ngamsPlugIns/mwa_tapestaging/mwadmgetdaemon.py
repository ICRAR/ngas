#!/usr/bin/env python

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
# Who       When        What
# --------  ----------  -------------------------------------------------------
# dpallot  8/08/2014  Created

import SocketServer, socket, struct, select, os, time, sys
import psycopg2, psycopg2.pool
import threading, subprocess, signal
import logging, logging.handlers

server = None

if not os.path.exists('log/'):
   os.makedirs('log/')

#f = open(os.devnull, 'w')
#sys.stdout = f
#sys.stderr = f

logger = logging.getLogger('mwadmget')
logger.setLevel(logging.DEBUG)
logger.propagate = False
rot = logging.handlers.RotatingFileHandler('log/mwadmget.log', maxBytes=33554432)
rot.setLevel(logging.DEBUG)
rot.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
logger.addHandler(rot)
   

class ErrorCode():
   socket_timeout_error = 1
   io_error = 2
   protocol_error = 3
   not_an_mwa_file_error = 4
   files_not_found_error = 5
   database_error = 6
   command_error = 7
   unknown_error = 8
   connection_error = 9
   invalid_args_error = 10


class ErrorCodeException(Exception):
   def __init__(self, err, msg):
      self.error_code = err
      self.msg = msg
   
   def getMsg(self):
      return self.msg
   
   def __str__(self):
      return repr(self.error_code)


class Command(object):
   def __init__(self, cmd):
      self.cmd = cmd
      self.process = None
      self.excp = None
      
      def __execute():
         try:
            # TO DO: Put timing code around this
            self.process = subprocess.Popen(self.cmd)
            self.process.communicate()
         except Exception as e:
            self.excp = e
      
      self.thread = threading.Thread(target=__execute)
      self.thread.start()


   def join(self, timeout = None):
      if timeout:
         self.thread.join(timeout)
         if self.thread.is_alive():
            self.process.terminate()
            raise TimeoutError()
      else:
         self.thread.join()
      
      if self.process:
         return self.process.returncode
      else:
         raise ErrorCodeException(ErrorCode.command_error, str(self.excp))
      

class mwadmgetServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
   
   dbp = None
   lock = threading.Lock()
   staging = {}
   
   def __init__(self, *args, **kwargs):

      self.dbp = psycopg2.pool.ThreadedConnectionPool(minconn=2, \
                                                    maxconn=20, \
                                                    host="mwa-pawsey-db01.pawsey.ivec.org", \
                                                    user="ngas_ro", \
                                                    database="ngas", \
                                                    password="ngas$ro", \
                                                    port=5432)
      
      self.allow_reuse_address = True
      SocketServer.TCPServer.__init__(self, *args, **kwargs)
   

   def queryFiles(self, obsid):
      
      files = []
      con = None
      cursor = None
   
      try:
         con = self.dbp.getconn()
         cursor = con.cursor()
         cursor.execute("select mount_point || '/' || file_name as path from ngas_files \
                        inner join ngas_disks on ngas_disks.disk_id = ngas_files.disk_id where file_id like %s \
                        and ngas_disks.disk_id in \
                        ('35ecaa0a7c65795635087af61c3ce903', '54ab8af6c805f956c804ee1e4de92ca4', \
                        '921d259d7bc2a0ae7d9a532bccd049c7', 'e3d87c5bc9fa1f17a84491d03b732afd')", [str(obsid) + '%'])
         
         row = cursor.fetchall()
         for r in row:
            files.append(r[0])
         
         return files
      
      except BaseException as e:
         raise ErrorCodeException(ErrorCode.database_error, str(e))
      
      finally:
         if cursor:
            cursor.close()
         
         if con:
            self.dbp.putconn(conn=con)
            
   
   
   def stageObservation(self, obsid):
      
      files = None
      command = None
      
      
      try:
         # if we are already staging all the files for this obsid then just return
         with self.lock:
            if not obsid in self.staging:
               
               files = self.queryFiles(obsid)
               if len(files) <= 0:
                  raise ErrorCodeException(ErrorCode.files_not_found_error, 'could not find any files for %s' % (str(obsid)))
               
               commandlist = ['dmget'] + files
               #commandlist = ['sleep', '10']
               
               logger.info("%s staging files" % (str(obsid)))
               
               command = Command(commandlist)
               
               self.staging[obsid] = (files, command)
               
            else:
               (files, command) = self.staging.get(obsid)
               logger.info("%s files already in the process of being staged" % (str(obsid)))
               
         # wait for staging to complete before returning to user
         return_code = command.join()
         if return_code != 0:
            raise ErrorCodeException(ErrorCode.command_error, "dmget exited with %s" % str(return_code))
      
      finally:
         # once it is complete remove the observation and all associated files
         with self.lock:
            if obsid in self.staging:
               self.staging.pop(obsid)

            
   def handleFile(self, filename):
      
      return_code = 0
      singleStage = False
      
      try:
         # parse out obs id if it exists
         filepart = os.path.basename(filename)
         obsid = int(filepart.split('_', 1)[0])
         self.stageObservation(obsid)
         
      except ValueError as ve:
         singleStage = True
         
      except ErrorCodeException as ere:
         if ere.error_code == ErrorCode.files_not_found_error:
            singleStage = True
         else:
            raise ere
            
      if singleStage:
         # if it is not an mwa file then just stage whatever it is
         command = Command(['dmget', filename])
         return_code = command.join()
      
      return return_code
      #raise ErrorCodeException(ErrorCode.not_an_mwa_file_error, "Unable to parse out obsID")


class mwadmgetHandler(SocketServer.BaseRequestHandler):

   def readPacket(self, size, timeout = None):
      try:
         buffer = ""
         bytesread = 0
   
         if timeout:
            self.request.setblocking(0)
      
         while bytesread < size:
            ready = select.select([self.request], [], [], 10)
            if not ready[0]:
               raise ErrorCodeException(ErrorCode.socket_timeout_error, "socket timeout")
            
            data = self.request.recv(size - bytesread)
            if data:
               buffer += data
               bytesread += len(data)
            else:
               break
   
         if (bytesread != size):
            raise ErrorCodeException(ErrorCode.protocol_error, "bytes expected != bytes received")
         
         return buffer
      
      except IOError as ioe:
         raise ErrorCodeException(ErrorCode.io_error, str(ioe))


   def readFilename(self):
      # read header as 16-bit value
      header = self.readPacket(2)
      
      # convert to number
      headerval = struct.unpack('!H', header)[0]
      
      # header size exceeded
      if headerval > 65536:
         raise ErrorCodeException(ErrorCode.protocol_error, "header size is too big")
      
      return self.readPacket(headerval)


   def handle(self):
      
      logger.info("[%s] connected" % str(self.client_address[0]))
      
      return_code = 0
      
      try:
         # get the filename from the client, this will be a full path
         filename = self.readFilename()
         return_code = self.server.handleFile(filename)
         
      except ErrorCodeException as ee:
         logger.error("[%s] errorcode: %s message: %s" % (str(self.client_address[0]), str(ee), ee.getMsg()))
         return_code = ee.error_code
         
      # catch unknown exceptions
      except Exception as be:
         logger.error("[%s] %s" % (str(self.client_address[0]), str(be)) )
         return_code = ErrorCode.unknown_error
      
      try:
         self.request.sendall(struct.pack('>H', return_code))
         
      except BaseException as basee:
         logger.error("[%s] %s" % (str(self.client_address[0]), str(basee) ))
         
      finally:
         self.request.close()
         logger.info("[%s] disconnected" % str(self.client_address[0]))


   def finish(self):
      pass
      
      
def signalHandler(signal, frame):  
   logger.info("Shutting down mwadmgetDaemon")
   if server:
      server.shutdown()
         
   sys.exit(0)

def main():
   
   try:
      logger.info("Starting mwadmgetDaemon")
      
      HOST, PORT = "", 9898
      server = mwadmgetServer((HOST, PORT), mwadmgetHandler)
      
      signal.signal(signal.SIGINT, signalHandler)
      
      server.serve_forever()
      
   finally:
      if server:
         server.shutdown()

if __name__ == "__main__":
   main()
  