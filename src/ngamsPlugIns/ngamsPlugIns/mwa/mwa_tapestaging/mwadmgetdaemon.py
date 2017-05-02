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

import SocketServer, socket, struct, select, os, time, sys, json, binascii
import psycopg2, psycopg2.pool
import threading, subprocess, signal
import logging, logging.handlers


APP_PATH = os.path.dirname(os.path.realpath(__file__))
path = APP_PATH + '/log/'
sys.path.append(APP_PATH)

if not os.path.exists(path):
   os.makedirs(path)

logger = logging.getLogger('mwadmget')
logger.setLevel(logging.DEBUG)
logger.propagate = False
rot = logging.FileHandler(path + 'mwadmget.log')
rot.setLevel(logging.DEBUG)
rot.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
logger.addHandler(rot)

server = None

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
   file_limit_exceeded = 11


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
      self.output = ""
      self.error = ""
      self.timing = 0

      def __execute():
         try:
            t0 = time.time()
            self.process = subprocess.Popen(self.cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            self.output, self.error = self.process.communicate()
            t1 = time.time()
            self.timing = t1-t0
         except Exception as s:
            self.error = str(s)

      self.thread = threading.Thread(target=__execute)
      self.thread.start()


   def join(self, timeout = None):
      if timeout:
         self.thread.join(timeout)
         if self.thread.is_alive():

            if self.process is None:
               raise Exception(self.error)

            self.process.terminate()
            raise TimeoutError()
      else:
         self.thread.join()

      if self.process is None:
         raise Exception(self.error)

      return self.process.returncode


class mwadmgetServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):

   dbp = None
   lock = threading.Lock()
   staging = {}

   mlock = threading.Lock()
   mstaging = {}

   def __init__(self, *args, **kwargs):

      self.dbp = psycopg2.pool.ThreadedConnectionPool(minconn=2,
                                                    maxconn=20,
                                                    host="",
                                                    user="",
                                                    database="",
                                                    password="",
                                                    port=5432)

      self.allow_reuse_address = True
      SocketServer.TCPServer.__init__(self, *args, **kwargs)


   def fillPath(self, filenames):
      fullfiles = []
      query = []

      # check to see if the file has a full path
      for f in filenames:
         query.append(os.path.basename(f))

      con = None
      cursor = None

      try:
         con = self.dbp.getconn()
         cursor = con.cursor()
         cursor.execute("select mount_point || '/' || file_name as path from ngas_files \
                        inner join ngas_disks on ngas_disks.disk_id = ngas_files.disk_id \
                        where file_id in %s and ngas_disks.disk_id in \
                        ('35ecaa0a7c65795635087af61c3ce903', '54ab8af6c805f956c804ee1e4de92ca4', \
                        '921d259d7bc2a0ae7d9a532bccd049c7', 'e3d87c5bc9fa1f17a84491d03b732afd', \
                        '848575aeeb7a8a6b5579069f2b72282c')", [tuple(query)])

         #print cursor.query
         row = cursor.fetchall()
         for r in row:
            fullfiles.append(r[0])

         if len(row) < len(filenames):
            raise ErrorCodeException(ErrorCode.invalid_args_error,
                                    "fillPath: Request files are not part of NGAS. \
                                    Queried: %s Requested: %s" \
                                    % (len(row), len(filenames)))

         return fullfiles

      except ErrorCodeException as ece:
         raise ece

      except BaseException as e:
         raise ErrorCodeException(ErrorCode.database_error, str(e))

      finally:
         if cursor:
            cursor.close()

         if con:
            self.dbp.putconn(conn=con)


   def queryFiles(self, obsid):

      files = []
      con = None
      cursor = None

      try:
         con = self.dbp.getconn()
         cursor = con.cursor()
         cursor.execute("select mount_point || '/' || file_name as path from ngas_files \
                        inner join ngas_disks on ngas_disks.disk_id = ngas_files.disk_id \
                        where file_id like %s and ngas_disks.disk_id in \
                        ('35ecaa0a7c65795635087af61c3ce903', '54ab8af6c805f956c804ee1e4de92ca4', \
                        '921d259d7bc2a0ae7d9a532bccd049c7', 'e3d87c5bc9fa1f17a84491d03b732afd')",
                        [str(obsid) + '%fits'])

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


   def stageMultipleFiles(self, filenames):

      # get unique hash for this set
      fhash = 0
      for f in filenames:
         fhash = binascii.crc32(f, fhash)

      command = None
      originator = False

      try:
         with self.mlock:
            if not fhash in self.mstaging:

               fullfilenames = self.fillPath(filenames)
               commandlist = ['dmget', '-a'] + fullfilenames
               logger.info("stageMultipleFiles staging: File hash: %s Staging: %s files."
                              % (str(fhash), len(fullfilenames)))

               command = Command(commandlist)

               self.mstaging[fhash] = command

               originator = True

            else:
                command = self.mstaging.get(fhash)
                logger.info("stageMultipleFiles staging: File hash: %s already being staged."
                              % str(fhash))


         return_code = command.join()
         if return_code != 0:
            raise ErrorCodeException(ErrorCode.command_error,
                                    "stageMultipleFiles error: File Hash: %s dmget error: %s message: %s"
                                    % (str(fhash), return_code, command.error))

         if originator:
            logger.info("stageMultipleFiles finished staging. File hash: %s Staging time: %.2f secs"
                        % (str(fhash), round(command.timing, 2)))

      finally:
         # once it is complete remove the hash and its dmget command
         with self.mlock:
            if fhash in self.mstaging:
               self.mstaging.pop(fhash)


   def stageObservation(self, obsid):

      files = None
      command = None
      originator = False

      try:
         # if we are already staging all the files for this obsid then just wait for the dmget to return
         with self.lock:
            if not obsid in self.staging:

               files = self.queryFiles(obsid)
               if len(files) <= 0:
                  raise ErrorCodeException(ErrorCode.files_not_found_error,
                                          'stageObservation could not find files: ObsID %s'
                                          % obsid)

               commandlist = ['dmget', '-a'] + files
               logger.info("stageObservation staging. ObsID: %s" % obsid)

               command = Command(commandlist)

               self.staging[obsid] = (files, command)

               originator = True

            else:
               (files, command) = self.staging.get(obsid)
               logger.info("stageObservation staging. ObsID: %s already being staged." % obsid)

         # wait for staging to complete before returning to user
         return_code = command.join()
         if return_code != 0:
            raise ErrorCodeException(ErrorCode.command_error,
                                    "stageObservation error: ObsID: %s dmget error: %s message: %s"
                                    % (obsid, return_code, command.error))

         if originator:
            logger.info("stageObservation finished staging. ObsID: %s Staging time: %.2f secs"
                        % (obsid, round(command.timing, 2)))

      finally:
         # once it is complete remove the observation and all associated files
         with self.lock:
            if obsid in self.staging:
               self.staging.pop(obsid)


   def handleFiles(self, filenames):

      return_code = 0
      singleStage = False

      # if we get a single file and its a fits file then bulk stage the whole observation
      # if its a voltage file, zip file or something else then single stage file
      if len(filenames) == 1:

         try:
            filepart = os.path.basename(filenames[0])

            # we want to ignore bulk staging for voltage data
            if '.dat' in filepart:
               singleStage = True

            # single stage any archive zip files
            elif '.zip' in filepart:
               singleStage = True

            else:
               # parse out obs id if it exists; if it does not exist then its
               # not an MWA visiblity file, so just single stage
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
            logger.info("singleStage: Staging single file: %s" % filenames[0])
            # if it is not an mwa file then just stage whatever it is
            command = Command(['dmget', '-a', filenames[0]])
            return_code = command.join()
            if return_code != 0:
               raise ErrorCodeException(ErrorCode.command_error,
                                    "singleStage error: File: %s dmget error: %s message: %s"
                                    % (filenames[0], return_code, command.error))

            logger.info("singleStage finished staging. File: %s Staging time: %.2f secs"
                        % (filenames[0], round(command.timing, 2)))

      else:
         # if we get a list of files then just stage the whole lot
         self.stageMultipleFiles(filenames)

      return return_code


class mwadmgetHandler(SocketServer.BaseRequestHandler):

   def readPacket(self, size, timeout = None):
      try:
         buffer = []
         bytesread = 0

         if timeout:
            self.request.setblocking(0)

         while bytesread < size:
            ready = select.select([self.request], [], [], 10)
            if not ready[0]:
               raise ErrorCodeException(ErrorCode.socket_timeout_error, "socket timeout")

            data = self.request.recv(size - bytesread)
            if data:
               buffer.append(data)
               bytesread += len(data)
            else:
               break

         if (bytesread != size):
            raise ErrorCodeException(ErrorCode.protocol_error, "bytes expected != bytes received")

         return ''.join(buffer)

      except IOError as ioe:
         raise ErrorCodeException(ErrorCode.io_error, str(ioe))


   def readInput(self):
      # read header as 32-bit value
      header = self.readPacket(4)

      # convert to 32-bit value to unsigned int
      headerval = struct.unpack('!I', header)[0]

      # header size exceeded
      #if headerval > 65536:
      #   raise ErrorCodeException(ErrorCode.protocol_error, "header size is too big")

      return self.readPacket(headerval)


   def handle(self):

      logger.info("handle: %s connected" % self.client_address[0])

      return_code = 0

      try:
         # get the filename from the client, this will be a full path
         jsoninput = self.readInput()
         inputfiles = json.loads(jsoninput)

         files = inputfiles['files']
         if len(files) <= 0:
            raise ErrorCodeException(ErrorCode.invalid_args_error,
                                    'file input list is 0')

         numfiles = len(files)
         if numfiles >= 20000:
            raise ErrorCodeException(ErrorCode.file_limit_exceeded,
                                    "file limit exceeded 20000: %s" % numfiles)

         return_code = self.server.handleFiles(files)

      except ErrorCodeException as ee:
         logger.error("handle: %s errorcode: %s message: %s" %
                        (self.client_address[0], str(ee), ee.getMsg()))
         return_code = ee.error_code

      # catch unknown exceptions
      except Exception as be:
         logger.error("handle: %s %s" % (self.client_address[0], str(be)))
         return_code = ErrorCode.unknown_error

      try:
         self.request.sendall(struct.pack('>H', return_code))

      except Exception as basee:
         logger.error("handle: %s %s" % (self.client_address[0], str(basee)))

      finally:
         self.request.close()
         logger.info("handle: %s disconnected" % self.client_address[0])


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
