#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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

import time
from email.parser import Parser
from ngams import error, info

CRLF = '\r\n'

class MIMEMultipartParser(object):

	class ReadingState:
		mainheader, delimiter, headers, data = range(4)

	def __init__(self, handler, fd, totalSize, readSize):
		self._handler = handler
		self._fd = fd
		self._totalSize = totalSize
		self._readSize = readSize

	def getReadingTime(self):
		return self._readingTime

	def parse(self):

		rdSize = self._readSize
		remSize = self._totalSize

		readingTime = 0

		readingFile = False
		prevBuf = None
		state = self.ReadingState.mainheader

		while True:

			# Don't try to over-read during the last reading
			if remSize < rdSize:
				rdSize = remSize

			# Read, read, read...
			rdt = time.time()
			buf = self._fd.read(rdSize)
			readingTime += (time.time() - rdt)
			remSize -= rdSize

			# Anything coming from a previous iteration gets prefixed
			if prevBuf:
				buf = prevBuf + buf
			prevBuf = None

			# On the first stage we read the MIME multipart headers and parse them
			# If found, we start reading delimiters; otherwise we keep reading data
			if state == self.ReadingState.mainheader:
				endingIdx = buf.find(CRLF + CRLF)
				if endingIdx != -1:
					info(4, 'Parsing MIME multipart headers')
					state         = self.ReadingState.delimiter
					headers       = buf[:endingIdx + 4]
					buf           = buf[endingIdx + 4:]
					msg           = Parser().parsestr(headers, headersonly=True)
					boundary      = msg.get_param('boundary')
					containerName = msg.get_param('container_name')
					info(5, 'MIME multipart boundary: ' + boundary)

					# Fail if we're missing any of these
					if not boundary or not containerName:
						msg = 'Either \'boundary\' or \'container_name\' are not specified in the Content-type header'
						error(msg)
						raise Exception, msg

					self._handler.startContainer(containerName)

				else:
					prevBuf = buf
					continue

			# We can read delimiters either because we've just started
			# reading the body of the MIME multipart message or because
			# we just finished reading a particular part of the multipart
			if state == self.ReadingState.delimiter:

				# We come from reading a previous multipart part
				if readingFile:
					self._handler.endFile()
				readingFile = False

				# Look for both delimiter and final delimiter
				delIdx  = buf.find(CRLF + '--' + boundary + CRLF)
				fDelIdx = buf.find(CRLF + '--' + boundary + '--')
				if delIdx != -1:
					# We don't actually need the delimiter itself
					#delimiter = buf[:delIdx + 4 + len(boundary) + 2]
					buf       = buf[delIdx + 4 + len(boundary) + 2:]
					state     = self.ReadingState.headers
				elif fDelIdx != -1:
					self._handler.endContainer()
					break
				else:
					prevBuf = buf
					continue

			if state == self.ReadingState.headers:
				idx = buf.find(CRLF + CRLF)
				if idx != -1:
					info(4, 'Processing file headers')
					headers  = buf[:idx+4]
					buf      = buf[idx+4:]
					msg      = Parser().parsestr(headers, headersonly=True)
					filename = msg.get_filename()
					if not filename:
						msg = 'No filename found in internal multipart part header'
						raise Exception, msg
					state = self.ReadingState.data
					self._handler.startFile(filename)
					readingFile = True
				else:
					prevBuf = buf
					continue

			# When reading data, look for the next delimiter
			# When found, finish writing data, and pass the
			# delimiter to the ReadingState.delimiter state
			if state == self.ReadingState.data:
				delIdx  = buf.find(CRLF + '--' + boundary)
				if delIdx != -1:
					info(5, 'Found end of file ' + filename)
					state = self.ReadingState.delimiter
					prevBuf = buf[delIdx:]
					buf = buf[:delIdx]

				buf = self._handler.handleData(buf, state == self.ReadingState.data)
				if buf and len(buf) > 0:
					prevBuf = buf

		self._readingTime = readingTime
